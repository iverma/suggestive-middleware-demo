"""
Live rules-vs-agent comparison against the deployed demo: the same generated customers (each
wants one particular offer), with agent reroutes (Claude Haiku on Bedrock) and with the rules
engine's order, run side by side as two tenants. Prints both summaries as annotations.

    python3 scripts/agent_compare.py https://<id>.lambda-url.<region>.on.aws/ [records]
"""
import json
import sys
import time
import urllib.error
import urllib.request
import uuid


def call(base, method, path):
    req = urllib.request.Request(base.rstrip("/") + path, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def start_arm(base, agent, records, timeout_s=1200):
    end = time.time() + timeout_s
    while True:                                 # wait for room in the shared budget
        status, body = call(base, "POST", f"/run?kind=load&customer=preference&agent={agent}&records={records}"
                                          f"&client_token={uuid.uuid4()}")
        if status == 202:
            print(f"arm agent={agent}: {body['run_id']} at {body.get('rate')} records/s")
            return body["run_id"]
        if status != 429 or time.time() > end:
            raise SystemExit(f"::error::arm agent={agent} not admitted: {status} {body}")
        print("waiting for budget room:", body)
        time.sleep(20)


def wait_arms(base, runs, timeout_s=2400):
    end, done = time.time() + timeout_s, {}
    while time.time() < end and len(done) < len(runs):
        time.sleep(20)
        for agent, run_id in runs.items():
            if agent in done:
                continue
            status, res = call(base, "GET", f"/results?run={run_id}")
            if status != 200:
                continue                        # busy (429): try again next round
            lr = res.get("latest_run") or {}
            s = lr.get("stats", {})
            print(f"  agent={agent} {lr.get('status')}: released {s.get('released')} closed {s.get('closed')}/"
                  f"{s.get('records')} at {s.get('duration_s')}s")
            if lr.get("summary"):
                done[agent] = lr["summary"]
            elif lr.get("status") in ("failed", "cancelled"):
                raise SystemExit(f"::error::{run_id} {lr.get('status')}: {lr.get('error')}")
    if len(done) < len(runs):
        raise SystemExit("::error::arms did not finish in time")
    return done


def main(base, records="500"):
    runs = {agent: start_arm(base, agent, int(records)) for agent in (1, 0)}     # side by side
    out = wait_arms(base, runs)
    for agent, s in out.items():
        print(f"::notice title={'agent' if agent else 'rules'} arm {runs[agent]}::{json.dumps(s)}")
    a, r = out[1], out[0]
    first = lambda s: f"{round(100 * s['closed_first_reroute'] / max(1, s['rerouted']))}%"
    print(f"::notice title=Rules vs agent::avg passes {r['avg_passes']} vs {a['avg_passes']}; closed on first reroute "
          f"{first(r)} vs {first(a)}; avg close {r['avg_close_s']}s vs {a['avg_close_s']}s; to a person "
          f"{r['human_review']} vs {a['human_review']}; model calls {a['agent_calls']} ({a['agent_fallbacks']} fallbacks), "
          f"{a['agent_tokens_in']}/{a['agent_tokens_out']} tokens, ~${a['agent_cost_usd']}")
    bad = [k for k, s in (("rules", r), ("agent", a)) if not s["passed"]]
    if bad:
        print(f"::error::arms not clean: {bad}")
        return 1
    if a["agent_calls"] and a["agent_fallbacks"] > a["agent_calls"] // 2:
        print("::error::most model calls fell back to the rules order (no Bedrock access?)")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
