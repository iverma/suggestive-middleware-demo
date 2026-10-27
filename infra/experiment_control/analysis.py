"""
Read-only analysis of a run, for /results: what the workers did, how each crash was
recovered, race outcomes, and the incident feed. Pure functions over items already read.
"""
from common import KEY, KILL_POINTS, TERMINAL

def score(row, g):
    offered = [h["o"] for h in row.get("history", [])]
    bad = [o for o in offered if o in g["unacceptable"]]
    ok = (row.get("status") == "resolved" and row.get("resolution") in g["acceptable"] and not bad)
    return ok, bad


def worker_activity(rows, kills, start, end, t):
    """
    Reconstruct what the autoscaled workers did from the records themselves: every pass
    a worker handles leaves a [claim, commit] interval in the record's history, a record
    being worked right now has a live lease, and a killed worker left a KILL item.
    """
    killed_envs = {k["worker"] for k in kills if k.get("worker") and k.get("mode", "exit") in ("exit", "timeout")}
    faulted = {(k["worker"], k["case_id"], int(k.get("pass", 1))) for k in kills}   # their leases are abandoned
    intervals = []   # (start, end, env, pool)
    for r in rows:
        for h in r.get("history", []):
            if "c" in h:
                intervals.append((float(h["c"]), float(h["t"]), h["w"], "rebooking" if int(h["p"]) == 1 else "rerouting"))
        if (r.get("status") == "in_progress" and r.get("lease_owner") not in killed_envs
                and (r.get("lease_owner"), r[KEY], int(r.get("pass_no", 1))) not in faulted
                and float(r.get("lease_expires", 0)) > t):
            pool = "rebooking" if int(r.get("pass_no", 1)) == 1 else "rerouting"
            intervals.append((float(r.get("claimed_at", t)), t, r["lease_owner"], pool))
    for k in kills:
        if k.get("worker"):
            began = float(k.get("claimed_at") or k.get("started_at") or k["killed_at"])
            pool = "rebooking" if int(k.get("pass", 1)) == 1 else "rerouting"
            intervals.append((began, float(k["killed_at"]), k["worker"], pool))
    # One claim = one interval. A worker killed after committing appears in both the record's
    # history and its kill entry; keep the longer of the two.
    by_claim = {}
    for a, b, env, pool in intervals:
        key = (env, round(a, 3))
        if key not in by_claim or b > by_claim[key][1]:
            by_claim[key] = (a, b, env, pool)
    intervals = list(by_claim.values())

    timeline = concurrency_timeline(intervals, start, end)
    envs = {}
    for a, b, env, pool in intervals:
        e = envs.setdefault(env, {"worker_id": env, "first_seen": a, "last_seen": b, "passes": 0,
                                  "pools": set(), "takeovers": 0, "faults": 0, "killed": env in killed_envs})
        e["first_seen"], e["last_seen"] = min(e["first_seen"], a), max(e["last_seen"], b)
        e["pools"].add(pool)
    for r in rows:
        for h in r.get("history", []):
            if h.get("w") in envs:
                envs[h["w"]]["passes"] += 1
                envs[h["w"]]["takeovers"] += 1 if "from" in h else 0
    for k in kills:
        if k.get("worker") in envs and k.get("mode") == "raise":
            envs[k["worker"]]["faults"] += 1
    busy_now = {r["lease_owner"] for r in rows if r.get("status") == "in_progress"
                and r.get("lease_owner") not in killed_envs
                and (r.get("lease_owner"), r[KEY], int(r.get("pass_no", 1))) not in faulted
                and float(r.get("lease_expires", 0)) > t}
    workers = []
    for e in sorted(envs.values(), key=lambda x: x["first_seen"]):
        state = "killed" if e["killed"] else ("busy" if e["worker_id"] in busy_now else "idle")
        workers.append({**e, "pools": sorted(e["pools"]), "state": state,
                        "first_seen_s": round(e["first_seen"] - start, 1), "last_seen_s": round(e["last_seen"] - start, 1)})
    first_claim = min((a for a, *_ in intervals), default=None)
    return {
        "busy_now": len(busy_now), "peak": max(timeline["total"], default=0), "used": len(envs),
        "first_claim_s": round(first_claim - start, 1) if first_claim else None,
        "timeline": timeline, "workers": workers,
    }


def concurrency_timeline(intervals, start, end):
    """Per 1s bin: the most workers that were busy at the same instant, per pool and in total.
    An exact sweep over [claim, commit] intervals, so a crashed worker and its replacement
    are never counted as concurrent just because they fall in the same second."""
    n_bins = max(1, int((end - start) // 1) + 1)
    events = sorted([(a, 1, p) for a, b, _, p in intervals] + [(b, -1, p) for a, b, _, p in intervals],
                    key=lambda e: (e[0], e[1]))          # at a tie, ends before starts
    cur = {"rebooking": 0, "rerouting": 0}
    out = {"bin_s": 1, "rebooking": [0] * n_bins, "rerouting": [0] * n_bins, "total": [0] * n_bins}
    i = 0
    for b in range(n_bins):
        lo, hi = start + b, start + b + 1
        peak = {k: v for k, v in cur.items()}
        peak_total = sum(cur.values())
        while i < len(events) and events[i][0] < hi:
            t_, d, p = events[i]
            cur[p] += d
            if t_ >= lo or d > 0:
                peak[p] = max(peak[p], cur[p])
                peak_total = max(peak_total, sum(cur.values()))
            i += 1
        out["rebooking"][b], out["rerouting"][b], out["total"][b] = peak["rebooking"], peak["rerouting"], peak_total
    return out


RECOVERY = {
    "on_receive": "redelivered",
    "after_claim": "lease takeover", "during_offer": "lease takeover",
    "during_customer_response": "lease takeover", "before_commit": "lease takeover",
    "after_commit_before_handoff": "handoff repaired",
    "after_handoff_before_ack": "duplicate dropped",
    "after_final_commit_before_ack": "finished duplicate dropped",
    "stalled_past_timeout": "lease takeover",
}
TIMEOUT_POINT = "stalled_past_timeout"


LEGACY_STAGES = {"mid_work": "before_commit", "after_commit": "after_commit_before_handoff",
                 "before_handoff": "after_commit_before_handoff"}


def normalize_kill(k):
    """Kill items written before the 8 kill points existed carried only a coarse stage."""
    if "point" not in k:
        k = {**k, "point": LEGACY_STAGES.get(k.get("stage"), "before_commit")}
    return {"pass": 1, "mode": "exit", **k}


def attribute_kills(kills, rows):
    """
    For each crash, check the record for the recovery that crash point should have caused,
    and how long it took. Recovered = the record moved past the crashed pass (or finished)
    and, for crashes holding a lease, the next claim of that pass shows a takeover from it.
    """
    out = []
    for k in sorted(map(normalize_kill, kills), key=lambda k: float(k["killed_at"])):
        r = rows.get(k["case_id"], {})
        p = int(k.get("pass", 1))
        hist = r.get("history", [])
        moved_on = r.get("status") in TERMINAL or int(r.get("pass_no", 0)) > p
        how = RECOVERY.get(k["point"], "?")
        nxt = None
        if how in ("redelivered", "lease takeover"):
            done = [h for h in hist if int(h["p"]) == p]
            nxt = done[0] if done else None
            recovered = bool(nxt) and (how == "redelivered" or nxt.get("from") == k["worker"])
            if how == "redelivered" and nxt and nxt.get("from"):
                how = "redelivered (after a takeover)"
        elif how == "handoff repaired":
            later = [h for h in hist if int(h["p"]) == p + 1]
            nxt = later[0] if later else None
            recovered = moved_on and (bool(nxt) or r.get("status") in TERMINAL)
        else:                                    # the work itself was done before the crash
            recovered = moved_on
        out.append({**k, "recovered": bool(recovered), "recovered_by": how,
                    "recovery_s": round(float(nxt["c"]) - float(k["killed_at"]), 1) if nxt and "c" in nxt else None})
    return out


EVENT_COUNTERS = ("fenced", "repaired", "stale", "deferred", "duplicates_sent", "throttles_injected",
                  "sdk_retries", "failed_throttled", "failed_poison", "failed_injected", "failed_error", "backoffs",
                  "own_write_recognized")


def incidents(start, rows, kills, zombies, sweeps, limit=60):
    """Everything that went wrong and how it was handled, newest first, for the incident feed."""
    out = []
    for k in kills:
        out.append({"t": float(k["killed_at"]), "kind": "crash", "worker": k["worker"], "case_id": k["case_id"],
                    "pass": int(k.get("pass", 1)), "point": k["point"], "mode": k.get("mode"),
                    "recovered": k.get("recovered"), "recovered_by": k.get("recovered_by")})
    for r in rows:
        for h in r.get("history", []):
            if h.get("from") and "c" in h:
                out.append({"t": float(h["c"]), "kind": "takeover", "worker": h["w"], "from": h["from"],
                            "case_id": r[KEY], "pass": int(h["p"])})
    for z in zombies:
        out.append({"t": float(z["woke_at"]), "kind": "zombie", "worker": z["worker"], "case_id": z["case_id"],
                    "pass": int(z["pass"]), "outcome": z.get("outcome"), "rival": z.get("taken_over_by")})
    for w in sweeps:
        out.append({"t": float(w["swept_at"]), "kind": "swept", "case_id": w["case_id"], "pass": int(w["pass"]),
                    "deliveries": int(w.get("deliveries", 0))})
    for e in out:
        e["at_s"] = round(e.pop("t") - start, 1)
    return sorted(out, key=lambda e: e["at_s"], reverse=True)[:limit]


def race_results(run_items, rows, events):
    """Slow-worker and duplicate-delivery outcomes, and the invariant duplicates must never break."""
    zombies = [i for i in run_items if i["sk"].startswith("ZOMBIE#")]
    # Exactly one committed result per (record, pass), however many copies or workers touched it.
    double = [{"case_id": r[KEY], "passes": sorted({int(h["p"]) for h in r.get("history", [])
                                                    if [int(x["p"]) for x in r["history"]].count(int(h["p"])) > 1})}
              for r in rows if len({int(h["p"]) for h in r.get("history", [])}) != len(r.get("history", []))]
    return {
        "zombies": len(zombies),
        "zombies_fenced": sum(1 for z in zombies if z.get("outcome") == "fenced"),
        "zombies_uncontested": sum(1 for z in zombies if z.get("outcome") == "uncontested"),
        "zombie_list": sorted(zombies, key=lambda z: float(z["claimed_at"])),
        "duplicates_sent": int(events.get("duplicates_sent", 0)),
        "duplicates_held_back": int(events.get("deferred", 0)),
        "duplicates_dropped": int(events.get("stale", 0)),
        "double_commits": len(double), "double_commit_list": double,
    }


def kills_by_point(fired):
    rows = []
    for point in KILL_POINTS + [TIMEOUT_POINT]:
        ks = [k for k in fired if k["point"] == point]
        waits = [k["recovery_s"] for k in ks if k["recovery_s"] is not None]
        rows.append({"point": point, "fired": len(ks),
                     "exit": sum(1 for k in ks if k.get("mode") == "exit"),
                     "raise": sum(1 for k in ks if k.get("mode") == "raise"),
                     "timeout": sum(1 for k in ks if k.get("mode") == "timeout"),
                     "recovered": sum(1 for k in ks if k["recovered"]),
                     "recovered_by": RECOVERY[point],
                     "passes": sorted({int(k.get("pass", 1)) for k in ks}),
                     "avg_recovery_s": round(sum(waits) / len(waits), 1) if waits else None})
    return rows


