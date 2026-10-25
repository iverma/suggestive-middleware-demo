"""
Golden test set generator for the IROPS (irregular operations) re-booking engine.

Each record is a weather-cancelled MSP departure: the customer/booking record,
a simulated inventory snapshot at cancellation time, and the ground truth the
engine is scored against -- an ORDERED list of acceptable resolutions. The
engine (or a simulated customer rejecting offers) walks down that list until
an offer is accepted; HUMAN_REVIEW is always the final fallback.

Deliberately NOT in this file: customer history, preferences, recommendations,
ancillaries, or expected customer responses. Those come from the customer
history DB and the agent at run time, and rejections are simulated by the test
harness.

Deterministic: same seed -> same file. Change N_RECORDS to scale up.
"""
import csv
import random
import string
from datetime import datetime, timedelta

N_RECORDS = 100
SEED = 20261025
OUT = "golden_set_irops_msp.csv"

rng = random.Random(SEED)

# --- Reference data ----------------------------------------------------------
# code: (city, miles from MSP, nearby alternate airports)
DESTINATIONS = {
    "ATL": ("Atlanta", 907, []),
    "BOS": ("Boston", 1124, ["PVD"]),
    "CLT": ("Charlotte", 931, []),
    "DEN": ("Denver", 680, []),
    "DFW": ("Dallas-Fort Worth", 852, ["DAL"]),
    "DTW": ("Detroit", 528, []),
    "EWR": ("Newark", 1008, ["JFK", "LGA"]),
    "IAD": ("Washington Dulles", 931, ["DCA", "BWI"]),
    "IAH": ("Houston", 1034, ["HOU"]),
    "JFK": ("New York JFK", 1029, ["LGA", "EWR"]),
    "LAS": ("Las Vegas", 1299, []),
    "LAX": ("Los Angeles", 1535, ["BUR", "SNA", "LGB"]),
    "MCO": ("Orlando", 1310, []),
    "MIA": ("Miami", 1501, ["FLL"]),
    "ORD": ("Chicago O'Hare", 334, ["MDW"]),
    "PHL": ("Philadelphia", 980, []),
    "PHX": ("Phoenix", 1276, []),
    "SAN": ("San Diego", 1532, []),
    "SEA": ("Seattle", 1399, []),
    "SFO": ("San Francisco", 1589, ["OAK", "SJC"]),
    "STL": ("St. Louis", 448, []),
}
GROUND_MAX_MILES = 550          # rail / coach / rental car is realistic below this
HUBS = ["DEN", "ORD", "DTW", "ATL", "DFW", "SLC"]

DISRUPTIONS = [
    "Winter Storm Kaia - heavy snow, runway closures",
    "Freezing rain - de-icing capacity exceeded",
    "Severe thunderstorms - FAA ground stop",
    "Low visibility / freezing fog",
    "Blizzard - crew legality timeouts",
]

CABINS = [("economy", 0.6), ("business", 0.25), ("first_class", 0.15)]
TIERS = [("Member", 0.4), ("Silver", 0.2), ("Gold", 0.18), ("Platinum", 0.14), ("Diamond", 0.08)]
URGENCY = [("flexible", 0.4), ("urgent", 0.35), ("critical", 0.25)]
DEADLINE_HRS = {"critical": 12, "urgent": 24, "flexible": 72}

PURPOSE = {
    "critical": ["funeral", "scheduled_surgery", "cruise_departure", "wedding_party_member",
                 "court_appearance", "international_connection"],
    "urgent": ["business_meeting", "job_interview", "conference_speaker", "return_for_work_shift"],
    "flexible": ["vacation", "visiting_family", "leisure_weekend", "college_visit"],
}

CONCERNS = [
    ("infant_in_arms", 0.11), ("pregnancy_third_trimester", 0.06), ("medical_condition", 0.07),
    ("wheelchair_assistance", 0.07), ("elderly_passenger", 0.08), ("service_animal", 0.04),
    ("traveling_with_pet", 0.05), ("unaccompanied_minor", 0.04), ("dietary_restriction", 0.04),
]
SENSITIVE = {"infant_in_arms", "pregnancy_third_trimester", "medical_condition",
             "wheelchair_assistance", "elderly_passenger", "service_animal"}
NO_GROUND = {"medical_condition", "pregnancy_third_trimester", "wheelchair_assistance", "unaccompanied_minor"}
NO_PARTNER = {"traveling_with_pet", "unaccompanied_minor"}


def pick(weighted):
    items, weights = zip(*weighted)
    return rng.choices(items, weights=weights)[0]


def code(chars, k):
    return "".join(rng.choices(chars, k=k))


PNR_CHARS = string.ascii_uppercase.replace("O", "").replace("I", "") + "23456789"


def make_party(concerns):
    if "unaccompanied_minor" in concerns:
        kids = rng.randint(1, 2)
        return kids, 0, kids
    size = rng.choices([1, 2, 3, 4, 5], weights=[30, 28, 18, 14, 10])[0]
    needs_kid = "infant_in_arms" in concerns
    if needs_kid and size == 1:
        size = 2
    max_kids = size - 1
    kids = rng.randint(1, max_kids) if needs_kid else (rng.randint(0, max_kids) if rng.random() < 0.35 else 0)
    return size, size - kids, kids


# --- Record generation -------------------------------------------------------
def gen_record(i, base_date):
    dest = rng.choice(list(DESTINATIONS))
    city, miles, alt_airports = DESTINATIONS[dest]
    cabin = pick(CABINS)
    urgency = pick(URGENCY)

    concerns = [c for c, p in CONCERNS if rng.random() < p]
    if "unaccompanied_minor" in concerns:  # UMNR excludes adult-only concerns
        concerns = [c for c in concerns if c in ("unaccompanied_minor", "dietary_restriction", "medical_condition")]
        cabin = "economy"
    concerns = concerns[:2]

    size, adults, kids = make_party(concerns)
    dep = base_date + timedelta(hours=rng.randint(6, 22), minutes=rng.choice([0, 15, 30, 45]))
    stops = rng.choices([0, 1, 2, 3], weights=[45, 33, 15, 7])[0]
    if miles < 600:
        stops = min(stops, 1)

    same_route = rng.choice([2, 3, 4, 5, 6, 8, 10, 14, 18, 22, 26, 30, 38, 48, 60])
    alt_via = rng.choice([h for h in HUBS if h != dest]) if rng.random() < 0.7 else ""
    alt_hrs = rng.randint(3, 30) if alt_via else ""
    alt_stops = (max(1, stops) + (1 if rng.random() < 0.25 else 0)) if alt_via else ""
    partner_hrs = rng.randint(4, 36) if rng.random() < 0.5 else ""
    alt_ap = rng.choice(alt_airports) if alt_airports and rng.random() < 0.8 else ""
    alt_ap_hrs = rng.randint(3, 28) if alt_ap else ""
    ground_hrs = round(miles / 55 + 1, 1) if miles <= GROUND_MAX_MILES else ""
    premium_avail = "" if cabin == "economy" else rng.choice(["Y", "Y", "N"])
    split_hrs = rng.randint(1, 6) if size >= 3 and rng.random() < 0.5 else ""

    tier = pick(TIERS)
    if cabin != "economy" and tier == "Member" and rng.random() < 0.6:
        tier = "Gold"

    return {
        "case_id": f"IROPS-MSP-{i:04d}",
        "customer_id": f"CUST-{code(string.digits, 7)}",   # join key for the customer history DB
        "pnr": code(PNR_CHARS, 6),
        "disruption_event": rng.choice(DISRUPTIONS),
        "scheduled_departure_local": dep.strftime("%Y-%m-%d %H:%M"),
        "origin": "MSP",
        "destination": dest,
        "destination_city": city,
        "original_stops": stops,
        "cabin": cabin,
        "fare_type": rng.choice(["refundable", "non_refundable"]) if cabin != "economy"
        else rng.choice(["basic_economy", "standard", "standard", "refundable"]),
        "loyalty_tier": tier,
        "party_size": size,
        "adults": adults,
        "children": kids,
        "travel_concerns": ";".join(concerns) if concerns else "none",
        "urgency": urgency,
        "trip_purpose": rng.choice(PURPOSE[urgency]),
        # simulated inventory snapshot at time of cancellation
        "next_same_route_seat_hrs": same_route,
        "premium_cabin_available": premium_avail,
        "split_party_first_seats_hrs": split_hrs,
        "alt_route_via": alt_via,
        "alt_route_stops": alt_stops,
        "alt_route_hrs": alt_hrs,
        "partner_airline_hrs": partner_hrs,
        "alt_airport": alt_ap,
        "alt_airport_hrs": alt_ap_hrs,
        "ground_transport_hrs": ground_hrs,
    }


# --- Ground truth: ordered acceptable resolutions ----------------------------
def acceptable_outcomes(r):
    concerns = set() if r["travel_concerns"] == "none" else set(r["travel_concerns"].split(";"))
    urg = r["urgency"]
    deadline = DEADLINE_HRS[urg]
    premium = r["cabin"] != "economy"
    sensitive = bool(concerns & SENSITIVE)
    notes, unacceptable, options = [], [], []

    def consider(name, hrs, blocked_reason=None):
        if hrs == "":
            return
        if blocked_reason:
            unacceptable.append(name)
            notes.append(blocked_reason)
        elif hrs > deadline:
            unacceptable.append(name)
            notes.append(f"{name} at {hrs}h misses {deadline}h deadline")
        else:
            options.append((name, hrs))

    consider("REBOOK_SAME_ROUTE", r["next_same_route_seat_hrs"])
    consider("REBOOK_ALTERNATE_AIRPORT", r["alt_airport_hrs"])
    consider("REBOOK_PARTNER_AIRLINE", r["partner_airline_hrs"],
             "partner can't carry UMNR/pet reservation" if concerns & NO_PARTNER else None)
    consider("REBOOK_ALTERNATE_ROUTE", r["alt_route_hrs"],
             "2+ connections unsuitable for passenger needs"
             if sensitive and r["alt_route_stops"] != "" and r["alt_route_stops"] >= 2 else None)
    consider("ALTERNATE_GROUND_TRANSPORT", r["ground_transport_hrs"],
             "ground travel unsuitable for medical/mobility/UMNR" if concerns & NO_GROUND else None)
    consider("SPLIT_PARTY_REBOOK", r["split_party_first_seats_hrs"],
             "never separate minors or assisted passengers from party"
             if r["children"] > 0 or sensitive else None)

    # order: same route first if within 4h of the fastest option, then fastest-first
    options.sort(key=lambda x: x[1])
    same = [o for o in options if o[0] == "REBOOK_SAME_ROUTE"]
    if same and same[0][1] <= options[0][1] + 4:
        options.remove(same[0])
        options.insert(0, same[0])
    acceptable = [o[0] for o in options]

    if premium and r["premium_cabin_available"] == "N":
        if urg == "flexible":
            unacceptable.append("CABIN_DOWNGRADE_WITH_FARE_DIFF_REFUND")
            notes.append("flexible premium traveler: protect cabin")
        else:
            acceptable.append("CABIN_DOWNGRADE_WITH_FARE_DIFF_REFUND")

    if urg == "flexible":
        acceptable.append("TRAVEL_CREDIT")
    else:
        unacceptable.append("TRAVEL_CREDIT")
    acceptable.append("FULL_REFUND")   # refund is always owed on a carrier cancellation

    # policy cases that must go to a person before any automated offer
    mandatory_human = None
    if "unaccompanied_minor" in concerns:
        mandatory_human = "UMNR requires coordination with guardian and escort"
    elif "medical_condition" in concerns and urg == "critical":
        mandatory_human = "critical trip with medical condition needs clearance"
    elif not options and urg == "critical":
        mandatory_human = "no transport option meets critical deadline"

    if mandatory_human:
        acceptable.insert(0, "HUMAN_REVIEW")
        notes.insert(0, mandatory_human)
    else:
        acceptable.append("HUMAN_REVIEW")  # final fallback once every offer is rejected

    return {
        "acceptable_resolutions": "|".join(acceptable),
        "unacceptable_resolutions": "|".join(dict.fromkeys(unacceptable)) or "none",
        "acceptability_notes": "; ".join(dict.fromkeys(notes)),
    }


def main():
    base = datetime(2026, 12, 14)
    rows = []
    for i in range(1, N_RECORDS + 1):
        r = gen_record(i, base)
        r.update(acceptable_outcomes(r))
        rows.append(r)
    with open(OUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {len(rows)} rows -> {OUT}")


if __name__ == "__main__":
    main()
