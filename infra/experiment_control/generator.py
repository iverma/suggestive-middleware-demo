"""
Load-test record generator: unique, varied customer records on demand.

Derived from golden_set/generate_golden_set.py (same reference data, distributions and
rules), but every record is generated from its own seeded RNG, so record i is the same every
time, on any machine, without storing a file: the load producer creates record 4,317 when it
needs it, and the finalizer regenerates it to score the outcome against its acceptable list.
Case ids are LOAD-xxxxx, so they never collide with the golden set (IROPS-MSP-xxxx).
"""
import random
import string
import threading
from datetime import datetime, timedelta

rng = random.Random(0)          # replaced per record under _lock
_lock = threading.Lock()
LOAD_SEED = 20261027
BASE_DATE = datetime(2026, 12, 14)

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




def make_record(i, seed=LOAD_SEED):
    """Record i of a load set: customer inputs plus its acceptable / unacceptable outcomes."""
    global rng
    with _lock:
        rng = random.Random(f"{seed}:{i}")
        r = gen_record(i, BASE_DATE)
        r.update(acceptable_outcomes(r))
    r["case_id"] = f"LOAD-{i:05d}"
    return r


def make_inputs(i, seed=LOAD_SEED, columns=None):
    """Just the customer-record columns, as stored on a run's copy (blank values dropped)."""
    r = make_record(i, seed)
    keep = columns or [k for k in r if k not in ("acceptable_resolutions", "unacceptable_resolutions",
                                                  "acceptability_notes")]
    return {k: r[k] for k in keep if k in r and r[k] != ""}


# --- Customers with a hidden preference (agent comparison runs) ---------------
# Each customer will accept exactly one offer and reject the others. Most of them said
# something about it on an earlier call; the note is stored on their record, the preference is
# not shown to anyone deciding offers (rules engine or model).
HINTS = {
    "REBOOK_SAME_ROUTE": ["I really want to stay on my original routing, even if it means waiting a bit longer.",
                          "Please keep me on the same route, I have lounge access and checked bags through."],
    "REBOOK_ALTERNATE_AIRPORT": ["Landing at a nearby airport is fine, my sister can pick me up from either one.",
                                 "Any airport in the area works for me, I have a rental car anyway."],
    "REBOOK_PARTNER_AIRLINE": ["I don't care which airline it is, I just need to get there today.",
                               "Put me on any carrier, my loyalty status doesn't matter for this trip."],
    "REBOOK_ALTERNATE_ROUTE": ["Extra connections don't bother me, I'd rather keep moving than sit here.",
                               "Route me through wherever, I'm fine with a couple of stops."],
    "ALTERNATE_GROUND_TRANSPORT": ["Honestly I'd take a bus or a car service if it gets me there sooner.",
                                   "I can drive or take a shuttle, it isn't that far."],
    "SPLIT_PARTY_REBOOK": ["We don't need to sit together or even fly together, whoever can go first should go.",
                           "Splitting our group across flights is totally fine with us."],
    "CABIN_DOWNGRADE_WITH_FARE_DIFF_REFUND": ["I'll sit anywhere on the plane, just refund the difference.",
                                              "Economy is fine for me today if I get the fare difference back."],
    "TRAVEL_CREDIT": ["I can travel another weekend, keep the value for a future trip.",
                      "No rush on my side, I'd take credit and rebook myself later."],
    "FULL_REFUND": ["At this point I just want my money back and I'll make other plans.",
                    "Cancel it and refund me, I'm not traveling anymore."],
}
VAGUE = ["Please help, this whole day has been really stressful.",
         "I've been on hold for an hour, just tell me what you can do.",
         "This is the second cancellation this month, I'm frustrated."]
HINT_SHARE = 0.7


def preference(i, seed, offers):
    """(preferred offer, customer note) for load record i, given its rules-engine offers."""
    real = [o for o in offers if o != "HUMAN_REVIEW"]
    r = random.Random(f"{seed}:{i}:pref")
    if not real or offers[0] == "HUMAN_REVIEW":
        return None, r.choice(VAGUE)            # goes straight to a person either way
    # The rules engine's first choice already works for many customers; these are the ones
    # it ranks wrong (anything but its first choice), which is where reading the note pays off.
    pref = r.choice(real[1:] if len(real) > 1 else real)
    note = r.choice(HINTS[pref]) if r.random() < HINT_SHARE and pref in HINTS else r.choice(VAGUE)
    return pref, note
