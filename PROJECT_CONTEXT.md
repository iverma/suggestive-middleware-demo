# Project Context: Autonomous Travel Re-booking Engine (Demo)

## The ask being demonstrated
> Real systems must handle failure, concurrency, and scale. Build something that demonstrates thoughtful systems engineering — whether that's a developer tool that solves a real workflow pain point, a distributed primitive that handles failure gracefully, or infrastructure that degrades predictably under stress.

## Product idea
An autonomous backend engine for travel re-booking during disruption events (e.g., weather-driven mass flight cancellations).

- **Input:** A large batch of impacted customers (customer profile + cancelled itinerary).
- **Goal:** Route each customer to an acceptable next outcome (rebooking, alternate route, refund/voucher, hotel, etc.) based on their profile.
- **Core requirements:** Reliable, horizontally scalable, concurrency-safe processing.

## Demo centerpiece: failure tolerance
- Workers are repeatedly killed live during the demo.
- Must prove **no customer record is lost or double-processed** mid-flight.
- Infra should recover gracefully and degrade predictably under stress.

## Additional feature: multi-pass resolution
- Booking/scheduling systems see heavy customer-side changes and rejections; first-pass solutions frequently come back into the re-routing queue.
- **Pass 1:** Deterministic rules-based engine (fast, cheap, predictable).
- **Pass 2+ (rejections / re-entries):** Increasingly fuzzy cases that are hard to encode as rules or ML models → resolved via **agent reasoning**, informed by the customer's prior booking history, to reach an acceptable outcome.

## Working agreement
- Build step by step, following the user's subsequent asks. Don't get ahead of them.
