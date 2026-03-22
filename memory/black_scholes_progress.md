---
name: Black-Scholes education progress
description: Where we stopped in the Black-Scholes primer walkthrough
type: project
---

User is working through Black-Scholes from first principles — no formula memorization, intuition first.

**Completed:**
- All five inputs understood intuitively in plain English:
  - S (spot) → higher spot = higher probability of finishing ITM = higher call value
  - K (strike) → intrinsic value (ITM) vs extrinsic/time value (OTM) distinction landed cleanly
  - T (time) → more time = more probability of finishing ITM = higher value
  - r (risk-free rate) → options attractive vs owning stock outright when rates high → direct relationship with call value; inverse for puts
  - σ (volatility) → the formula runs both directions: forward with HV gives theoretical price; backward from market price gives IV. The gap between them IS the IV/HV ratio.

**Stopped here:**
- Next question pending answer: "What do N(d1) and N(d2) represent conceptually?"
- User understands these are probability-related but hasn't answered yet

**Why:** N() is the cumulative normal distribution function — N(d1) is the delta of the option (probability-weighted sensitivity of the call to stock price movement); N(d2) is the probability that the option expires ITM under the risk-neutral measure. These are the two core probability terms that tie the formula back to all the intuition built so far.

**How to apply:** Resume at N(d1) and N(d2) — let user answer first before explaining. Then connect d1 and d2 formulas back to the inputs they already understand.
