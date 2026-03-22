# Credit Spread Strategy — Concept Primer

## Signal Validation Plan

This application has two main primary tasks. 

**Task 1:** using the Alpaca API (Free Tier), collect OHLC and Volume data on SPY, Sector ETFs, and 131 preidentified mega-cap stocks. Using SPY as a benchmark, compute **Real Relative Strength (RRS)** for each Sector ETF and all 131 stocks. Scope: weekly, daily, and hourly (optional) time frames. Derived variables: log normalized closing prices, relative volume, and volatility ratio - weighted more heavily for higher time frames. Computes a composite RRS score (M1 + W1 + D1) using z_scoring method and sorts each stock by it's daily composite RRS z_score for that day.  Daily AMC scans are conducted from CLI/terminal command.  The daily scan logs the entire universe of sector ETF / stock data, and it also generates / returns 3 tables: 1) Sector Performance, 2) Top 10 Strongest Performance, 3) Top 10 Weakest Performance.  

**Purpose 1:** Edge Quanitification - to validate RRS and establish the reliability of RRS as a short duration, directional, price performance indicator. Tracks sector rotation and performance. Answers the quesiton: does a stock appearing in the top 10 produce statistically better forward returns than a random stock from the universe? If yes, by how much, and how consistently? That's your hit rate and your expected value — the two numbers that determine position sizing. 

**Plan:** Scan daily for 30 days.  At 30 days, evaluate price return performance at 5d and 10d for stocks that are reported in the top 10 category in order to establish signal validation direcitonality in stock movement.  

**Task 2:** using Alpaca's API (Free Tier if availbale), collect daily options price data options 30DTE at Strike Prices +/- .30 Delta in order to track volatility term structure data stream.  Collect Implied Volatility (IV) data and Historical Volatility (HV) data in order to derive Implied Volatility Rank (IVR), Implied Volatility Percentile, IV/HV ratio. IVR establishes where current IV is with it's 52 week range. 

Per symbol, per day:
    → Contracts expiring 30–60 DTE
    → 30-delta call + 30-delta put
    → Check bid/ask spread — flag if wide
    → Average the two IVs → daily IV reading
    → Log to CSV alongside scan date
    → After 252 days → full IV percentile history

**Purpose 2:** to establish trade viability (option pricing and potential premium) decision gates for 90DTE credit spreads. "Based on volatility regime for the market and for this individual stock's own IV and HV history, is there premium to collect on this stock?"  Stock's volatility curve, current market volatility regime, volatility slope and trajectory (expanding or fading), position sizing, s this a tradeable instrument or not?

**Plan:** The maturity curve - collect at least 120 days of data to allow the IV data to develop into a signal.

**Task 3:** using Task 2 methodology - derive VIX approximation from SPY options data market volatility term structure and comparison to individual stocks.

**Purpose:** gated logic - "Is this a good market to try to sell credit spreads?"  Gated logic: Trade / No Trade

**Tools Integration:**
 This scanner establishes
  - VIX level: systemic volatility gate "Do I trade?"
  - RS scores: sector and stock-level momentum "Where do I look?"
  - IV metrics: premium opportunity identification "Is there premium to collect here?"
  - Filter stack → specific trade candidates

## Implied Volatility (IV)

- IV is derived by running the Black-Scholes pricing model **in reverse** — plug in the actual market price of an option, solve for the volatility that produces it
- It is not calculated directly; it is *implied by* what the market is already paying
- IV is a mathematical representation of **supply and demand for optionality** — both put buying (fear) and call buying (greed) push IV higher
- Market makers set IV continuously as their best estimate of future realized volatility; their pricing reflects hedging costs, not directional bets
- Same IV number means different things on different stocks — context is everything

## Historical / Realized Volatility (HV)

- HV measures what the stock **actually did** over a lookback period — it's backward-looking
- Calculated from realized price returns, no options involved
- Short-term HV (10-day) vs long-term HV (252-day) can diverge — a spike in short-term HV is a red flag even when long-term HV looks calm
- If short-term HV is accelerating, the stock is already moving against you before you enter

## IV Rank (IVR)

```
IVR = (current IV − 52w low) / (52w high − 52w low) × 100
```

- Measures where current IV sits within its own 52-week range
- IVR 0 = at 52-week low; IVR 100 = at 52-week high
- **Weakness:** a single outlier spike drags the 52-week high up, making current IV look cheap when it isn't
- Use as secondary confirmation, not primary signal

## IV Percentile

- Measures what percentage of days over the past 252 trading days had IV **lower** than today
- IV percentile 80 = IV is higher than it was on 80% of days in the past year
- More robust than IVR for stocks with one-off event spikes
- **Use as primary screen** — IVR confirms

### When IVR and IV Percentile Diverge

If IVR is high but IV percentile is low — a small number of extreme days (earnings blowup, macro shock) are inflating the 52-week high. IV is not actually elevated relative to where it spends most of its time. Pass or investigate.

If IV percentile is high but IVR is moderate — IV is elevated on most days but the 52-week high is an outlier. Still potentially a good entry depending on IV/HV.

## IV / HV Ratio

```
IV/HV ratio = current IV / historical volatility
```

- Ratio of 1.0 = market pricing exactly what stock has realized. No edge.
- **Above 1.0** = market overpaying relative to realized behavior → edge for premium seller
- **Below 1.0** = market underpricing volatility → do not sell
- Threshold: ratio > 1.3–1.5 considered meaningful; > 2.0 is a strong signal
- **Critical caveat:** HV is backward-looking. Regime changes, known catalysts, or accelerating short-term HV can make the ratio misleading

## Why You Need All Three

| Metric | Question it answers |
|--------|-------------------|
| IV percentile | Is IV elevated vs where it spends most of its time? |
| IVR | Confirms percentile isn't an outlier artifact |
| IV/HV ratio | Is the market overpaying vs what the stock actually does? |

No single metric is sufficient. All three elevated = clean signal. Any one failing = reason to pass or size down.

## Term Structure

- IV varies across expiration dates — plotting IV vs expiration produces the **volatility term structure curve**
- In normal conditions: slight upward slope (longer dated = marginally higher IV)
- Around binary events (earnings): near-term IV spikes sharply, creating a **kink** in the curve
- After the event resolves: front-month IV collapses rapidly (**IV crush**)
- Longer-dated options smooth the event into many other trading days — back-month IV barely moves

## Why 90+ DTE Is a Structural Advantage

1. **Avoids the crush** — targeting past the next earnings date means you're selling baseline uncertainty, not event premium; IV metrics are more meaningful signals
2. **Predictable theta** — at 90 DTE, decay is slow and consistent; no white-knuckling through accelerating decay near expiration
3. **Room to manage** — time to roll, adjust strikes, or exit at a smaller loss if the position moves against you
4. **Vega awareness** — longer-dated options have higher vega; price is more sensitive to IV changes. Ideal entry is when IV is elevated and **mean-reverting**, not still climbing

## Systemic vs Idiosyncratic Volatility

**Systemic volatility** — the whole market is turbulent. Every stock's IV is elevated because the environment is fearful, not because any stock is special. Captured by the VIX (S&P 500 30-day implied volatility).

**Idiosyncratic volatility** — elevated IV on a specific stock while the broader market is calm. This is the opportunity. The VIX confirms the ocean is flat; the stock-level filters identify the wave.

**The trap:** high stock IV in a high VIX environment looks identical to high stock IV in a calm VIX environment on a stock-level screen alone. Without checking VIX first, you mistake a hurricane wave for a special wave.

### VIX as an Environment Gate

| VIX level | Direction | Action |
|-----------|-----------|--------|
| < 20 | Stable or falling | Green light — proceed with stock filters |
| 20–30 | Watch direction | Caution — size smaller, be more selective |
| > 30 | Rising | Stand aside — wait for calm |

VIX goes first. If the environment fails, the rest of the scan is academic.

## What to Pull from the Options Chain

- **Which contracts:** 30-delta call + 30-delta put at the nearest expiration past 30 DTE
- **Why 30-delta:** liquid, representative, where institutional hedging concentrates; cleaner IV than ATM
- **Why average call and put:** put-call parity means they should be equal in theory; averaging removes skew noise and hedging demand artifacts
- **Why 30+ DTE expiration:** avoids theta collapse and event distortion in the final weeks
- **Bid/ask spread check:** if spread > ~10–15% of option price, flag as noisy — thin liquidity distorts the IV you back out
- **Result:** two contracts per symbol per day → one clean daily IV reading

## Building IV History

- Day 1–20: too short for any meaningful signal; use for pipeline validation
- Day 60–120: directionally useful, not tradeable; IV percentile has enough observations for a rough compass
- Day 120–180: tradeable with caution; size small, acknowledge incomplete history
- Day 252: full signal — one complete earnings cycle per stock, seasonal patterns, regime shifts captured

**IVR** needs only a rolling high and low — meaningful from day 20, but the range is short until you approach 252 days.

**IV percentile** needs enough observations to be statistically stable — meaningful from ~60 days, reliable at 252.

**In the meantime:** paper trade from day one. Log what you would have done. Validate the filters against real setups before committing real capital.

## Order of Operations (Daily Scan)

```
1. VIX level and direction          ← environment gate
2. RS scan — sectors, then stocks   ← directional bias
3. IV percentile / IVR / IV/HV      ← stock-specific premium elevation
4. Earnings calendar check          ← binary event avoidance
5. Spread construction              ← strike selection, DTE confirmation
```

## The Complete Filter Stack

| Layer | Signal | Threshold |
|-------|--------|-----------|
| Environment | VIX level + direction | < 20 and stable/falling preferred |
| Universe | Large cap, high options OI | 131 symbols across 11 sectors |
| Directional bias | RS composite score + aligned flag | Top quartile → bull put spread; bottom quartile → bear call spread |
| IV elevation | IV percentile | > 50 minimum, > 70 preferred |
| IV elevation | IVR | > 50, confirms percentile |
| Premium richness | IV/HV ratio | > 1.3 minimum, > 1.5 preferred |
| IV timing | IV trend | Peaked and mean-reverting, not still climbing |
| Event avoidance | Earnings date | Target expiration clears next earnings |
| Structure | DTE | 90+ days |

## Mechanics Reminder

**Bearish RS (weak, aligned negative) → Bear Call Spread**
Sell calls above current price. Profit if stock stays below short strike. Betting it doesn't rally past a level.

**Bullish RS (strong, aligned positive) → Bull Put Spread**
Sell puts below current price. Profit if stock stays above short strike. Betting it doesn't crash through a level.

The RS score identifies *where the stock is unlikely to go*. You sell premium on that side.
