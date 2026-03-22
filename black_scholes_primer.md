# Black Scholes Options Model

Variables:
  - S — spot price (current stock price) -> if the spot price increases for the underlying, the probability of the option ending up in the money increases
  
  - K — strike price -> Intrinsic vs Extrinsin Value
  - T — time to maturity -> More time = Higher Probability that option might end ITM
  - r — risk-free rate -> If rates are high, the cost of owning the stock outright is higher — you're giving up that 5% risk-free return to tie up capital in shares. The option lets you control the same shares for much less capital, leaving the rest to earn that 5%


  - σ (sigma) — volatility

In the original formula — σ is HV. Black-Scholes was designed to take historical volatility as an input and produce a theoretical option price as an output. You plug in what the stock has been doing and the model tells you what the option should be worth.

In practice — σ is IV. And this is where it gets interesting. The market doesn't use Black-Scholes to calculate prices from volatility. The market sets prices through supply and demand directly. Black-Scholes gets run in reverse — plug in the market price, solve for σ. The σ you back out is IV.

Formula:

$$
d_1 = \frac{\ln(\frac{S_t}{K}) + (r + \frac{\sigma^2}{2})t}{\sigma\sqrt{t}}
$$

What N() actually is

  N() is the cumulative normal distribution — it takes a number and returns the probability that a standard normal random variable falls below that number. In
  plain terms: it converts a position on a bell curve into a probability between 0 and 1.

  So N(0) = 0.50 — exactly at the mean, 50% probability.
  N(1) ≈ 0.84 — one standard deviation above mean, 84% probability.
  N(-1) ≈ 0.16 — one standard deviation below mean, 16% probability.

  That's the machinery. Now what are we feeding into it.

  ---
  What d1 and d2 actually measure

  Before the formula — what is a call option fundamentally worth?

  Two things have to be true for a call to pay off at expiration:
  1. The stock has to finish above the strike
  2. When it does, you capture the difference between stock price and strike

  Black-Scholes prices both of those components separately and adds them together. That's the structure of C = N(d1)St - N(d2)Ke^-rt.

  N(d2) — the simpler one — is the probability that the option expires in the money. It answers: what are the odds the stock finishes above the strike? This is
  the pure probability term.

  N(d1) — is slightly more complex. It's not just the probability of finishing ITM — it's the probability weighted by how much the stock is expected to be above
   the strike. Think of it as delta — how much does the option's value move per $1 move in the stock. It accounts for the magnitude of the payoff, not just the
  likelihood.

  ---
  Why subtract them

  Look at the formula again:

  C = N(d1) × St  −  N(d2) × Ke^-rt

  Read it in plain English:

  First term N(d1) × St — the probability-weighted value of receiving the stock. What you get.

  Second term N(d2) × Ke^-rt — the probability-weighted present value of paying the strike. What you pay. The e^-rt is just discounting the strike price back to
   today using the risk-free rate — a dollar paid in the future is worth less than a dollar today.

  So the formula is literally: expected value of what you receive minus expected value of what you pay. The option is worth the probability-weighted difference
  between getting the stock and paying the strike.

  ---
  Tie it back to your inputs

  Now look at d1 and d2 and you'll recognize every piece:

  d1 = [ln(S/K) + (r + σ²/2)t] / σ√t
  d2 = d1 - σ√t

  - ln(S/K) — how far the stock is from the strike right now, in log terms. How ITM or OTM you are.
  - r + σ²/2)t — drift over time. How much the stock is expected to move upward given rates and volatility.
  - σ√t — total uncertainty over the life of the option. Volatility scaled by time.

  d1 and d2 are both just standardized measures of "how likely is this option to finish ITM" — d2 gives you the raw probability, d1 adjusts it for the size of
  the payoff.

  ---
  The whole thing in one sentence

  A call option is worth the probability-weighted value of receiving the stock, minus the probability-weighted present value of paying the strike — where both
  probabilities are derived from how far the stock is from the strike, how much time remains, and how volatile the stock is.

N(d1) is always larger than N(d2) because it's not just asking "does this option finish ITM" — it's asking "what's the expected value of the
  stock given it finishes ITM." When you condition on finishing in the money, the expected stock price is always higher than the strike by some amount. That
  upside magnitude pulls N(d1) above N(d2).

  The difference between them — N(d1) - N(d2) — represents the value of that magnitude. It's the extra compensation you're pricing in for how far ITM the option
   might finish, not just whether it finishes ITM.

  This is also why deep ITM options have N(d1) and N(d2) converging toward 1.0 and moving together — the probability of finishing ITM is near certain, and the
  magnitude question becomes less important relative to intrinsic value. Deep OTM options have both converging toward 0 for the same reason in reverse.

The Greeks are just derivatives of the Black-Scholes formula with respect to each input.

  That word "derivative" is calculus — it means the rate of change of option price when you move one input slightly while holding everything else constant. You
  already built the intuition for this without knowing it:

  - You know that when S increases, call value increases → the rate of that relationship is Delta (derivative with respect to S)
  - You know that when T decreases, option value decreases → the rate of that relationship is Theta (derivative with respect to T)
  - You know that when σ increases, option value increases → the rate of that relationship is Vega (derivative with respect to σ)
  - You know that when r increases, call value increases → the rate of that relationship is Rho (derivative with respect to r)

  Gamma is the one that doesn't map directly to an input — it's the derivative of Delta with respect to S. The rate of change of the rate of change. How fast
  Delta itself is moving as the stock moves.

  ---
  In a table:

  ┌───────┬────────────────────────────────────────────────────┬─────────────────────────┐
  │ Greek │                  What it measures                  │ Input it differentiates │
  ├───────┼────────────────────────────────────────────────────┼─────────────────────────┤
  │ Delta │ How much option price moves per $1 in stock        │ S                       │
  ├───────┼────────────────────────────────────────────────────┼─────────────────────────┤
  │ Theta │ How much option price decays per day               │ T                       │
  ├───────┼────────────────────────────────────────────────────┼─────────────────────────┤
  │ Vega  │ How much option price moves per 1% change in IV    │ σ                       │
  ├───────┼────────────────────────────────────────────────────┼─────────────────────────┤
  │ Rho   │ How much option price moves per 1% change in rates │ r                       │
  ├───────┼────────────────────────────────────────────────────┼─────────────────────────┤
  │ Gamma │ How fast Delta changes per $1 in stock             │ Delta (2nd order)       │
  └───────┴────────────────────────────────────────────────────┴─────────────────────────┘

  ---
  The reason this matters for you practically:

  You don't need to compute the Greeks yourself — Alpaca returns them directly in the options snapshot. But now you know they're not magic numbers — they're
  just the Black-Scholes formula measured from different angles. Each one answers "how sensitive is this option's price to this specific input?"

  For your credit spread strategy the two you'll care most about are Theta (your friend — time decay working for you as a seller) and Vega (your risk — IV
  expansion working against you). Delta tells you your directional exposure. Gamma tells you how fast that exposure is changing as the stock moves toward your
  strike.
