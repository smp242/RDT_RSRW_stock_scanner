---
name: Trading strategy context
description: User's primary trading strategy — credit spreads 90+ DTE with elevated premiums
type: user
---

User is focused on selling credit spreads (not day trading) with:
- **DTE target: 90+ days out** — theta decay over a longer horizon
- **Entry criterion: elevated premiums** — wants to sell when IV is high relative to history
- **Not intraday** — async fetch speed optimization is low priority for their workflow

**How to apply:** Scanner suggestions and improvements should be framed around supporting credit spread selection — directional conviction (which way to lean the spread) and premium elevation (is IV rich enough to sell). The RS/momentum engine provides the directional filter; IV rank/percentile is the missing layer for premium assessment.
