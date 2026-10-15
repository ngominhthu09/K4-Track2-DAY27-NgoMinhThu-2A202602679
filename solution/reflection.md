# Reflection

The hardest faults are subtle feature or embedding shifts and partial lineage breaks. Contract violations are directly exposed by the contract tool, while small distribution changes can overlap normal variation. A lineage slice reports the observed graph, so detecting a missing edge depends on knowing the expected upstreams for that model.

The supplied composite-distance example at 1.30 caught every Practice fault but also raised the false-alarm rate. I increased the alert threshold after measuring that tradeoff, then checked the choice on Public and Private. With another pass, I would use `ctx.state` to maintain cautious per-source reference statistics and alert on corroborated shifts, while keeping one relevant metered call per event and evaluating both missed faults and false alarms.
