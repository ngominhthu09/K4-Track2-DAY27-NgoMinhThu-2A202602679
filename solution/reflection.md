# Reflection

The hardest faults to catch are partial lineage changes and subtle drift. A lineage slice shows the actual upstream and downstream graph, but when the event does not provide an expected graph, a missing edge can look like a valid smaller graph. Small feature or embedding shifts can also sit close to normal variation, so fixed calibrated limits favor fewer false alarms over catching every subtle fault.

With another pass, I would use `ctx.state` to build cautious online reference profiles for repeated transforms or data sources, and alert on sustained changes rather than one borderline value. I would keep one relevant metered call per event and tune those adaptive thresholds on practice to preserve the current low false-alarm rate and stay within budget.
