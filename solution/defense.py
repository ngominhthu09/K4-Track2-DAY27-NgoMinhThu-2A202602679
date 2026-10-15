"""Streaming detectors for the Data Siege lab."""
from api import Verdict


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def _failed(result):
    return not isinstance(result, dict) or "error" in result


def check_data_batch(payload, ctx):
    result = ctx.tools.batch_profile(payload["batch_id"])
    if _failed(result):
        return Verdict(alert=False, reason="batch profile unavailable", pillar="checks")

    base = ctx.baseline
    null_rates = result.get("null_rate", {})
    null_rate = null_rates.get("customer_id") if isinstance(null_rates, dict) else None
    row_count = result.get("row_count")
    mean_amount = result.get("mean_amount")
    stale = result.get("staleness_min")

    reasons = []
    if row_count is not None and (
        row_count < base["row_count_min"] or row_count > base["row_count_max"]
    ):
        reasons.append("row_count outside calibrated range")
    if null_rate is not None and null_rate > base["null_rate_max"]:
        reasons.append("customer_id null rate too high")
    if mean_amount is not None and (
        mean_amount < base["mean_amount_min"] or mean_amount > base["mean_amount_max"]
    ):
        reasons.append("mean_amount outside calibrated range")
    if stale is not None and stale > base["staleness_min_max"]:
        reasons.append("batch is stale")

    return Verdict(alert=bool(reasons), reason="; ".join(reasons), pillar="checks")


def check_contract_checkpoint(payload, ctx):
    result = ctx.tools.contract_diff(
        payload["contract_id"], payload["checkpoint_batch_id"]
    )
    if _failed(result):
        return Verdict(alert=False, reason="contract diff unavailable", pillar="contracts")

    violations = result.get("violations", [])
    delay = result.get("freshness_delay_min")
    reasons = []
    if violations:
        reasons.append("contract violation: " + ", ".join(violations))
    if delay is not None and delay > ctx.baseline["freshness_delay_max_min"]:
        reasons.append("freshness delay above calibrated maximum")

    return Verdict(alert=bool(reasons), reason="; ".join(reasons), pillar="contracts")


def check_lineage_run(payload, ctx):
    result = ctx.tools.lineage_graph_slice(payload["run_id"])
    if _failed(result):
        return Verdict(alert=False, reason="lineage slice unavailable", pillar="lineage")

    duration = result.get("duration_ms")
    actual_upstream = result.get("actual_upstream")
    downstream_count = result.get("actual_downstream_count")
    reasons = []
    if duration is not None and duration > ctx.baseline["lineage_duration_ms_max"]:
        reasons.append("lineage runtime above calibrated maximum")

    expected_upstream = payload.get("expected_upstream")
    if expected_upstream is not None and actual_upstream is not None:
        if sorted(expected_upstream) != sorted(actual_upstream):
            reasons.append("upstream lineage differs from expectation")
    elif actual_upstream is not None and len(actual_upstream) == 0:
        reasons.append("lineage has no upstream edges")
    if downstream_count is not None and downstream_count == 0:
        reasons.append("lineage output has no downstream edges")

    return Verdict(alert=bool(reasons), reason="; ".join(reasons), pillar="lineage")


def check_feature_materialization(payload, ctx):
    result = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    if _failed(result):
        return Verdict(alert=False, reason="feature drift unavailable", pillar="ai_infra")

    shift = result.get("mean_shift_sigma")
    alert = shift is not None and shift > ctx.baseline["feature_mean_shift_sigma_max"]
    reason = "feature mean shift above calibrated maximum" if alert else ""
    return Verdict(alert=alert, reason=reason, pillar="ai_infra")


def check_embedding_batch(payload, ctx):
    result = ctx.tools.embedding_drift(payload["corpus"], payload["chunk_batch_id"])
    if _failed(result):
        return Verdict(alert=False, reason="embedding drift unavailable", pillar="ai_infra")

    reasons = []
    shift = result.get("centroid_shift")
    age = result.get("avg_doc_age_days")
    if shift is not None and shift > ctx.baseline["embedding_centroid_shift_max"]:
        reasons.append("embedding centroid shift above calibrated maximum")
    if age is not None and age > ctx.baseline["corpus_avg_doc_age_days_max"]:
        reasons.append("corpus documents are too old")

    return Verdict(alert=bool(reasons), reason="; ".join(reasons), pillar="ai_infra")
