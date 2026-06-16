"""Unit tests for the BigQuery FinOps audit backend.

These tests are fully offline: the BigQuery client / query execution is mocked,
and warehouse coordinates are supplied via env so no secret files are read.
Focus areas: injection-safe filter binding, SQL assembly, the datamart
registry, and the orchestration helpers.
"""
import os

import pytest

# Set warehouse coordinates before importing the modules so get_dataset_ref()
# never touches the secret files.
os.environ["FASTBI_BQ_AUDIT_WAREHOUSE_PROJECT"] = "test-project"
os.environ["FASTBI_BQ_AUDIT_WAREHOUSE_DATASET"] = "test_dataset"
os.environ["FASTBI_BQ_AUDIT_WAREHOUSE_DATASET_REGION"] = "eu"

from app.datawarehouse_stats import bigquery_audit as audit  # noqa: E402
from app.datawarehouse_stats import bq_audit_client as client  # noqa: E402


# --------------------------------------------------------------------------
# build_filters
# --------------------------------------------------------------------------

def _param_map(params):
    return {p.name: (p.type_, p.value) for p in params}


def test_build_filters_empty():
    where, params = client.build_filters({}, {"date": "day", "project": "project_id"})
    assert where == ""
    assert params == []


def test_build_filters_date_range_inclusive_upper_bound():
    where, params = client.build_filters(
        {"date_from": "2026-05-01", "date_to": "2026-06-01"},
        {"date": "day"},
    )
    assert "`day` >= @date_from" in where
    assert "`day` <= @date_to" in where
    pm = _param_map(params)
    assert pm["date_from"][0] == "TIMESTAMP"
    # date-only upper bound is widened to end-of-day so the whole day is included
    assert pm["date_to"][1].hour == 23 and pm["date_to"][1].minute == 59


def test_build_filters_binds_values_as_parameters_not_interpolated():
    malicious = "x'; DROP TABLE y--"
    where, params = client.build_filters(
        {"project_id": malicious}, {"project": "project_id"}
    )
    # The raw value must never appear in the SQL fragment; only a placeholder.
    assert malicious not in where
    assert "@project_id" in where
    pm = _param_map(params)
    assert pm["project_id"] == ("STRING", malicious)


def test_build_filters_ignores_unsupported_keys():
    # user filter supplied but datamart map only supports project -> dropped
    where, params = client.build_filters(
        {"user_email": "a@b.com", "project_id": "p"},
        {"project": "project_id"},
    )
    assert "user_email" not in where
    assert "@project_id" in where
    assert _param_map(params).keys() == {"project_id"}


def test_build_filters_invalid_date_skipped():
    where, params = client.build_filters({"date_from": "not-a-date"}, {"date": "day"})
    assert where == ""
    assert params == []


# --------------------------------------------------------------------------
# registry + build_query
# --------------------------------------------------------------------------

def test_dataset_ref_uses_env_overrides():
    assert client.get_dataset_ref() == "test-project.test_dataset"


def test_is_valid_datamart():
    assert audit.is_valid_datamart("daily_spend")
    assert not audit.is_valid_datamart("nonexistent")
    assert not audit.is_valid_datamart("daily_spend; DROP")


def test_build_query_structure():
    sql, params = audit.build_query("daily_spend", {})
    assert sql.startswith("SELECT ")
    assert "`test-project.test_dataset.daily_spend`" in sql
    assert "ORDER BY day ASC" in sql
    assert "LIMIT 5000" in sql
    assert params == []


def test_build_query_applies_supported_filters():
    sql, params = audit.build_query(
        "most_expensive_jobs",
        {"date_from": "2026-05-01", "project_id": "p", "user_email": "u@x.com"},
    )
    assert "`hour` >= @date_from" in sql
    assert "@project_id" in sql
    assert "@user_email" in sql
    assert {p.name for p in params} == {"date_from", "project_id", "user_email"}


def test_build_query_unknown_datamart_raises():
    with pytest.raises(KeyError):
        audit.build_query("definitely_not_real", {})


def test_registry_specs_are_well_formed():
    for name, spec in audit.DATAMARTS.items():
        assert spec["select"].strip(), f"{name} has empty select"
        assert spec["order"].strip(), f"{name} has no order"
        assert isinstance(spec["limit"], int) and spec["limit"] > 0
        assert isinstance(spec["filters"], dict)
        # filter keys must be from the known generic set
        assert set(spec["filters"]).issubset({"date", "project", "user", "model", "category"})


def test_categories_reference_valid_datamarts():
    for category, names in audit.CATEGORIES.items():
        for name in names:
            assert audit.is_valid_datamart(name), f"{category} references unknown {name}"


# --------------------------------------------------------------------------
# orchestration (mocked run_query)
# --------------------------------------------------------------------------

def test_fetch_datamart_executes_built_query(monkeypatch):
    captured = {}

    def fake_run_query(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return [{"day": "2026-05-01", "cost_category": "compute", "cost": 10.0}]

    monkeypatch.setattr(client, "run_query", fake_run_query)
    rows = audit.fetch_datamart("daily_spend", {"cost_category": "compute"})
    assert rows[0]["cost"] == 10.0
    assert "daily_spend" in captured["sql"]
    assert any(p.name == "cost_category" for p in captured["params"])


def test_fetch_datamart_rejects_unknown(monkeypatch):
    monkeypatch.setattr(client, "run_query", lambda *a, **k: [])
    with pytest.raises(KeyError):
        audit.fetch_datamart("../etc/passwd", {})


def test_registry_exposes_every_dataset_object():
    # Full coverage of every table/view in the monitoring dataset (global service):
    # 34 documented datamarts + config/audit + reservations + raw/backing models.
    assert len(audit.DATAMARTS) == 57


def test_get_summary_sources_cost_from_daily_spend(monkeypatch):
    calls = []

    def fake_run_query(sql, params=None):
        calls.append(sql)
        if "daily_spend" in sql:
            return [{"spend_last_30d": 3500.0, "compute_last_30d": 3400.0, "storage_last_30d": 100.0}]
        if "project_cost_summary" in sql:
            return [{"storage_monthly_forecast": 80.0, "storage_potential_savings": 60.0,
                     "tables_monitored": 3000, "projects_monitored": 1, "unique_users": 4000}]
        if "recommendations" in sql:
            return [{"active_recommendations": 2, "rec_savings": 15.0}]
        return []

    monkeypatch.setattr(client, "run_query", fake_run_query)
    summary = audit.get_summary({})
    # Cost comes from billing export, not project_cost_summary estimate.
    assert summary["spend_last_30d"] == 3500.0
    assert summary["compute_last_30d"] == 3400.0
    assert summary["storage_last_30d"] == 100.0
    assert summary["tables_monitored"] == 3000
    # Combined potential savings = storage savings + recommendation savings.
    assert summary["potential_savings"] == 75.0
    assert len(calls) == 3


def test_get_summary_potential_savings_handles_nulls(monkeypatch):
    def fake_run_query(sql, params=None):
        if "daily_spend" in sql:
            return [{"spend_last_30d": 10.0}]
        if "project_cost_summary" in sql:
            return [{"storage_potential_savings": None}]
        if "recommendations" in sql:
            return [{"active_recommendations": 0, "rec_savings": None}]
        return []

    monkeypatch.setattr(client, "run_query", fake_run_query)
    summary = audit.get_summary({})
    assert summary["potential_savings"] == 0


def test_get_filter_options_maps_columns(monkeypatch):
    def fake_run_query(sql, params=None):
        if "project_cost_summary" in sql:
            return [{"project_id": "p1"}, {"project_id": "p2"}]
        if "most_expensive_users" in sql:
            return [{"user_email": "a@x.com"}]
        if "most_expensive_models" in sql:
            return [{"dbt_model_name": "stg_orders"}]
        if "daily_spend" in sql:
            return [{"cost_category": "compute"}, {"cost_category": "storage"}]
        return []

    monkeypatch.setattr(client, "run_query", fake_run_query)
    opts = audit.get_filter_options()
    assert opts["projects"] == ["p1", "p2"]
    assert opts["users"] == ["a@x.com"]
    assert opts["dbt_models"] == ["stg_orders"]
    assert opts["cost_categories"] == ["compute", "storage"]


def test_convert_value_serializes_datetimes():
    from datetime import datetime
    out = client._convert_value({"t": datetime(2026, 5, 1, 12, 30, 0), "n": 5})
    assert out["t"] == "2026-05-01T12:30:00"
    assert out["n"] == 5
