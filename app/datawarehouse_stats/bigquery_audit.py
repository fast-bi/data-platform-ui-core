"""BigQuery FinOps datamart access (dbt-bigquery-monitoring).

Exposes the pre-built consumption datamarts from the
``dbt-bigquery-monitoring`` package as a data-driven registry. Each entry
declares the scalar columns to select (RECORD/nested columns are avoided so
results serialize cleanly to JSON), which generic filters it supports, and a
default ordering + row cap.

The API layer calls :func:`fetch_datamart` with a datamart name (validated
against the registry) and the raw request filters.
"""
import logging

from app.datawarehouse_stats import bq_audit_client as client

logger = logging.getLogger(__name__)

# Query text can be very large; cap it for transport/display. The UI provides
# a "view / copy" affordance, so we keep enough to be useful for analysis.
_QUERY_SNIPPET = "SUBSTR(query, 0, 5000) AS query"

# --- Datamart registry -----------------------------------------------------
# key:        the view name in the monitoring dataset (also the API id)
# select:     explicit scalar projection (no RECORD columns)
# filters:    generic-filter-key -> column name (see build_filters)
# order:      ORDER BY expression
# limit:      max rows returned
# category:   UI grouping
DATAMARTS = {
    # ---- Overview / Global ----
    "daily_spend": {
        "category": "overview",
        "select": "day, cost_category, ROUND(cost, 2) AS cost",
        "filters": {"date": "day", "category": "cost_category"},
        "order": "day ASC",
        "limit": 5000,
    },
    "cost_trend_comparison": {
        "category": "overview",
        "select": (
            "day, cost_category, ROUND(daily_cost, 2) AS daily_cost, "
            "ROUND(rolling_7d_avg_cost, 2) AS rolling_7d_avg_cost, "
            "ROUND(rolling_30d_avg_cost, 2) AS rolling_30d_avg_cost, "
            "ROUND(cost_same_day_last_week, 2) AS cost_same_day_last_week, "
            "ROUND(wow_pct_change, 2) AS wow_pct_change, "
            "ROUND(pct_deviation_from_7d_avg, 2) AS pct_deviation_from_7d_avg"
        ),
        "filters": {"date": "day", "category": "cost_category"},
        "order": "day ASC",
        "limit": 5000,
    },
    "project_cost_summary": {
        "category": "overview",
        "select": (
            "project_id, ROUND(compute_cost, 2) AS compute_cost, "
            "ROUND(storage_monthly_forecast, 2) AS storage_monthly_forecast, "
            "ROUND(total_estimated_cost, 2) AS total_estimated_cost, "
            "ROUND(storage_potential_savings, 2) AS storage_potential_savings, "
            "query_count, unique_users, table_count"
        ),
        "filters": {"project": "project_id"},
        "order": "total_estimated_cost DESC",
        "limit": 500,
    },
    "recommendations": {
        "category": "overview",
        "select": (
            "project_id, recommender_label, subtype, description, priority, "
            "priority_rank, impact_category, "
            "ROUND(estimated_monthly_savings, 2) AS estimated_monthly_savings, "
            "target_resources, last_updated_time"
        ),
        "filters": {"project": "project_id"},
        "order": "priority_rank ASC, estimated_monthly_savings DESC",
        "limit": 500,
    },
    # ---- Compute / Queries ----
    "most_expensive_jobs": {
        "category": "compute",
        "select": (
            f"hour, project_id, user_email, job_id, statement_type, "
            f"{_QUERY_SNIPPET}, cache_hit, ROUND(query_cost, 4) AS query_cost, "
            "total_slot_ms, total_time_seconds, total_bytes_billed"
        ),
        "filters": {"date": "hour", "project": "project_id", "user": "user_email"},
        "order": "query_cost DESC",
        "limit": 200,
    },
    "slowest_jobs": {
        "category": "compute",
        "select": (
            f"hour, project_id, user_email, job_id, statement_type, "
            f"{_QUERY_SNIPPET}, cache_hit, ROUND(query_cost, 4) AS query_cost, "
            "total_slot_ms, total_time_seconds, total_bytes_billed"
        ),
        "filters": {"date": "hour", "project": "project_id", "user": "user_email"},
        "order": "total_time_seconds DESC",
        "limit": 200,
    },
    "most_repeated_jobs": {
        "category": "compute",
        "select": (
            f"{_QUERY_SNIPPET}, ROUND(cache_hit_ratio, 2) AS cache_hit_ratio, "
            "ROUND(total_query_cost, 4) AS total_query_cost, total_slot_ms, "
            "query_count"
        ),
        "filters": {},
        "order": "query_count DESC",
        "limit": 200,
    },
    "job_failure_analysis": {
        "category": "compute",
        "select": (
            "error_reason, error_message, error_count, "
            "ROUND(total_failed_cost, 4) AS total_failed_cost, "
            "ROUND(avg_slot_seconds, 2) AS avg_slot_seconds, "
            "first_occurrence, last_occurrence, duration_hours"
        ),
        "filters": {},
        "order": "error_count DESC",
        "limit": 200,
    },
    "error_rate_over_time": {
        "category": "compute",
        "select": (
            "day, project_id, user_email, total_jobs, failed_jobs, "
            "ROUND(error_rate_pct, 2) AS error_rate_pct, "
            "ROUND(wasted_cost_on_failures, 4) AS wasted_cost_on_failures, "
            "ROUND(rolling_7d_avg_error_rate_pct, 2) AS rolling_7d_avg_error_rate_pct"
        ),
        "filters": {"date": "day", "project": "project_id", "user": "user_email"},
        "order": "day ASC",
        "limit": 5000,
    },
    "materialization_candidates": {
        "category": "compute",
        "select": (
            f"{_QUERY_SNIPPET}, query_count, "
            "ROUND(total_query_cost, 4) AS total_query_cost, "
            "ROUND(avg_query_cost, 4) AS avg_query_cost, "
            "ROUND(cache_hit_ratio, 2) AS cache_hit_ratio, "
            "ROUND(potential_savings_from_caching, 4) AS potential_savings_from_caching, "
            "ROUND(avg_duration_seconds, 2) AS avg_duration_seconds, "
            "ROUND(total_gb_processed, 2) AS total_gb_processed, "
            "materialization_recommendation"
        ),
        "filters": {},
        "order": "potential_savings_from_caching DESC",
        "limit": 200,
    },
    "cost_per_project": {
        "category": "compute",
        "select": (
            "day, project_id, ROUND(total_query_cost, 2) AS total_query_cost, "
            "ROUND(total_failing_query_cost, 2) AS total_failing_query_cost, "
            "query_count, unique_users, "
            "ROUND(cache_hit_ratio, 2) AS cache_hit_ratio, "
            "ROUND(avg_duration_seconds, 2) AS avg_duration_seconds"
        ),
        "filters": {"date": "day", "project": "project_id"},
        "order": "day ASC",
        "limit": 5000,
    },
    "cost_by_label": {
        "category": "compute",
        "select": (
            "day, label_key, label_value, project_id, query_count, "
            "ROUND(total_query_cost, 2) AS total_query_cost, "
            "ROUND(avg_query_cost, 4) AS avg_query_cost, "
            "ROUND(cache_hit_ratio, 2) AS cache_hit_ratio"
        ),
        "filters": {"date": "day", "project": "project_id"},
        "order": "total_query_cost DESC",
        "limit": 500,
    },
    "query_with_better_pricing_using_flat_pricing_view": {
        "category": "compute",
        "select": (
            f"hour, project_id, user_email, dbt_model_name, {_QUERY_SNIPPET}, "
            "ROUND(query_cost, 4) AS query_cost, "
            "ROUND(flat_pricing_query_cost, 4) AS flat_pricing_query_cost, "
            "ROUND(ondemand_query_cost, 4) AS ondemand_query_cost, "
            "ROUND(cost_savings, 4) AS cost_savings, "
            "ROUND(cost_savings_pct, 2) AS cost_savings_pct"
        ),
        "filters": {
            "date": "hour",
            "project": "project_id",
            "user": "user_email",
            "model": "dbt_model_name",
        },
        "order": "cost_savings DESC",
        "limit": 200,
    },
    "query_with_better_pricing_using_on_demand_view": {
        "category": "compute",
        "select": (
            f"hour, project_id, user_email, dbt_model_name, {_QUERY_SNIPPET}, "
            "ROUND(query_cost, 4) AS query_cost, "
            "ROUND(flat_pricing_query_cost, 4) AS flat_pricing_query_cost, "
            "ROUND(ondemand_query_cost, 4) AS ondemand_query_cost, "
            "ROUND(cost_savings, 4) AS cost_savings, "
            "ROUND(cost_savings_pct, 2) AS cost_savings_pct"
        ),
        "filters": {
            "date": "hour",
            "project": "project_id",
            "user": "user_email",
            "model": "dbt_model_name",
        },
        "order": "cost_savings DESC",
        "limit": 200,
    },
    # ---- Users ----
    "most_expensive_users": {
        "category": "users",
        "select": (
            "day, user_email, ROUND(avg_query_cost, 4) AS avg_query_cost, "
            "ROUND(total_query_cost, 2) AS total_query_cost, total_slot_ms, "
            "query_count, "
            "ROUND(avg_slot_seconds_per_query, 2) AS avg_slot_seconds_per_query, "
            "ROUND(cache_hit_ratio, 2) AS cache_hit_ratio"
        ),
        "filters": {"date": "day", "user": "user_email"},
        "order": "total_query_cost DESC",
        "limit": 1000,
    },
    # ---- dbt Models ----
    "most_expensive_models": {
        "category": "dbt",
        "select": (
            "dbt_model_name, ROUND(cache_hit_ratio, 2) AS cache_hit_ratio, "
            "ROUND(total_query_cost, 2) AS total_query_cost, total_slot_ms, "
            "query_count"
        ),
        "filters": {"model": "dbt_model_name"},
        "order": "total_query_cost DESC",
        "limit": 200,
    },
    "most_repeated_models": {
        "category": "dbt",
        "select": (
            "dbt_model_name, ROUND(cache_hit_ratio, 2) AS cache_hit_ratio, "
            "ROUND(total_query_cost, 2) AS total_query_cost, total_slot_ms, "
            "query_count"
        ),
        "filters": {"model": "dbt_model_name"},
        "order": "query_count DESC",
        "limit": 200,
    },
    "dbt_model_trends": {
        "category": "dbt",
        "select": (
            "day, dbt_model_name, query_count, "
            "ROUND(total_query_cost, 2) AS total_query_cost, "
            "ROUND(avg_query_cost, 4) AS avg_query_cost, "
            "ROUND(avg_duration_seconds, 2) AS avg_duration_seconds, "
            "ROUND(p90_duration_seconds, 2) AS p90_duration_seconds, "
            "max_duration_seconds, failed_runs, "
            "ROUND(failure_rate, 2) AS failure_rate, "
            "ROUND(cache_hit_ratio, 2) AS cache_hit_ratio, total_bytes_billed"
        ),
        "filters": {"date": "day", "model": "dbt_model_name"},
        "order": "day ASC",
        "limit": 5000,
    },
    # ---- Storage ----
    "most_expensive_tables": {
        "category": "storage",
        "select": (
            "project_id, dataset_id, table_id, table_type, total_rows, "
            "ROUND(total_logical_tb, 4) AS total_logical_tb, "
            "ROUND(total_physical_tb, 4) AS total_physical_tb, "
            "ROUND(cost_monthly_forecast, 2) AS cost_monthly_forecast, "
            "storage_billing_model, optimal_storage_billing_model, "
            "ROUND(potential_savings, 2) AS potential_savings"
        ),
        "filters": {"project": "project_id"},
        "order": "cost_monthly_forecast DESC",
        "limit": 200,
    },
    "unused_tables": {
        "category": "storage",
        "select": (
            "project_id, dataset_id, table_id, table_type, "
            "ROUND(total_logical_tb, 4) AS total_logical_tb, "
            "ROUND(cost_monthly_forecast, 2) AS cost_monthly_forecast, "
            "last_used_date, storage_last_modified_time"
        ),
        "filters": {"project": "project_id"},
        "order": "cost_monthly_forecast DESC",
        "limit": 200,
    },
    "write_only_tables": {
        "category": "storage",
        "select": (
            "project_id, dataset_id, table_id, table_type, last_read_date, "
            "total_reference_count, days_since_last_write, "
            "ROUND(cost_monthly_forecast, 2) AS cost_monthly_forecast, "
            "total_logical_bytes"
        ),
        "filters": {"project": "project_id"},
        "order": "cost_monthly_forecast DESC",
        "limit": 200,
    },
    "read_heavy_tables": {
        "category": "storage",
        "select": (
            "project_id, dataset_id, table_id, table_type, reference_count, "
            "ROUND(total_logical_tb, 4) AS total_logical_tb, "
            "ROUND(cost_monthly_forecast, 2) AS cost_monthly_forecast"
        ),
        "filters": {"project": "project_id"},
        "order": "reference_count DESC",
        "limit": 200,
    },
    "dataset_with_cost": {
        "category": "storage",
        "select": (
            "project_id, dataset_id, total_rows, total_partitions, "
            "ROUND(total_logical_tb, 4) AS total_logical_tb, "
            "ROUND(total_physical_tb, 4) AS total_physical_tb, "
            "ROUND(logical_cost_monthly_forecast, 2) AS logical_cost_monthly_forecast, "
            "ROUND(physical_cost_monthly_forecast, 2) AS physical_cost_monthly_forecast, "
            "prefer_physical_pricing_model, "
            "ROUND(storage_pricing_model_difference, 2) AS storage_pricing_model_difference"
        ),
        "filters": {"project": "project_id"},
        "order": "logical_cost_monthly_forecast DESC",
        "limit": 500,
    },
    "partitions_monitoring": {
        "category": "storage",
        "select": (
            "project_id, dataset_id, table_id, partition_type, "
            "partition_expiration_days, partition_count, sum_total_logical_bytes, "
            "earliest_partition_time, latest_partition_time, max_last_updated_time"
        ),
        "filters": {"project": "project_id"},
        "order": "partition_count DESC",
        "limit": 500,
    },
    # ---- Recommendations & Savings ----
    "table_with_potential_savings": {
        "category": "savings",
        "select": (
            "project_id, dataset_id, table_id, "
            "ROUND(total_logical_tb, 4) AS total_logical_tb, "
            "ROUND(total_physical_tb, 4) AS total_physical_tb, "
            "ROUND(logical_cost_monthly_forecast, 2) AS logical_cost_monthly_forecast, "
            "ROUND(physical_cost_monthly_forecast, 2) AS physical_cost_monthly_forecast, "
            "optimal_storage_billing_model, "
            "ROUND(potential_savings, 2) AS potential_savings"
        ),
        "filters": {"project": "project_id"},
        "order": "potential_savings DESC",
        "limit": 200,
    },
    "dataset_with_potential_savings": {
        "category": "savings",
        "select": (
            "project_id, dataset_id, storage_billing_model, "
            "ROUND(total_logical_tb, 4) AS total_logical_tb, "
            "ROUND(total_physical_tb, 4) AS total_physical_tb, "
            "ROUND(logical_cost_monthly_forecast, 2) AS logical_cost_monthly_forecast, "
            "ROUND(physical_cost_monthly_forecast, 2) AS physical_cost_monthly_forecast, "
            "ROUND(maximum_potential_savings, 2) AS maximum_potential_savings, "
            "optimal_storage_billing_model, "
            "ROUND(potential_savings, 2) AS potential_savings"
        ),
        "filters": {"project": "project_id"},
        "order": "maximum_potential_savings DESC",
        "limit": 200,
    },
    # ---- Compute / Time granularity ----
    "compute_cost_per_hour_view": {
        "category": "compute",
        "select": (
            "hour, project_id, reservation_id, "
            "ROUND(total_query_cost, 2) AS total_query_cost, query_count, "
            "unique_users, cache_hits, "
            "ROUND(avg_duration_seconds, 2) AS avg_duration_seconds, total_bytes_billed"
        ),
        "filters": {"date": "hour", "project": "project_id"},
        "order": "hour ASC",
        "limit": 5000,
    },
    "compute_cost_per_minute_view": {
        "category": "compute",
        "select": (
            "minute, project_id, "
            "ROUND(total_query_cost, 2) AS total_query_cost, query_count, "
            "unique_users, cache_hits, total_bytes_billed"
        ),
        "filters": {"date": "minute", "project": "project_id"},
        "order": "minute DESC",
        "limit": 5000,
    },
    "job_timeline_analysis": {
        "category": "compute",
        "select": (
            "project_id, user_email, job_type, statement_type, pricing_model, "
            "queue_performance, slot_efficiency, duration_category, job_count, "
            "ROUND(avg_duration_seconds, 2) AS avg_duration_seconds, "
            "ROUND(avg_queue_seconds, 2) AS avg_queue_seconds, "
            "ROUND(avg_slot_utilization_pct, 2) AS avg_slot_utilization_pct, "
            "ROUND(total_gb_processed, 2) AS total_gb_processed, "
            "ROUND(cache_hit_rate_pct, 2) AS cache_hit_rate_pct, "
            "optimization_recommendation"
        ),
        "filters": {"project": "project_id", "user": "user_email"},
        "order": "job_count DESC",
        "limit": 200,
    },
    "bi_engine_materialized_view_analysis": {
        "category": "compute",
        "select": (
            "project_id, bi_engine_mode, bi_engine_queries, "
            "ROUND(bi_engine_avg_slot_seconds, 2) AS bi_engine_avg_slot_seconds, "
            "ROUND(bi_engine_cache_hit_percentage, 2) AS bi_engine_cache_hit_percentage, "
            "bi_engine_performance_tier, optimization_recommendation"
        ),
        "filters": {"project": "project_id"},
        "order": "bi_engine_queries DESC",
        "limit": 200,
    },
    # ---- Storage / Write ingestion & billing ----
    "write_ingestion_cost_per_table": {
        "category": "storage",
        "select": (
            "day, project_id, dataset_id, table_id, source_type, total_requests, "
            "total_rows, ROUND(total_input_mb, 2) AS total_input_mb, error_count, "
            "ROUND(error_rate, 2) AS error_rate, "
            "ROUND(estimated_cost, 4) AS estimated_cost"
        ),
        "filters": {"date": "day", "project": "project_id"},
        "order": "estimated_cost DESC",
        "limit": 500,
    },
    "write_ingestion_errors_analysis": {
        "category": "storage",
        "select": (
            "project_id, dataset_id, table_id, source_type, error_code, "
            "error_request_count, error_row_count, first_occurrence, "
            "last_occurrence, ROUND(error_rate, 2) AS error_rate"
        ),
        "filters": {"project": "project_id"},
        "order": "error_request_count DESC",
        "limit": 200,
    },
    "storage_billing_per_hour": {
        "category": "storage",
        "select": "hour, storage_type, ROUND(storage_cost, 4) AS storage_cost",
        "filters": {"date": "hour"},
        "order": "hour ASC",
        "limit": 5000,
    },
    "reservation_usage_per_hour": {
        "category": "compute",
        "select": (
            "hour, reservation_id, project_id, slots_assigned, slots_max_assigned, "
            "total_slot_ms, query_count, "
            "ROUND(total_query_cost, 2) AS total_query_cost, "
            "ROUND(slot_utilization_ratio, 2) AS slot_utilization_ratio, "
            "ROUND(max_slot_utilization_ratio, 2) AS max_slot_utilization_ratio, "
            "utilization_category, autoscaling_status"
        ),
        "filters": {"date": "hour", "project": "project_id"},
        "order": "hour DESC",
        "limit": 5000,
    },
    # ---- Configuration & Audit ----
    "dbt_bigquery_monitoring_options": {
        "category": "config",
        "select": "option_label, option_value",
        "filters": {},
        "order": "option_label ASC",
        "limit": 500,
    },
    "dataset_options": {
        "category": "config",
        "select": "project_id, dataset_id, storage_billing_model",
        "filters": {"project": "project_id"},
        "order": "project_id ASC, dataset_id ASC",
        "limit": 2000,
    },
    "information_schema_effective_project_options": {
        "category": "config",
        "select": (
            "project_id, project_number, option_name, option_description, "
            "option_type, option_set_level, option_set_on_id, option_value"
        ),
        "filters": {"project": "project_id"},
        "order": "option_name ASC",
        "limit": 500,
    },
    "information_schema_project_options": {
        "category": "config",
        "select": (
            "project_id, project_number, option_name, option_description, "
            "option_type, option_value"
        ),
        "filters": {"project": "project_id"},
        "order": "option_name ASC",
        "limit": 500,
    },
    "information_schema_project_options_changes": {
        "category": "config",
        "select": (
            "update_time, username, project_id, project_number, "
            "TO_JSON_STRING(updated_options) AS updated_options"
        ),
        "filters": {"project": "project_id"},
        "order": "update_time DESC",
        "limit": 500,
    },
    "information_schema_organization_options": {
        "category": "config",
        "select": "option_name, option_description, option_type, option_value",
        "filters": {},
        "order": "option_name ASC",
        "limit": 500,
    },
    "information_schema_organization_options_changes": {
        "category": "config",
        "select": (
            "update_time, username, project_id, project_number, "
            "TO_JSON_STRING(updated_options) AS updated_options"
        ),
        "filters": {"project": "project_id"},
        "order": "update_time DESC",
        "limit": 500,
    },
    # ---- Storage: full inventory & metadata (global completeness) ----
    "reservation_usage_per_minute": {
        "category": "compute",
        "select": (
            "minute, reservation_id, project_id, slots_assigned, slots_max_assigned, "
            "total_slot_ms, query_count, "
            "ROUND(total_query_cost, 2) AS total_query_cost, "
            "ROUND(slot_utilization_ratio, 2) AS slot_utilization_ratio, "
            "ROUND(max_slot_utilization_ratio, 2) AS max_slot_utilization_ratio, "
            "utilization_category, autoscaling_status"
        ),
        "filters": {"date": "minute", "project": "project_id"},
        "order": "minute DESC",
        "limit": 5000,
    },
    "storage_with_cost": {
        "category": "storage",
        "select": (
            "project_id, dataset_id, table_id, table_type, total_rows, "
            "ROUND(total_logical_tb, 4) AS total_logical_tb, "
            "ROUND(total_physical_tb, 4) AS total_physical_tb, "
            "ROUND(cost_monthly_forecast, 2) AS cost_monthly_forecast, "
            "storage_billing_model, optimal_storage_billing_model, "
            "ROUND(potential_savings, 2) AS potential_savings"
        ),
        "filters": {"project": "project_id"},
        "order": "cost_monthly_forecast DESC",
        "limit": 2000,
    },
    "table_and_storage_with_cost": {
        "category": "storage",
        "select": (
            "project_id, dataset_id, table_id, table_type, is_insertable_into, "
            "total_rows, ROUND(total_logical_tb, 4) AS total_logical_tb, "
            "ROUND(cost_monthly_forecast, 2) AS cost_monthly_forecast, "
            "storage_billing_model, SUBSTR(ddl, 0, 2000) AS ddl"
        ),
        "filters": {"project": "project_id"},
        "order": "cost_monthly_forecast DESC",
        "limit": 2000,
    },
    # ---- Raw & Advanced: backing / staging models (every object exposed) ----
    "compute_billing_per_hour": {
        "category": "raw",
        "select": (
            "hour, compute_type, ROUND(compute_cost, 4) AS compute_cost, currency_symbol"
        ),
        "filters": {"date": "hour"},
        "order": "hour DESC",
        "limit": 5000,
    },
    "compute_cost_per_hour": {
        "category": "raw",
        "select": (
            "hour, project_id, reservation_id, "
            "ROUND(total_query_cost, 2) AS total_query_cost, "
            "ROUND(failing_query_cost, 2) AS failing_query_cost, total_bytes_billed, "
            "total_slot_ms, query_count, unique_users, cache_hits, "
            "ROUND(avg_duration_seconds, 2) AS avg_duration_seconds"
        ),
        "filters": {"date": "hour", "project": "project_id"},
        "order": "hour DESC",
        "limit": 5000,
    },
    "compute_cost_per_minute": {
        "category": "raw",
        "select": (
            "minute, project_id, reservation_id, "
            "ROUND(total_query_cost, 2) AS total_query_cost, "
            "ROUND(failing_query_cost, 2) AS failing_query_cost, total_bytes_billed, "
            "total_slot_ms, query_count, unique_users, cache_hits, "
            "ROUND(avg_duration_seconds, 2) AS avg_duration_seconds"
        ),
        "filters": {"date": "minute", "project": "project_id"},
        "order": "minute DESC",
        "limit": 5000,
    },
    "compute_rollup_per_hour": {
        "category": "raw",
        "select": (
            "hour, project_id, reservation_id, bi_engine_mode, client_type, edition, "
            "ROUND(total_query_cost, 2) AS total_query_cost, "
            "ROUND(failing_query_cost, 2) AS failing_query_cost, total_bytes_billed, "
            "total_slot_ms, query_count, unique_users, cache_hits, "
            "ROUND(avg_duration_seconds, 2) AS avg_duration_seconds, "
            "ROUND(median_duration_seconds, 2) AS median_duration_seconds"
        ),
        "filters": {"date": "hour", "project": "project_id"},
        "order": "hour DESC",
        "limit": 5000,
    },
    "compute_rollup_per_minute": {
        "category": "raw",
        "select": (
            "minute, project_id, reservation_id, bi_engine_mode, client_type, edition, "
            "ROUND(total_query_cost, 2) AS total_query_cost, "
            "ROUND(failing_query_cost, 2) AS failing_query_cost, total_bytes_billed, "
            "total_slot_ms, query_count, unique_users, cache_hits, "
            "ROUND(avg_duration_seconds, 2) AS avg_duration_seconds, median_duration_seconds"
        ),
        "filters": {"date": "minute", "project": "project_id"},
        "order": "minute DESC",
        "limit": 5000,
    },
    "jobs_costs_incremental": {
        "category": "raw",
        "select": (
            f"hour, {_QUERY_SNIPPET}, cache_hit, "
            "ROUND(total_query_cost, 4) AS total_query_cost, total_slot_ms, "
            "query_count, failed_jobs, total_bytes_billed, projects_count, "
            "users_count, ROUND(avg_duration_seconds, 2) AS avg_duration_seconds"
        ),
        "filters": {"date": "hour"},
        "order": "total_query_cost DESC",
        "limit": 500,
    },
    "models_costs_incremental": {
        "category": "raw",
        "select": (
            "hour, dbt_model_name, cache_hit, "
            "ROUND(total_query_cost, 4) AS total_query_cost, total_slot_ms, "
            "query_count, failed_runs, total_bytes_billed, "
            "ROUND(avg_duration_seconds, 2) AS avg_duration_seconds, "
            "max_duration_seconds, p90_duration_seconds"
        ),
        "filters": {"date": "hour", "model": "dbt_model_name"},
        "order": "total_query_cost DESC",
        "limit": 500,
    },
    "users_costs_incremental": {
        "category": "raw",
        "select": (
            "hour, user_email, ROUND(total_query_cost, 4) AS total_query_cost, "
            "total_slot_ms, query_count, cache_hit, failed_queries, projects_used, "
            "reservations_used, ROUND(avg_duration_seconds, 2) AS avg_duration_seconds, "
            "total_bytes_processed"
        ),
        "filters": {"date": "hour", "user": "user_email"},
        "order": "total_query_cost DESC",
        "limit": 500,
    },
    "jobs_with_cost": {
        "category": "raw",
        "select": (
            f"hour, job_id, project_id, user_email, statement_type, job_type, state, "
            f"cache_hit, {_QUERY_SNIPPET}, ROUND(query_cost, 4) AS query_cost, "
            "total_slot_ms, total_time_seconds, total_bytes_billed, dbt_model_name, "
            "reservation_id, edition"
        ),
        "filters": {"date": "hour", "project": "project_id", "user": "user_email", "model": "dbt_model_name"},
        "order": "query_cost DESC",
        "limit": 200,
    },
    "jobs_by_project_with_cost": {
        "category": "raw",
        "select": (
            f"hour, job_id, project_id, user_email, statement_type, job_type, state, "
            f"cache_hit, {_QUERY_SNIPPET}, ROUND(query_cost, 4) AS query_cost, "
            "total_slot_ms, total_time_seconds, total_bytes_billed, dbt_model_name, "
            "reservation_id, edition"
        ),
        "filters": {"date": "hour", "project": "project_id", "user": "user_email", "model": "dbt_model_name"},
        "order": "query_cost DESC",
        "limit": 200,
    },
    "jobs_from_audit_logs": {
        "category": "raw",
        "select": (
            f"creation_time, job_id, project_id, user_email, job_type, statement_type, "
            f"state, cache_hit, {_QUERY_SNIPPET}, total_slot_ms, total_billed_bytes, "
            "location, dataset_id, caller_ip_address, method_name, service_name"
        ),
        "filters": {"date": "creation_time", "project": "project_id", "user": "user_email"},
        "order": "creation_time DESC",
        "limit": 200,
    },
    "stg_partitions_monitoring": {
        "category": "raw",
        "select": (
            "project_id, dataset_id, table_id, partition_type, "
            "partition_expiration_days, partition_count, sum_total_logical_bytes, "
            "max_last_updated_time"
        ),
        "filters": {"project": "project_id"},
        "order": "partition_count DESC",
        "limit": 2000,
    },
    "table_reference_incremental": {
        "category": "raw",
        "select": "day, project_id, dataset_id, table_id, reference_count",
        "filters": {"date": "day", "project": "project_id"},
        "order": "day DESC",
        "limit": 5000,
    },
}

# UI grouping order (used by the frontend / for documentation).
CATEGORIES = {
    "overview": [
        "daily_spend",
        "cost_trend_comparison",
        "project_cost_summary",
        "recommendations",
    ],
    "compute": [
        "most_expensive_jobs",
        "slowest_jobs",
        "most_repeated_jobs",
        "job_failure_analysis",
        "error_rate_over_time",
        "materialization_candidates",
        "cost_per_project",
        "cost_by_label",
        "compute_cost_per_hour_view",
        "compute_cost_per_minute_view",
        "job_timeline_analysis",
        "bi_engine_materialized_view_analysis",
        "reservation_usage_per_hour",
        "reservation_usage_per_minute",
        "query_with_better_pricing_using_flat_pricing_view",
        "query_with_better_pricing_using_on_demand_view",
    ],
    "users": ["most_expensive_users"],
    "dbt": ["most_expensive_models", "most_repeated_models", "dbt_model_trends"],
    "storage": [
        "most_expensive_tables",
        "unused_tables",
        "write_only_tables",
        "read_heavy_tables",
        "dataset_with_cost",
        "partitions_monitoring",
        "write_ingestion_cost_per_table",
        "write_ingestion_errors_analysis",
        "storage_billing_per_hour",
        "storage_with_cost",
        "table_and_storage_with_cost",
    ],
    "savings": ["recommendations", "table_with_potential_savings", "dataset_with_potential_savings"],
    "config": [
        "dbt_bigquery_monitoring_options",
        "dataset_options",
        "information_schema_effective_project_options",
        "information_schema_project_options",
        "information_schema_project_options_changes",
        "information_schema_organization_options",
        "information_schema_organization_options_changes",
    ],
    "raw": [
        "compute_billing_per_hour",
        "compute_cost_per_hour",
        "compute_cost_per_minute",
        "compute_rollup_per_hour",
        "compute_rollup_per_minute",
        "jobs_costs_incremental",
        "models_costs_incremental",
        "users_costs_incremental",
        "jobs_with_cost",
        "jobs_by_project_with_cost",
        "jobs_from_audit_logs",
        "stg_partitions_monitoring",
        "table_reference_incremental",
    ],
}


def is_valid_datamart(name):
    return name in DATAMARTS


def build_query(name, filters):
    """Build the SQL string and bound params for a datamart + filters.

    Returns ``(sql, query_params)``. Raises ``KeyError`` for unknown datamart.
    """
    spec = DATAMARTS[name]
    dataset_ref = client.get_dataset_ref()
    where_sql, params = client.build_filters(filters or {}, spec["filters"])
    sql = (
        f"SELECT {spec['select']} "
        f"FROM `{dataset_ref}.{name}`"
        f"{where_sql} "
        f"ORDER BY {spec['order']} "
        f"LIMIT {int(spec['limit'])}"
    )
    return sql, params


def fetch_datamart(name, filters=None):
    """Fetch rows for a registered datamart, applying supported filters."""
    if not is_valid_datamart(name):
        raise KeyError(f"Unknown datamart: {name}")
    sql, params = build_query(name, filters)
    return client.run_query(sql, params)


def get_summary(filters=None):
    """Return headline big-number metrics for the Overview sub-tab.

    Cost KPIs are sourced from ``daily_spend`` (the GCP billing export), which
    is the source of truth for actual € spent — NOT from
    ``project_cost_summary.total_estimated_cost``, which is a short-window
    on-demand estimate and badly understates real cost.

    Inventory + forecast + savings come from ``project_cost_summary`` and
    ``recommendations``. An optional ``project_id`` filter narrows the
    inventory/recommendation scope (``daily_spend`` is project-agnostic).
    """
    filters = filters or {}
    dataset_ref = client.get_dataset_ref()

    # Actual spend (billing export) for the CURRENT calendar month to date,
    # split by category. This answers "how much will we pay this month".
    # We also forecast the full month by extrapolating MTD over elapsed days.
    spend_sql = (
        "SELECT "
        "ROUND(SUM(cost), 2) AS spend_mtd, "
        "ROUND(SUM(IF(cost_category = 'compute', cost, 0)), 2) AS compute_mtd, "
        "ROUND(SUM(IF(cost_category = 'storage', cost, 0)), 2) AS storage_mtd, "
        "ROUND("
        "  SUM(cost) "
        "  / GREATEST(DATE_DIFF(CURRENT_DATE(), DATE_TRUNC(CURRENT_DATE(), MONTH), DAY) + 1, 1) "
        "  * EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE()))"
        ", 2) AS spend_month_forecast "
        f"FROM `{dataset_ref}.daily_spend` "
        "WHERE day >= TIMESTAMP(DATE_TRUNC(CURRENT_DATE(), MONTH))"
    )

    # Inventory + storage forecast/savings.
    proj_where, proj_params = client.build_filters(filters, {"project": "project_id"})
    proj_sql = (
        "SELECT "
        "ROUND(SUM(storage_monthly_forecast), 2) AS storage_monthly_forecast, "
        "ROUND(SUM(storage_potential_savings), 2) AS storage_potential_savings, "
        "SUM(table_count) AS tables_monitored, "
        "COUNT(DISTINCT project_id) AS projects_monitored, "
        "SUM(unique_users) AS unique_users "
        f"FROM `{dataset_ref}.project_cost_summary`{proj_where}"
    )

    rec_where, rec_params = client.build_filters(filters, {"project": "project_id"})
    rec_sql = (
        "SELECT COUNT(*) AS active_recommendations, "
        "ROUND(SUM(estimated_monthly_savings), 2) AS rec_savings "
        f"FROM `{dataset_ref}.recommendations`{rec_where}"
    )

    spend = client.run_query(spend_sql, [])
    proj = client.run_query(proj_sql, proj_params)
    recs = client.run_query(rec_sql, rec_params)

    result = {}
    result.update(spend[0] if spend else {})
    result.update(proj[0] if proj else {})
    result.update(recs[0] if recs else {})

    # Combined potential savings = storage billing-model savings + Google recs.
    storage_savings = result.get("storage_potential_savings") or 0
    rec_savings = result.get("rec_savings") or 0
    result["potential_savings"] = round(storage_savings + rec_savings, 2)
    return result


def get_filter_options():
    """Return distinct values for the global filter dropdowns.

    Pulls projects, users and dbt models from the most relevant datamarts so
    the UI can populate filter selectors without scanning raw jobs.
    """
    dataset_ref = client.get_dataset_ref()
    projects = client.run_query(
        "SELECT DISTINCT project_id FROM "
        f"`{dataset_ref}.project_cost_summary` "
        "WHERE project_id IS NOT NULL ORDER BY project_id"
    )
    users = client.run_query(
        "SELECT DISTINCT user_email FROM "
        f"`{dataset_ref}.most_expensive_users` "
        "WHERE user_email IS NOT NULL ORDER BY user_email LIMIT 500"
    )
    models = client.run_query(
        "SELECT DISTINCT dbt_model_name FROM "
        f"`{dataset_ref}.most_expensive_models` "
        "WHERE dbt_model_name IS NOT NULL ORDER BY dbt_model_name LIMIT 1000"
    )
    categories = client.run_query(
        "SELECT DISTINCT cost_category FROM "
        f"`{dataset_ref}.daily_spend` "
        "WHERE cost_category IS NOT NULL ORDER BY cost_category"
    )
    return {
        "projects": [r["project_id"] for r in projects],
        "users": [r["user_email"] for r in users],
        "dbt_models": [r["dbt_model_name"] for r in models],
        "cost_categories": [r["cost_category"] for r in categories],
    }
