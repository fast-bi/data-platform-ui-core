"""BigQuery client + secret/query helpers for the BigQuery FinOps (dbt-bigquery-monitoring) tab.

This module is intentionally separate from ``bigquery_stats.py`` (the legacy
``INFORMATION_SCHEMA`` based statistics). It targets the pre-built
``dbt-bigquery-monitoring`` datamarts and authenticates with a dedicated,
read-only analysis service account (``DATA_ANALYSIS_GCP_SA_SECRET``).

Design notes:
- Secrets are read from the same mounted path the rest of the app uses
  (``/fastbi/secrets/bigquery/``). The SA secret value is base64-encoded JSON.
- The target project / dataset / region are fully parameterized via env vars
  so no warehouse coordinate is ever hardcoded.
- All user-supplied filter *values* are bound as BigQuery query parameters.
  Column / table names are never taken from user input.
"""
import base64
import json
import logging
import os
from datetime import date, datetime, time

from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

# --- Secret handling -------------------------------------------------------

SECRETS_PATH = os.environ.get("FASTBI_BQ_SECRETS_PATH", "/fastbi/secrets/bigquery/")

# Secrets this module is allowed to read. The analysis SA is dedicated to
# read-only consumption of the monitoring datamarts (least privilege).
_ALLOWED_SECRETS = {
    "BIGQUERY_PROJECT_ID",
    "BIGQUERY_REGION",
    "DATA_ANALYSIS_GCP_SA_EMAIL",
    "DATA_ANALYSIS_GCP_SA_SECRET",
}

_SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform",
]


def _read_secret(secret_name):
    if secret_name not in _ALLOWED_SECRETS:
        raise ValueError(f"Invalid secret name requested: {secret_name}")
    secret_path = os.path.join(SECRETS_PATH, secret_name)
    with open(secret_path, "r") as handle:
        return handle.read().strip()


def _decode_base64_sa(encoded_str):
    """Decode the base64-encoded service-account JSON into a dict."""
    decoded_str = base64.b64decode(encoded_str).decode("utf-8")
    try:
        return json.loads(decoded_str)
    except json.JSONDecodeError as exc:
        logger.error("Failed to decode service account JSON: %s", exc)
        raise ValueError(f"Failed to decode service account JSON: {exc}") from exc


# --- Warehouse coordinates (parameterized) ---------------------------------


def get_warehouse_project():
    """Project that hosts the monitoring dataset.

    Defaults to the BigQuery project from the secret but can be overridden via
    ``FASTBI_BQ_AUDIT_WAREHOUSE_PROJECT``.
    """
    override = os.environ.get("FASTBI_BQ_AUDIT_WAREHOUSE_PROJECT")
    if override:
        return override
    return _read_secret("BIGQUERY_PROJECT_ID")


def get_warehouse_dataset():
    return os.environ.get(
        "FASTBI_BQ_AUDIT_WAREHOUSE_DATASET", "prod_dbt_bigquery_monitoring"
    )


def get_warehouse_region():
    override = os.environ.get("FASTBI_BQ_AUDIT_WAREHOUSE_DATASET_REGION")
    if override:
        return override
    return _read_secret("BIGQUERY_REGION")


def get_dataset_ref():
    """Return the fully-qualified ``project.dataset`` reference (no backticks)."""
    return f"{get_warehouse_project()}.{get_warehouse_dataset()}"


# --- SSL handling (mirrors bigquery_stats.py behaviour) --------------------


def _handle_ssl_issues():
    ssl_vars = {
        "SSL_CERT_FILE": os.getenv("SSL_CERT_FILE"),
        "REQUESTS_CA_BUNDLE": os.getenv("REQUESTS_CA_BUNDLE"),
    }
    if any(ssl_vars.values()):
        original = ssl_vars.copy()
        os.environ["SSL_CERT_FILE"] = ""
        os.environ["REQUESTS_CA_BUNDLE"] = ""
        return original
    return None


def _restore_ssl_settings(original_values):
    if not original_values:
        return
    for key, value in original_values.items():
        if value is not None:
            os.environ[key] = value
        else:
            os.environ.pop(key, None)


# --- Client + query execution ----------------------------------------------


def _build_client():
    encoded_sa = _read_secret("DATA_ANALYSIS_GCP_SA_SECRET")
    decoded_sa = _decode_base64_sa(encoded_sa)
    credentials = service_account.Credentials.from_service_account_info(
        decoded_sa, scopes=_SCOPES
    )
    return bigquery.Client(project=get_warehouse_project(), credentials=credentials)


def _convert_value(value):
    if isinstance(value, list):
        return [_convert_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _convert_value(val) for key, val in value.items()}
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def run_query(sql, query_params=None):
    """Execute ``sql`` with optional bound parameters; return list[dict].

    Raises on failure so the caller (API layer) can surface a clean error
    envelope. Region is set via job config location.
    """
    original_ssl = _handle_ssl_issues()
    try:
        client = _build_client()
        job_config = bigquery.QueryJobConfig(query_parameters=query_params or [])
        region = get_warehouse_region()
        location = region.replace("region-", "") if region else None
        first_line = sql.strip().splitlines()[0] if sql.strip() else ""
        logger.info("Executing audit query: %s ...", first_line)
        query_job = client.query(sql, job_config=job_config, location=location)
        rows = query_job.result()
        return [_convert_value(dict(row)) for row in rows]
    except GoogleAPIError as exc:
        logger.error("BigQuery audit query failed: %s", exc)
        raise
    finally:
        _restore_ssl_settings(original_ssl)


# --- Filter builder (injection-safe) ---------------------------------------


def _parse_timestamp(raw):
    """Parse an ISO date/datetime string into a datetime; None if unparseable."""
    if not raw:
        return None
    text = str(raw).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed
        except ValueError:
            continue
    return None


def build_filters(filters, filter_map):
    """Build a parameterized WHERE clause from request filters.

    ``filters`` is the raw request dict. ``filter_map`` maps a generic filter
    key to the actual column name for the datamart, e.g.::

        {"date": "day", "project": "project_id", "user": "user_email"}

    Supported keys: ``date`` (uses ``date_from``/``date_to`` from filters),
    ``project``, ``user``, ``model``, ``category``.

    Returns ``(where_sql, query_params)``. Only values are bound; column names
    come from the trusted ``filter_map``.
    """
    clauses = []
    params = []

    date_col = filter_map.get("date")
    if date_col:
        date_from = _parse_timestamp(filters.get("date_from"))
        date_to = _parse_timestamp(filters.get("date_to"))
        if date_from:
            clauses.append(f"`{date_col}` >= @date_from")
            params.append(
                bigquery.ScalarQueryParameter("date_from", "TIMESTAMP", date_from)
            )
        if date_to:
            # Make the upper bound inclusive of the whole day when only a date
            # (midnight) was supplied.
            if date_to.time() == time(0, 0, 0):
                date_to = datetime.combine(date_to.date(), time(23, 59, 59))
            clauses.append(f"`{date_col}` <= @date_to")
            params.append(
                bigquery.ScalarQueryParameter("date_to", "TIMESTAMP", date_to)
            )

    for filter_key, param_name in (
        ("project", "project_id"),
        ("user", "user_email"),
        ("model", "dbt_model_name"),
        ("category", "cost_category"),
    ):
        column = filter_map.get(filter_key)
        value = filters.get(param_name)
        if column and value:
            clauses.append(f"`{column}` = @{param_name}")
            params.append(
                bigquery.ScalarQueryParameter(param_name, "STRING", str(value))
            )

    where_sql = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return where_sql, params
