import os
import logging
from datetime import datetime, timezone
import duckdb

logger = logging.getLogger(__name__)

MONITORING_SCHEMA = "monitoring"
MONITORING_TABLE = "pipeline_runs"


def record_run(command, records_in, records_out, records_rejected=0):
    """Enregistre les métriques d'un run dans la table monitoring.pipeline_runs.

    Le dag_run_id est lu depuis la variable d'environnement AIRFLOW_CTX_DAG_RUN_ID,
    injectée automatiquement par Airflow dans les pods Kubernetes.
    Hors Airflow (exécution locale), la fonction log uniquement sans écrire en base
    afin d'éviter de polluer la table de monitoring de production.
    """
    dag_run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")

    if not dag_run_id:
        logger.info(f"Monitoring (local): {records_in} in, {records_out} out, {records_rejected} rejected")
        return

    motherduck_token = os.environ["DUCKDB_TOKEN"]
    motherduck_db = os.environ["DUCKDB_DB"]

    rejection_rate = round(records_rejected / records_in * 100, 2) if records_in > 0 else 0.0
    executed_at = datetime.now(timezone.utc)

    con = duckdb.connect(f"md:{motherduck_db}?motherduck_token={motherduck_token}")
    con.sql(f"CREATE SCHEMA IF NOT EXISTS {MONITORING_SCHEMA}")
    con.sql(f"""
        CREATE TABLE IF NOT EXISTS {MONITORING_SCHEMA}.{MONITORING_TABLE} (
            dag_run_id       VARCHAR,
            command          VARCHAR,
            records_in       INTEGER,
            records_out      INTEGER,
            records_rejected INTEGER,
            rejection_rate   FLOAT,
            executed_at      TIMESTAMP
        )
    """)
    con.execute(
        f"INSERT INTO {MONITORING_SCHEMA}.{MONITORING_TABLE} VALUES (?, ?, ?, ?, ?, ?, ?)",
        [dag_run_id, command, records_in, records_out, records_rejected, rejection_rate, executed_at]
    )
    con.close()

    logger.info(
        f"Monitoring: {records_in} in, {records_out} out, "
        f"{records_rejected} rejected ({rejection_rate}%)"
    )
