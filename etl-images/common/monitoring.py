import os
import logging
from datetime import datetime, timezone
import duckdb

logger = logging.getLogger(__name__)

def record_run(command, records_in, records_out, records_rejected=0, monitoring_schema="monitoring", monitoring_table="pipeline_runs"):
    """Enregistre les métriques d'un run dans la table monitoring.pipeline_runs.

    dag_run_id et dag_name sont lus depuis les variables d'environnement
    AIRFLOW_CTX_DAG_RUN_ID et AIRFLOW_CTX_DAG_ID, injectées automatiquement
    par Airflow dans les pods Kubernetes.
    Hors Airflow (exécution locale), la fonction log uniquement sans écrire en base
    afin d'éviter de polluer la table de monitoring de production.

    Un DELETE précède l'INSERT pour garantir qu'un seul enregistrement par
    (dag_run_id, command) est conservé — le plus récent en cas de re-run.
    """
    dag_run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")

    if not dag_run_id:
        logger.info(f"Monitoring (local): {records_in} in, {records_out} out, {records_rejected} rejected")
        return

    dag_name = os.environ.get("AIRFLOW_CTX_DAG_ID", "unknown")
    motherduck_token = os.environ["DUCKDB_TOKEN"]
    motherduck_db = os.environ["DUCKDB_DB"]

    rejection_rate = round(records_rejected / records_in * 100, 2) if records_in > 0 else 0.0
    executed_at = datetime.now(timezone.utc)

    con = duckdb.connect(f"md:{motherduck_db}?motherduck_token={motherduck_token}")
    con.sql(f"CREATE SCHEMA IF NOT EXISTS {monitoring_schema}")
    con.sql(f"""
        CREATE TABLE IF NOT EXISTS {monitoring_schema}.{monitoring_table} (
            dag_run_id       VARCHAR,
            dag_name         VARCHAR,
            command          VARCHAR,
            records_in       BIGINT,
            records_out      BIGINT,
            records_rejected BIGINT,
            rejection_rate   DOUBLE,
            executed_at      TIMESTAMPTZ
        )
    """)
    con.execute(
        f"DELETE FROM {monitoring_schema}.{monitoring_table} WHERE dag_run_id = ? AND command = ?",
        [dag_run_id, command]
    )
    con.execute(
        f"INSERT INTO {monitoring_schema}.{monitoring_table} VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [dag_run_id, dag_name, command, records_in, records_out, records_rejected, rejection_rate, executed_at]
    )
    con.close()

    logger.info(
        f"Monitoring: {records_in} in, {records_out} out, "
        f"{records_rejected} rejected ({rejection_rate}%)"
    )
