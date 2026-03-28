import os
import pytest
from unittest.mock import Mock, patch, call
from common.monitoring import record_run


class TestMonitoring:
    """Tests for monitoring helper"""

    @pytest.fixture
    def mock_env_vars(self):
        env_vars = {
            "DUCKDB_TOKEN": "test-token",
            "DUCKDB_DB": "test-db",
        }
        with patch.dict(os.environ, env_vars):
            yield env_vars

    def test_record_run_success(self, mock_env_vars):
        """Test that metrics are inserted into the monitoring table when in Airflow context."""
        with patch.dict(os.environ, {"AIRFLOW_CTX_DAG_RUN_ID": "run_20260327"}), \
             patch("common.monitoring.duckdb.connect") as mock_connect:
            mock_con = Mock()
            mock_connect.return_value = mock_con

            record_run("validate_data", records_in=1000, records_out=900, records_rejected=100)

            mock_connect.assert_called_once_with("md:test-db?motherduck_token=test-token")

            sql_calls = [c[0][0] for c in mock_con.sql.call_args_list]
            assert any("CREATE SCHEMA IF NOT EXISTS monitoring" in s for s in sql_calls)
            assert any("CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs" in s for s in sql_calls)

            execute_args = mock_con.execute.call_args[0]
            assert "INSERT INTO monitoring.pipeline_runs" in execute_args[0]
            params = execute_args[1]
            assert params[0] == "run_20260327"
            assert params[1] == "validate_data"
            assert params[2] == 1000
            assert params[3] == 900
            assert params[4] == 100
            assert params[5] == 10.0

            mock_con.close.assert_called_once()

    def test_no_db_write_outside_airflow(self, mock_env_vars):
        """Test that no DB write happens when AIRFLOW_CTX_DAG_RUN_ID is not set."""
        env = {k: v for k, v in os.environ.items() if k != "AIRFLOW_CTX_DAG_RUN_ID"}
        with patch.dict(os.environ, env, clear=True), \
             patch.dict(os.environ, mock_env_vars), \
             patch("common.monitoring.duckdb.connect") as mock_connect:

            record_run("validate_data", records_in=100, records_out=100)

            mock_connect.assert_not_called()

    def test_rejection_rate_calculation(self, mock_env_vars):
        """Test that rejection rate is correctly calculated."""
        with patch.dict(os.environ, {"AIRFLOW_CTX_DAG_RUN_ID": "run_20260327"}), \
             patch("common.monitoring.duckdb.connect") as mock_connect:
            mock_con = Mock()
            mock_connect.return_value = mock_con

            record_run("validate_data", records_in=200, records_out=150, records_rejected=50)

            params = mock_con.execute.call_args[0][1]
            assert params[5] == 25.0  # 50/200 * 100

    def test_zero_records_in(self, mock_env_vars):
        """Test that rejection rate is 0.0 when records_in is 0."""
        with patch.dict(os.environ, {"AIRFLOW_CTX_DAG_RUN_ID": "run_20260327"}), \
             patch("common.monitoring.duckdb.connect") as mock_connect:
            mock_con = Mock()
            mock_connect.return_value = mock_con

            record_run("validate_data", records_in=0, records_out=0, records_rejected=0)

            params = mock_con.execute.call_args[0][1]
            assert params[5] == 0.0

    def test_missing_duckdb_env_var(self):
        """KeyError is raised when DuckDB env vars are missing but Airflow context is set."""
        with patch.dict(os.environ, {"AIRFLOW_CTX_DAG_RUN_ID": "run_20260327"}, clear=True):
            with pytest.raises(KeyError):
                record_run("validate_data", records_in=100, records_out=100)
