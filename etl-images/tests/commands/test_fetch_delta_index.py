import json
import pytest
from unittest.mock import Mock, patch, mock_open
from commands.fetch_delta_index import handle


INDEX_TXT = "20240101120000.jsonl.gz\n20240102120000.jsonl.gz\n20240103120000.jsonl.gz\n"


class TestFetchDeltaIndex:
    """Tests for fetch_delta_index command"""

    @pytest.fixture
    def mock_session(self):
        with patch("commands.fetch_delta_index.requests.Session") as mock_cls:
            mock_sess = Mock()
            mock_cls.return_value = mock_sess
            mock_response = Mock()
            mock_response.text = INDEX_TXT
            mock_sess.get.return_value = mock_response
            yield mock_sess

    def test_filenames_are_sorted(self, mock_session, tmp_path):
        """Filenames returned in index.txt are sorted alphabetically."""
        unsorted_index = "20240103120000.jsonl.gz\n20240101120000.jsonl.gz\n20240102120000.jsonl.gz\n"
        mock_session.get.return_value.text = unsorted_index

        with patch("commands.fetch_delta_index.os.makedirs"), \
             patch("builtins.open", mock_open()) as mocked_file:
            handle("https://example.com/delta/index.txt")

            content = "".join(call[0][0] for call in mocked_file().write.call_args_list)
            assert json.loads(content) == [
                "20240101120000.jsonl.gz",
                "20240102120000.jsonl.gz",
                "20240103120000.jsonl.gz",
            ]

    def test_empty_lines_are_ignored(self, mock_session):
        """Blank lines in index.txt are not included in the output."""
        mock_session.get.return_value.text = "\n20240101120000.jsonl.gz\n\n20240102120000.jsonl.gz\n\n"

        with patch("commands.fetch_delta_index.os.makedirs"), \
             patch("builtins.open", mock_open()) as mocked_file:
            handle("https://example.com/delta/index.txt")

            content = "".join(call[0][0] for call in mocked_file().write.call_args_list)
            assert len(json.loads(content)) == 2

    def test_xcom_written_in_airflow_context(self, mock_session):
        """XCom file is written when /airflow/xcom directory is accessible."""
        with patch("commands.fetch_delta_index.os.makedirs"), \
             patch("builtins.open", mock_open()) as mocked_file:
            handle("https://example.com/delta/index.txt")

            mocked_file.assert_called_with("/airflow/xcom/return.json", "w")

    def test_no_xcom_outside_airflow(self, mock_session):
        """No exception is raised when /airflow/xcom is not accessible (local run)."""
        with patch("commands.fetch_delta_index.os.makedirs", side_effect=OSError):
            handle("https://example.com/delta/index.txt")  # should not raise

    def test_http_error_is_raised(self, mock_session):
        """An HTTP error from the index URL propagates as an exception."""
        mock_session.get.return_value.raise_for_status.side_effect = Exception("404")

        with pytest.raises(Exception, match="404"):
            handle("https://example.com/delta/index.txt")
