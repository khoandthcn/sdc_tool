import unittest
from unittest.mock import MagicMock, patch, mock_open
from datetime import datetime, timedelta
import os
import gzip
import json

from sdc_tool.cortex_xdr_source import CortexXDRSource
from sdc_tool.config_parser import ConfigParser

class TestCortexXDRSource(unittest.TestCase):
    def setUp(self):
        self.mock_config_file = "mock_cortex_xdr_config.ini"
        self.create_mock_config()
        self.config_parser = ConfigParser(self.mock_config_file)

    def tearDown(self):
        if os.path.exists(self.mock_config_file):
            os.remove(self.mock_config_file)

    def create_mock_config(self):
        config_content = """
[Pipeline]
pipeline = cortex_xdr > hdfs

[CortexXDR]
cortex_xdr.initial_collection_timestamp = 2024-01-01 00:00:00
cortex_xdr.api.fqdn = mock_cortex_xdr_fqdn
cortex_xdr.api.key_id = mock_cortex_xdr_key_id
cortex_xdr.api.key = mock_cortex_xdr_key
cortex_xdr.api.xql_query_template_alerts = dataset = xdr_data | filter _time > \'{start_time}\' and _time <= \'{end_time}\' | fields * | limit 10000
"""
        with open(self.mock_config_file, "w") as f:
            f.write(config_content)

    @patch("sdc_tool.cortex_xdr_source.CortexXDRClient") # Patch the custom CortexXDRClient
    @patch("time.sleep") # Mock time.sleep to avoid actual delays during testing
    @patch("builtins.open", new_callable=mock_open) # Mock open to simulate file operations
    @patch("os.remove") # Mock os.remove to avoid actual file deletion
    def test_xql_alerts_collection_gzipped_stream(self, MockOsRemove, MockOpen, MockSleep, MockCortexXDRClient):
        # Mock the CortexXDRClient instance that CortexXDRSource will create
        mock_client_instance = MockCortexXDRClient.return_value
        mock_client_instance.xql_api = MagicMock() # Mock the xql_api attribute

        # Mock start_xql_query to return a dummy query_id
        mock_client_instance.xql_api.start_xql_query.return_value = "dummy_query_id_123"

        # Mock get_query_results for status polling
        # Simulate PENDING status first, then SUCCESS
        mock_client_instance.xql_api.get_query_results.side_effect = [
            {"reply": {"status": "PENDING"}},
            {"reply": {"status": "SUCCESS", "number_of_results": 2}}
        ]

        # Simulate gzipped data that would be written by write_query_results
        simulated_data = [
            {"alert_id": 1, "message": "test alert 1"},
            {"alert_id": 2, "message": "test alert 2"}
        ]
        json_data = "\n".join([json.dumps(record) for record in simulated_data]).encode("utf-8")
        gzipped_json_data = gzip.compress(json_data)

        # Mock write_query_results to return the number of bytes written
        mock_client_instance.xql_api.write_query_results.return_value = len(gzipped_json_data)

        # Mock the file read operation to return the gzipped_json_data
        MockOpen.return_value.read.return_value = gzipped_json_data

        source = CortexXDRSource(self.config_parser)

        start_time = datetime(2024, 1, 1, 0, 0, 0)
        end_time = datetime(2024, 1, 1, 1, 0, 0)
        
        collected_data = source.collect_data(start_time, end_time)

        # Assert that start_xql_query was called
        mock_client_instance.xql_api.start_xql_query.assert_called_once()

        # Assert that get_query_results was called twice for status polling
        self.assertEqual(mock_client_instance.xql_api.get_query_results.call_count, 2)
        mock_client_instance.xql_api.get_query_results.assert_any_call("dummy_query_id_123", limit=0)

        # Assert that write_query_results was called
        expected_temp_file_path = f"/tmp/cortex_xdr_output_{os.getpid()}.json.gz"
        mock_client_instance.xql_api.write_query_results.assert_called_once_with("dummy_query_id_123", expected_temp_file_path)

        # Assert that open was called to read the temp file
        MockOpen.assert_called_with(expected_temp_file_path, "rb")

        # Assert that os.remove was called to clean up the temp file
        MockOsRemove.assert_called_with(expected_temp_file_path)

        # Assert that the returned data is gzipped bytes
        self.assertIsInstance(collected_data, bytes)
        self.assertTrue(collected_data.startswith(b'\x1f\x8b\x08')) # Gzip magic number

        # Decompress and verify content
        decompressed_data = gzip.decompress(collected_data).decode("utf-8")
        expected_data = '{"alert_id": 1, "message": "test alert 1"}\n{"alert_id": 2, "message": "test alert 2"}'
        self.assertEqual(decompressed_data.strip(), expected_data.strip())


