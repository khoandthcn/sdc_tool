import unittest
from unittest.mock import patch, MagicMock
import subprocess
import os
import gzip
import json
import time
import shutil
from datetime import datetime

class IntegrationTestCortexXDRSource(unittest.TestCase):
    def setUp(self):
        self.config_file = "/tmp/integration_test_cortex_xdr_config.ini"
        self.output_dir = "/tmp/sdc_integration_output"
        self.log_file = "/tmp/sdc_integration.log"
        self.state_file = "/tmp/sdc_integration_state.json"

        # Create a dummy config file for the integration test
        config_content = f"""
[Pipeline]
pipeline = cortex_xdr > local_file

[General]
output_format = json_gz
state_file_path = {self.state_file}
log_file_path = {self.log_file}
log_level = INFO

[CortexXDR]
cortex_xdr.initial_collection_timestamp = 2024-01-01 00:00:00
cortex_xdr.api.fqdn = mock-api.xdr.com
cortex_xdr.api.key_id = MOCK_API_KEY_ID
cortex_xdr.api.key = MOCK_API_KEY
cortex_xdr.api.xql_query_template_alerts = dataset = xdr_data | filter _time > \'{{start_time}}\' and _time <= \'{{end_time}}\' | fields * | limit 5

[LocalFile]
local_file.base_path = {self.output_dir}
local_file.max_records_per_file = 10000
local_file.max_file_size_mb = 100
"""
        with open(self.config_file, "w") as f:
            f.write(config_content)

        # Ensure output directory exists and is empty
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        os.makedirs(self.output_dir, exist_ok=True)

        # Clean up log and state files
        if os.path.exists(self.log_file):
            os.remove(self.log_file)
        if os.path.exists(self.state_file):
            os.remove(self.state_file)

    def tearDown(self):
        if os.path.exists(self.config_file):
            os.remove(self.config_file)
        if os.path.exists(self.log_file):
            os.remove(self.log_file)
        if os.path.exists(self.state_file):
            os.remove(self.state_file)
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

    @patch("cortex_xdr_client.client.CortexXDRClient")
    @patch("time.sleep")
    def test_cortex_xdr_to_local_file_pipeline(self, mock_sleep, MockCortexXDRClient):
        # Mock the instance of CortexXDRClient that will be created by CortexXDRSource
        mock_client_instance = MockCortexXDRClient.return_value
        mock_client_instance.xql_api = MagicMock()

        # Mock start_xql_query
        mock_client_instance.xql_api.start_xql_query.return_value = "mock_query_id_123"

        # Mock get_query_results for polling status
        # First call: PENDING, Second call: SUCCESS
        mock_client_instance.xql_api.get_query_results.side_effect = [
            {"reply": {"status": "PENDING"}},
            {"reply": {"status": "SUCCESS", "number_of_results": 2}}
        ]

        # Simulate gzipped data for write_query_results
        simulated_data = [
            {"event_id": 1, "data": "mock event 1"},
            {"event_id": 2, "data": "mock event 2"}
        ]
        json_data = "\n".join([json.dumps(record) for record in simulated_data]).encode("utf-8")
        gzipped_json_data = gzip.compress(json_data)

        # Mock write_query_results to simulate writing to the temp file
        def mock_write_query_results(query_id, output_gz_file, limit=None, params={}):
            with open(output_gz_file, "wb") as f:
                f.write(gzipped_json_data)
            return len(gzipped_json_data)

        mock_client_instance.xql_api.write_query_results.side_effect = mock_write_query_results

        # Run the sdc command using python -m
        command = ["python3.11", "-m", "sdc_tool.main", "--config", self.config_file]
        # Pass environment variables to the subprocess to ensure mocking works
        env = os.environ.copy()
        env["CORTEX_XDR_MOCK_API"] = "true" # A flag to indicate test environment
        process = subprocess.run(command, capture_output=True, text=True, cwd="/home/ubuntu/sdc_tool", env=env)

        # Assertions
        self.assertEqual(process.returncode, 0, f"SDC command failed with error: {process.stderr}")
        
        # Verify log file content instead of stdout
        with open(self.log_file, "r") as f:
            log_content = f.read()
        self.assertIn("Starting Security Data Collector", log_content)
        self.assertIn("Successfully wrote", log_content)
        self.assertIn("XQL query mock_query_id_123 status: PENDING", log_content)
        self.assertIn("XQL query mock_query_id_123 status: SUCCESS, results: 2", log_content)
        self.assertIn("Successfully wrote 65 bytes to /tmp/cortex_xdr_output_", log_content) # Changed assertion

        # Verify the output file was created and contains the correct gzipped data
        output_files_path = os.path.join(self.output_dir, "cortex_xdr", "default", datetime.now().strftime("%Y%m%d"))
        output_files = os.listdir(output_files_path)
        self.assertEqual(len(output_files), 1)
        output_file_path = os.path.join(output_files_path, output_files[0])
        
        with gzip.open(output_file_path, "rb") as f:
            decompressed_read_data = f.read()
        
        self.assertEqual(decompressed_read_data, json_data)

        # Verify state file content
        with open(self.state_file, "r") as f:
            state_content = json.load(f)
        self.assertIn("cortex_xdr_local_file", state_content)
        self.assertIsNotNone(state_content["cortex_xdr_local_file"])

if __name__ == '__main__':
    unittest.main()


