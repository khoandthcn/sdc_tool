import unittest
import os
import shutil
import json
import gzip
from unittest.mock import patch, MagicMock
from datetime import datetime
import subprocess

from sdc_tool.hdfs_sink import HDFSSink
from sdc_tool.config_parser import ConfigParser

class TestHDFSSink(unittest.TestCase):
    def setUp(self):
        self.mock_config_file = "mock_hdfs_config.ini"
        self.create_mock_config()
        self.config_parser = ConfigParser(self.mock_config_file)

    def tearDown(self):
        if os.path.exists(self.mock_config_file):
            os.remove(self.mock_config_file)

    def create_mock_config(self):
        config_content = """
[Pipeline]
pipeline = qradar > hdfs

[Hadoop]
hadoop.namenode_url = http://mock-hadoop-namenode:50070
hadoop.kerberos_enabled = True
hadoop.kerberos_principal = test_sdc_collector@YOUR.REALM
hadoop.keytab_path = /etc/test_sdc_collector.keytab
hadoop.hdfs_qradar_api_events_base_path = /data/security/qradar/events
hadoop.hdfs_qradar_api_offenses_base_path = /data/security/qradar/offenses
hadoop.hdfs_qradar_syslog_base_path = /data/security/qradar/syslog_events
hadoop.hdfs_cortex_xdr_api_alerts_base_path = /data/security/cortex_xdr/alerts
hadoop.max_records_per_file = 100000
hadoop.max_file_size_mb = 256
"""
        with open(self.mock_config_file, "w") as f:
            f.write(config_content)

    @patch("subprocess.run")
    @patch("sdc_tool.hdfs_sink.InsecureClient") # Patch the InsecureClient within the hdfs_sink module
    def test_hdfs_sink_initialization_with_kerberos(self, MockInsecureClient, MockSubprocessRun):
        MockSubprocessRun.return_value = MagicMock(stdout="Kerberos ticket obtained", stderr="")
        
        sink = HDFSSink(self.config_parser)
        self.assertTrue(sink.kerberos_enabled)
        MockSubprocessRun.assert_called_once_with(
            ["kinit", "-kt", "/etc/test_sdc_collector.keytab", "test_sdc_collector@YOUR.REALM"],
            capture_output=True, check=True, text=True
        )
        # Ensure InsecureClient is not called when Kerberos is enabled (as it's handled by kinit)
        MockInsecureClient.assert_not_called()

    @patch("subprocess.run")
    @patch("sdc_tool.hdfs_sink.InsecureClient")
    def test_hdfs_sink_initialization_without_kerberos(self, MockInsecureClient, MockSubprocessRun):
        self.config_parser.config["Hadoop"]["hadoop.kerberos_enabled"] = "False"
        sink = HDFSSink(self.config_parser)
        self.assertFalse(sink.kerberos_enabled)
        MockSubprocessRun.assert_not_called()
        MockInsecureClient.assert_not_called() # In this test, it should not be called

    @patch("subprocess.run")
    @patch("sdc_tool.hdfs_sink.InsecureClient")
    def test_write_data_to_hdfs(self, MockInsecureClient, MockSubprocessRun):
        # Mock Kerberos authentication
        MockSubprocessRun.return_value = MagicMock(stdout="Kerberos ticket obtained", stderr="")

        # Mock the HDFS client and its write method
        mock_hdfs_client = MagicMock()
        MockInsecureClient.return_value = mock_hdfs_client
        mock_hdfs_client.write.return_value.__enter__.return_value = MagicMock()

        sink = HDFSSink(self.config_parser)
        test_data = [
            {"id": 1, "value": "hdfs_test1"},
            {"id": 2, "value": "hdfs_test2"}
        ]
        source_id = "qradar"
        input_type = "api_events"

        sink.write_data(test_data, source_id, input_type)

        # Assert that InsecureClient was called with the namenode URL
        MockInsecureClient.assert_called_once_with(sink.namenode_url)

        # Assert that the write method was called on the HDFS client
        # We need to be careful with the exact path as it includes datetime
        mock_hdfs_client.write.assert_called_once()
        args, kwargs = mock_hdfs_client.write.call_args
        self.assertTrue(args[0].startswith("/data/security/qradar/events/"))
        self.assertTrue(args[0].endswith(".json.gz"))
        self.assertEqual(kwargs["encoding"], "utf-8")
        self.assertTrue(kwargs["overwrite"])

        # Assert that data was written (this is a conceptual check as we mocked the write)
        # In a real test, you might inspect the mock_hdfs_client.write.return_value
        # to ensure the correct data was passed to the underlying file-like object.

    @patch("subprocess.run")
    @patch("sdc_tool.hdfs_sink.InsecureClient")
    def test_write_empty_data(self, MockInsecureClient, MockSubprocessRun):
        # Mock Kerberos authentication during initialization
        MockSubprocessRun.return_value = MagicMock(stdout="Kerberos ticket obtained", stderr="")

        sink = HDFSSink(self.config_parser)
        sink.write_data([], "qradar", "api_events")
        # InsecureClient should not be called if no data is written
        MockInsecureClient.assert_not_called()
        # subprocess.run is called during initialization, so we can't assert_not_called here.
        # We can assert that it was called once for initialization.
        MockSubprocessRun.assert_called_once()

    @patch("subprocess.run")
    @patch("sdc_tool.hdfs_sink.InsecureClient")
    def test_kerberos_authentication_failure(self, MockInsecureClient, MockSubprocessRun):
        MockSubprocessRun.side_effect = subprocess.CalledProcessError(1, "kinit", stderr="Authentication failed")
        with self.assertRaises(subprocess.CalledProcessError):
            HDFSSink(self.config_parser)

if __name__ == '__main__':
    unittest.main()


