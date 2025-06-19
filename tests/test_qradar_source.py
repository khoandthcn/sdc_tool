import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
import os

from sdc_tool.qradar_source import QRadarSource
from sdc_tool.config_parser import ConfigParser

class TestQRadarSource(unittest.TestCase):
    def setUp(self):
        self.mock_config_file = "mock_qradar_config.ini"
        self.create_mock_config()
        self.config_parser = ConfigParser(self.mock_config_file)

    def tearDown(self):
        if os.path.exists(self.mock_config_file):
            os.remove(self.mock_config_file)

    def create_mock_config(self):
        config_content = """
[Pipeline]
pipeline = qradar > hdfs

[QRadar]
qradar.initial_collection_timestamp = 2024-01-01 00:00:00
qradar.input_type = api_events
qradar.api.host = mock_qradar_host
qradar.api.token = mock_qradar_token
qradar.api.aql_query_template_events = SELECT * FROM events WHERE starttime > \'{start_time}\' AND starttime <= \'{end_time}\' ORDER BY starttime ASC LIMIT 10000
qradar.api.aql_query_template_offenses = SELECT * FROM offenses WHERE starttime > \'{start_time}\' AND starttime <= \'{end_time}\' ORDER BY starttime ASC LIMIT 10000
qradar.syslog.protocol = UDP
qradar.syslog.port = 514
qradar.syslog.bind_address = 0.0.0.0
qradar.syslog.parser_type = leef
"""
        with open(self.mock_config_file, "w") as f:
            f.write(config_content)

    @patch("sdc_tool.qradar_source.QRadarAPIClient") # Assuming a QRadarAPIClient would be used
    def test_api_events_collection(self, MockQRadarAPIClient):
        # Mock the QRadar API client and its methods
        mock_client_instance = MockQRadarAPIClient.return_value
        mock_client_instance.get_events.return_value = [
            {"event_id": 1, "message": "test event 1"},
            {"event_id": 2, "message": "test event 2"}
        ]

        self.config_parser.config["QRadar"]["qradar.input_type"] = "api_events"
        source = QRadarSource(self.config_parser)

        start_time = datetime(2024, 1, 1, 0, 0, 0)
        end_time = datetime(2024, 1, 1, 1, 0, 0)
        data = source.collect_data(start_time, end_time)

        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["event_id"], 1)
        # In a real test, you'd assert that the mock_client_instance.get_events was called with correct arguments

    @patch("sdc_tool.qradar_source.QRadarAPIClient")
    def test_api_offenses_collection(self, MockQRadarAPIClient):
        mock_client_instance = MockQRadarAPIClient.return_value
        mock_client_instance.get_offenses.return_value = [
            {"offense_id": 101, "description": "test offense 1"}
        ]

        self.config_parser.config["QRadar"]["qradar.input_type"] = "api_offenses"
        source = QRadarSource(self.config_parser)

        start_time = datetime(2024, 1, 1, 0, 0, 0)
        end_time = datetime(2024, 1, 1, 1, 0, 0)
        data = source.collect_data(start_time, end_time)

        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["offense_id"], 101)

    def test_syslog_collection_not_implemented(self):
        self.config_parser.config["QRadar"]["qradar.input_type"] = "syslog"
        source = QRadarSource(self.config_parser)

        start_time = datetime(2024, 1, 1, 0, 0, 0)
        end_time = datetime(2024, 1, 1, 1, 0, 0)
        data = source.collect_data(start_time, end_time)

        self.assertEqual(len(data), 0)

    def test_unsupported_input_type(self):
        self.config_parser.config["QRadar"]["qradar.input_type"] = "unsupported"
        with self.assertRaises(ValueError):
            QRadarSource(self.config_parser)

if __name__ == '__main__':
    unittest.main()


