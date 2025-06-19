import unittest
import os
import gzip
import json
import shutil
from datetime import datetime

from sdc_tool.local_file_sink import LocalFileSink
from sdc_tool.config_parser import ConfigParser

class TestLocalFileSink(unittest.TestCase):
    def setUp(self):
        self.test_output_dir = "/tmp/sdc_test_output"
        self.mock_config_file = "mock_local_file_config.ini"
        self.create_mock_config()
        self.config_parser = ConfigParser(self.mock_config_file)

    def tearDown(self):
        if os.path.exists(self.mock_config_file):
            os.remove(self.mock_config_file)
        if os.path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)

    def create_mock_config(self):
        config_content = f"""
[Pipeline]
pipeline = qradar > local_file

[LocalFile]
local_file.base_path = {self.test_output_dir}
local_file.max_records_per_file = 2
local_file.max_file_size_mb = 1
"""
        with open(self.mock_config_file, "w") as f:
            f.write(config_content)

    def test_write_data(self):
        sink = LocalFileSink(self.config_parser)
        test_data = [
            {"id": 1, "value": "test1"},
            {"id": 2, "value": "test2"},
            {"id": 3, "value": "test3"}
        ]
        source_id = "qradar"
        input_type = "api_events"

        sink.write_data(test_data, source_id, input_type)

        today_str = datetime.now().strftime("%Y%m%d")
        expected_dir = os.path.join(self.test_output_dir, source_id, input_type, today_str)
        self.assertTrue(os.path.exists(expected_dir))

        # Check if a .json.gz file was created
        written_files = [f for f in os.listdir(expected_dir) if f.endswith(".json.gz")]
        self.assertGreater(len(written_files), 0)

        # Read the content and verify
        with gzip.open(os.path.join(expected_dir, written_files[0]), "rt", encoding="utf-8") as f:
            lines = f.readlines()
            self.assertEqual(len(lines), len(test_data))
            for i, line in enumerate(lines):
                self.assertEqual(json.loads(line.strip()), test_data[i])

    def test_write_empty_data(self):
        sink = LocalFileSink(self.config_parser)
        source_id = "qradar"
        input_type = "api_events"

        # Ensure no directory is created for empty data
        expected_dir = os.path.join(self.test_output_dir, source_id, input_type, datetime.now().strftime("%Y%m%d"))
        self.assertFalse(os.path.exists(expected_dir))

        sink.write_data([], source_id, input_type)
        self.assertFalse(os.path.exists(expected_dir))

if __name__ == '__main__':
    unittest.main()


