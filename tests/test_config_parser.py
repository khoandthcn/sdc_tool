import unittest
import os
from sdc_tool.config_parser import ConfigParser

class TestConfigParser(unittest.TestCase):
    def setUp(self):
        self.test_config_file = "test_config.ini"
        self.create_test_config()

    def tearDown(self):
        if os.path.exists(self.test_config_file):
            os.remove(self.test_config_file)

    def create_test_config(self):
        config_content = """
[Pipeline]
pipeline = qradar > hdfs

[General]
output_format = json_gz
state_file_path = /tmp/test_state.json
log_file_path = /tmp/test_sdc.log
log_level = DEBUG

[QRadar]
qradar.initial_collection_timestamp = 2024-01-01 00:00:00
qradar.input_type = api_events
qradar.api.host = test_qradar_host
qradar.api.token = test_qradar_token
qradar.api.aql_query_template_events = SELECT * FROM events WHERE starttime > \'{start_time}\' AND starttime <= \'{end_time}\'

[Hadoop]
hadoop.namenode_url = http://test_hadoop:50070
hadoop.kerberos_enabled = True
hadoop.kerberos_principal = test_principal
hadoop.keytab_path = /etc/test.keytab
hadoop.hdfs_qradar_api_events_base_path = /data/test/qradar/events
"""
        with open(self.test_config_file, "w") as f:
            f.write(config_content)

    def test_get_pipeline_config(self):
        parser = ConfigParser(self.test_config_file)
        source, sink = parser.get_pipeline_config()
        self.assertEqual(source, "qradar")
        self.assertEqual(sink, "hdfs")

    def test_get_general_settings(self):
        parser = ConfigParser(self.test_config_file)
        self.assertEqual(parser.get("General.output_format"), "json_gz")
        self.assertEqual(parser.get("General.state_file_path"), "/tmp/test_state.json")
        self.assertEqual(parser.get("General.log_file_path"), "/tmp/test_sdc.log")
        self.assertEqual(parser.get("General.log_level"), "DEBUG")

    def test_get_qradar_settings(self):
        parser = ConfigParser(self.test_config_file)
        self.assertEqual(parser.get("QRadar.qradar.initial_collection_timestamp"), "2024-01-01 00:00:00")
        self.assertEqual(parser.get("QRadar.qradar.input_type"), "api_events")
        self.assertEqual(parser.get("QRadar.qradar.api.host"), "test_qradar_host")
        self.assertEqual(parser.get("QRadar.qradar.api.token"), "test_qradar_token")

    def test_get_hadoop_settings(self):
        parser = ConfigParser(self.test_config_file)
        self.assertEqual(parser.get("Hadoop.hadoop.namenode_url"), "http://test_hadoop:50070")
        self.assertTrue(parser.getboolean("Hadoop.hadoop.kerberos_enabled"))
        self.assertEqual(parser.get("Hadoop.hadoop.kerberos_principal"), "test_principal")
        self.assertEqual(parser.get("Hadoop.hadoop.keytab_path"), "/etc/test.keytab")

    def test_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            ConfigParser("non_existent_file.ini")

if __name__ == '__main__':
    unittest.main()


