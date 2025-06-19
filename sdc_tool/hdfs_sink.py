import logging
import os
import gzip
import json
from datetime import datetime
import subprocess
from hdfs import InsecureClient

from sdc_tool.base_sink import BaseSink

logger = logging.getLogger(__name__)

class HDFSSink(BaseSink):
    def __init__(self, config):
        super().__init__(config)
        self.namenode_url = self.config.get("Hadoop.hadoop.namenode_url")
        self.kerberos_enabled = self.config.getboolean("Hadoop.hadoop.kerberos_enabled", False)
        if self.kerberos_enabled:
            self.kerberos_principal = self.config.get("Hadoop.hadoop.kerberos_principal")
            self.keytab_path = self.config.get("Hadoop.hadoop.keytab_path")
            self._authenticate_kerberos()

        self.hdfs_qradar_api_events_base_path = self.config.get("Hadoop.hadoop.hdfs_qradar_api_events_base_path")
        self.hdfs_qradar_api_offenses_base_path = self.config.get("Hadoop.hadoop.hdfs_qradar_api_offenses_base_path")
        self.hdfs_qradar_syslog_base_path = self.config.get("Hadoop.hadoop.hdfs_qradar_syslog_base_path")
        self.hdfs_cortex_xdr_api_alerts_base_path = self.config.get("Hadoop.hadoop.hdfs_cortex_xdr_api_alerts_base_path")

        self.max_records_per_file = int(self.config.get("Hadoop.hadoop.max_records_per_file", 100000))
        self.max_file_size_mb = int(self.config.get("Hadoop.hadoop.max_file_size_mb", 256))

    def _authenticate_kerberos(self):
        if not self.kerberos_enabled:
            return
        logger.info(f"Attempting Kerberos authentication for principal {self.kerberos_principal}")
        try:
            # Using kinit for authentication
            command = ["kinit", "-kt", self.keytab_path, self.kerberos_principal]
            result = subprocess.run(command, capture_output=True, check=True, text=True)
            logger.info(f"Kerberos authentication successful: {result.stdout.strip()}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Kerberos authentication failed: {e.stderr.strip()}")
            raise
        except FileNotFoundError:
            logger.error("kinit command not found. Ensure Kerberos client is installed and in PATH.")
            raise

    def _get_hdfs_path(self, source_identifier, input_type):
        today_str = datetime.now().strftime("%Y%m%d")
        base_path = None

        if source_identifier == "qradar":
            if input_type == "api_events":
                base_path = self.hdfs_qradar_api_events_base_path
            elif input_type == "api_offenses":
                base_path = self.hdfs_qradar_api_offenses_base_path
            elif input_type == "syslog":
                base_path = self.hdfs_qradar_syslog_base_path
        elif source_identifier == "cortex_xdr":
            base_path = self.hdfs_cortex_xdr_api_alerts_base_path

        if not base_path:
            raise ValueError(f"HDFS base path not configured for source {source_identifier} and input type {input_type}")

        return os.path.join(base_path, today_str)

    def write_data(self, data, source_identifier, input_type):
        if not data:
            logger.info("No data to write to HDFS.")
            return

        hdfs_dir = self._get_hdfs_path(source_identifier, input_type)
        
        timestamp_str = datetime.now().strftime("%H%M%S")
        filename = f"data_{timestamp_str}_{os.getpid()}.json.gz"
        full_hdfs_path = os.path.join(hdfs_dir, filename)

        logger.info(f"Writing data to HDFS path: {full_hdfs_path}")
        try:
            client = InsecureClient(self.namenode_url) # Initialize client here
            # If data is already gzipped bytes, write directly
            if isinstance(data, bytes):
                with client.write(full_hdfs_path, overwrite=True) as writer:
                    writer.write(data)
            else:
                # Otherwise, assume it's a list of records and gzip it
                with client.write(full_hdfs_path, encoding="utf-8", overwrite=True) as writer:
                    with gzip.open(writer, "wt", encoding="utf-8") as gz_writer:
                        for record in data:
                            gz_writer.write(json.dumps(record) + "\n")
            logger.info(f"Successfully wrote data to HDFS path {full_hdfs_path}")
        except Exception as e:
            logger.error(f"Error writing to HDFS {full_hdfs_path}: {e}")
            raise


