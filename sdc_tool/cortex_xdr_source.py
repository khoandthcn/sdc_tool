import logging
from datetime import datetime, timedelta
import gzip
import json
import time
import os

from cortex_xdr_client.client import CortexXDRClient as RealCortexXDRClient
from cortex_xdr_client.api.authentication import Authentication
from cortex_xdr_client.api.models.exceptions import UnsuccessfulQueryStatusException

from sdc_tool.base_source import BaseSource

logger = logging.getLogger(__name__)

class CortexXDRClient(RealCortexXDRClient):
    """Wrapper for the real Cortex XDR client to handle gzipped streams directly."""
    def __init__(self, fqdn, api_key_id, api_key):
        auth = Authentication(api_key_id=api_key_id, api_key=api_key)
        super().__init__(auth=auth, fqdn=fqdn)
        self.fqdn = fqdn
        self.api_key_id = api_key_id
        self.api_key = api_key
        logger.info(f"Initialized RealCortexXDRClient for FQDN: {self.fqdn}")


class CortexXDRSource(BaseSource):
    def __init__(self, config):
        super().__init__(config)
        self.fqdn = self.config.get("CortexXDR.cortex_xdr.api.fqdn")
        self.key_id = self.config.get("CortexXDR.cortex_xdr.api.key_id")
        self.key = self.config.get("CortexXDR.cortex_xdr.api.key")
        self.api_client = CortexXDRClient(self.fqdn, self.key_id, self.key)

    def collect_data(self, start_time: datetime, end_time: datetime):
        query_template = self.config.get("CortexXDR.cortex_xdr.api.xql_query_template_alerts")
        query = query_template.format(start_time=int(start_time.timestamp()*1000),  # Convert to milliseconds
                                      end_time=int(end_time.timestamp()*1000))  # Convert to milliseconds
        logger.info(f"Collecting data from Cortex XDR from {start_time} to {end_time}: {query}")

        # Get tmp directory for gzipped output
        tmp_dir = self.config.get("CortexXDR.cortex_xdr.tmp_dir", "./tmp/xdr")
        prefix_filename = self.config.get("CortexXDR.cortex_xdr.prefix_filename", "xdr_data")
        # Real API calls
        query_id = self.api_client.xql_api.start_xql_query(query=query)
        logger.info(f"Started XQL query with ID: {query_id}")

        max_wait_time = self.config.get("CortexXDR.cortex_xdr.api.max_wait_time", 300)
        waited_time = 0
        
        temp_gz_file = f"{tmp_dir}/{prefix_filename}_{start_time}_{end_time}.json.gz"
        # Poll for query status
        while waited_time < max_wait_time:
            try:
                bytes_written = self.api_client.xql_api.write_query_results(query_id, temp_gz_file)
                logger.info(f"Successfully wrote {bytes_written} bytes to {temp_gz_file}")
                return temp_gz_file
            except Exception:
                logger.info(f"XQL query {query_id} is still running, waiting...")
                time.sleep(2)
                waited_time += 2
        logger.error(f"Query {query_id} did not complete within {max_wait_time} seconds.")
        raise UnsuccessfulQueryStatusException(f"Query {query_id} did not complete in time.")





