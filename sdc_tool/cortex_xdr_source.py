import logging
from datetime import datetime, timedelta
import gzip
import json
import time

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

    def get_xql_query_results_raw_stream(self, query_id: str):
        logger.info(f"Real Cortex XDR API call: get_xql_query_results_raw_stream for query_id: {query_id}")
        # The get_query_results_stream method in the actual library already handles gzipped responses
        # and returns a dictionary with 'data' containing a list of dictionaries.
        # We need to re-gzip this data to pass it as a gzipped stream to the sinks.
        try:
            results = self.xql_api.get_query_results_stream(query_id)
            if results and "data" in results:
                # Convert list of dicts back to newline-delimited JSON and then gzip
                json_data = "\n".join([json.dumps(record) for record in results["data"]]).encode("utf-8")
                gzipped_data = gzip.compress(json_data)
                return gzipped_data
            else:
                logger.warning(f"No data found for query_id: {query_id}")
                return gzip.compress(b'') # Return empty gzipped bytes
        except Exception as e:
            logger.error(f"Error fetching XQL stream for query_id {query_id}: {e}")
            raise


class CortexXDRSource(BaseSource):
    def __init__(self, config):
        super().__init__(config)
        self.fqdn = self.config.get("CortexXDR.cortex_xdr.api.fqdn")
        self.key_id = self.config.get("CortexXDR.cortex_xdr.api.key_id")
        self.key = self.config.get("CortexXDR.cortex_xdr.api.key")
        self.api_client = CortexXDRClient(self.fqdn, self.key_id, self.key)

    def collect_data(self, start_time: datetime, end_time: datetime):
        logger.info(f"Collecting data from Cortex XDR from {start_time} to {end_time}")
        query_template = self.config.get("CortexXDR.cortex_xdr.api.xql_query_template_alerts")
        query = query_template.format(start_time=start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                      end_time=end_time.strftime("%Y-%m-%dT%H:%M:%SZ"))
        logger.debug(f"Executing XQL query: {query}")

        try:
            # Start the XQL query
            query_id = self.api_client.xql_api.start_xql_query(query=query)
            logger.info(f"Started XQL query with ID: {query_id}")

            # Poll for query status using get_query_results with limit=0 to avoid fetching data repeatedly
            status = "PENDING"
            while status == "PENDING" or status == "RUNNING":
                time.sleep(5) # Wait for 5 seconds before polling again
                query_results_response = self.api_client.xql_api.get_query_results(query_id, limit=0)
                status = query_results_response.get("reply", {}).get("status") # Access status from 'reply' key
                logger.info(f"XQL query {query_id} status: {status}")
                if status == "SUCCESS":
                    break
                elif status == "FAILED":
                    raise UnsuccessfulQueryStatusException(f"XQL query {query_id} failed.")

            # Get the raw gzipped stream
            return self.api_client.get_xql_query_results_raw_stream(query_id)
        except Exception as e:
            logger.error(f"Error during Cortex XDR data collection: {e}")
            raise


