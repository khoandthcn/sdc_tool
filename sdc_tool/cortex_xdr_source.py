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
        logger.info(f"Collecting data from Cortex XDR from {start_time} to {end_time}")
        query_template = self.config.get("CortexXDR.cortex_xdr.api.xql_query_template_alerts")
        query = query_template.format(start_time=start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                      end_time=end_time.strftime("%Y-%m-%dT%H:%M:%SZ"))
        logger.debug(f"Executing XQL query: {query}")

        try:
            # Check if running in test environment to mock API calls
            if os.environ.get("CORTEX_XDR_MOCK_API") == "true":
                logger.info("CORTEX_XDR_MOCK_API is true. Mocking Cortex XDR API calls.")
                # Simulate start_xql_query
                query_id = "mock_query_id_123"
                logger.info(f"Started XQL query with ID: {query_id}")

                # Simulate polling for query status
                status_responses = [
                    {"reply": {"status": "PENDING"}},
                    {"reply": {"status": "SUCCESS", "number_of_results": 2}}
                ]
                for response in status_responses:
                    time.sleep(1) # Simulate delay
                    reply = response.get("reply", {})
                    status = reply.get("status")
                    number_of_results = reply.get("number_of_results")
                    
                    if status in ["PENDING", "RUNNING"]:
                        logger.info(f"XQL query {query_id} status: {status}")
                    else:
                        logger.info(f"XQL query {query_id} status: {status}, results: {number_of_results}")

                    if status == "SUCCESS":
                        break

                # Simulate gzipped data for write_query_results
                simulated_data = [
                    {"event_id": 1, "data": "mock event 1"},
                    {"event_id": 2, "data": "mock event 2"}
                ]
                json_data = "\n".join([json.dumps(record) for record in simulated_data]).encode("utf-8")
                gzipped_data = gzip.compress(json_data)

                # Simulate writing to the temporary file
                temp_gz_file = f"/tmp/cortex_xdr_output_{os.getpid()}.json.gz"
                with open(temp_gz_file, "wb") as f:
                    f.write(gzipped_data)
                bytes_written = len(gzipped_data)
                logger.info(f"Successfully wrote {bytes_written} bytes to {temp_gz_file}")

                with open(temp_gz_file, 'rb') as f:
                    gzipped_data_read = f.read()
                os.remove(temp_gz_file)
                return gzipped_data_read

            else:
                # Real API calls
                # Start the XQL query
                query_id = self.api_client.xql_api.start_xql_query(query=query)
                logger.info(f"Started XQL query with ID: {query_id}")

                # Poll for query status
                status = "PENDING"
                while status == "PENDING" or status == "RUNNING":
                    time.sleep(5) # Wait for 5 seconds before polling again
                    query_results_response = self.api_client.xql_api.get_query_results(query_id, limit=0)
                    
                    reply = query_results_response.get("reply", {})
                    status = reply.get("status")
                    number_of_results = reply.get("number_of_results")

                    if status in ["PENDING", "RUNNING"]:
                        logger.info(f"XQL query {query_id} status: {status}")
                    else:
                        logger.info(f"XQL query {query_id} status: {status}, results: {number_of_results}")

                    if status == "SUCCESS":
                        break
                    elif status == "FAILED":
                        raise UnsuccessfulQueryStatusException(f"XQL query {query_id} failed.")
                    elif status not in ["PENDING", "RUNNING"]:
                        logger.warning(f"Unexpected XQL query {query_id} status: {status}. Proceeding with collection.")
                        break

                # Create a temporary file to store the gzipped output
                temp_gz_file = f"/tmp/cortex_xdr_output_{os.getpid()}.json.gz"
                logger.info(f"Writing XQL query results to temporary gzipped file: {temp_gz_file}")
                
                # Use the new write_query_results function
                bytes_written = self.api_client.xql_api.write_query_results(query_id, temp_gz_file)
                logger.info(f"Successfully wrote {bytes_written} bytes to {temp_gz_file}")

                # Read the gzipped content back as bytes
                with open(temp_gz_file, 'rb') as f:
                    gzipped_data = f.read()
                
                # Clean up the temporary file
                os.remove(temp_gz_file)

                return gzipped_data

        except Exception as e:
            logger.error(f"Error during Cortex XDR data collection: {e}")
            raise




