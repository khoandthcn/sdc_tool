import logging
from datetime import datetime, timedelta

from sdc_tool.base_source import BaseSource

logger = logging.getLogger(__name__)

class QRadarAPIClient:
    """A dummy QRadar API client for testing purposes."""
    def __init__(self, host, token):
        self.host = host
        self.token = token
        logger.info(f"Initialized QRadarAPIClient for host: {self.host}")

    def get_events(self, query):
        logger.info(f"Dummy QRadar API call: get_events with query: {query}")
        # Simulate API response
        return [{"event_id": i, "message": f"Simulated QRadar event {i}"} for i in range(5)]

    def get_offenses(self, query):
        logger.info(f"Dummy QRadar API call: get_offenses with query: {query}")
        # Simulate API response
        return [{"offense_id": i, "description": f"Simulated QRadar offense {i}"} for i in range(3)]


class QRadarSource(BaseSource):
    def __init__(self, config):
        super().__init__(config)
        # Correctly access input_type from the QRadar section
        self.input_type = self.config.get_section("QRadar").get("qradar.input_type")

        if self.input_type in ["api_events", "api_offenses"]:
            self.host = self.config.get("QRadar.qradar.api.host")
            self.token = self.config.get("QRadar.qradar.api.token")
            self.api_client = QRadarAPIClient(self.host, self.token)

        elif self.input_type == "syslog":
            self.protocol = self.config.get("QRadar.qradar.syslog.protocol")
            self.port = int(self.config.get("QRadar.qradar.syslog.port"))
            self.bind_address = self.config.get("QRadar.qradar.syslog.bind_address")
            self.parser_type = self.config.get("QRadar.qradar.syslog.parser_type")
            # TODO: Implement Syslog listener
        else:
            raise ValueError(f"Unsupported QRadar input type: {self.input_type}")

    def collect_data(self, start_time: datetime, end_time: datetime):
        logger.info(f"Collecting data from QRadar (type: {self.input_type}) from {start_time} to {end_time}")

        if self.input_type == "api_events":
            query_template = self.config.get("QRadar.qradar.api.aql_query_template_events")
            query = query_template.format(start_time=start_time.strftime("%Y-%m-%d %H:%M:%S"),
                                          end_time=end_time.strftime("%Y-%m-%d %H:%M:%S"))
            logger.debug(f"Executing AQL query: {query}")
            return self.api_client.get_events(query)

        elif self.input_type == "api_offenses":
            query_template = self.config.get("QRadar.qradar.api.aql_query_template_offenses")
            query = query_template.format(start_time=start_time.strftime("%Y-%m-%d %H:%M:%S"),
                                          end_time=end_time.strftime("%Y-%m-%d %H:%M:%S"))
            logger.debug(f"Executing AQL query: {query}")
            return self.api_client.get_offenses(query)

        elif self.input_type == "syslog":
            logger.warning("Syslog collection is not yet implemented.")
            return []

        return []


