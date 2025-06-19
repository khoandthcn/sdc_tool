import logging
import requests
import time
import gzip
import json
import os
from requests.exceptions import RequestException
from sdc_tool.base_source import BaseSource
from datetime import datetime

from sdc_tool.base_source import BaseSource

logger = logging.getLogger(__name__)

class QRadarAPIClient:
    def __init__(self, host, token):
        self.host = host.rstrip("/")
        self.token = token
        logger.info(f"Initialized QRadarAPIClient for host: {self.host}")

    def get_events(self, query, output_gz_file, db_name="flows"):
        logger.info(f"QRadar API: get_events with query: {query}")
        api_url = f"{self.host}/api/ariel/searches"
        headers = {
            "SEC": self.token,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        try:
            # 1. Tạo search
            resp = requests.post(api_url, headers=headers, json={"query_expression": query}, verify=False)
            if resp.status_code not in (200, 201):
                logger.error(f"Failed to create search: {resp.status_code}, {resp.text}")
                return None
            search_id = resp.json().get("search_id")
            if not search_id:
                logger.error(f"No search_id in response: {resp.text}")
                return None

            logger.info(f"Created Ariel search with id: {search_id}")

            # 2. Poll status
            status_url = f"{api_url}/{search_id}"
            for _ in range(60):  # 5 phút
                time.sleep(5)
                status_resp = requests.get(status_url, headers=headers, verify=False)
                if status_resp.status_code != 200:
                    logger.warning(f"Check status failed: {status_resp.status_code}, {status_resp.text}")
                    continue
                status = status_resp.json().get("status")
                logger.info(f"Search {search_id} status: {status}")
                if status == "COMPLETED":
                    break
                elif status in ("CANCELED", "ERROR"):
                    logger.error(f"Search {search_id} ended with status: {status}")
                    return None
            else:
                logger.error("Timeout waiting for Ariel search to complete")
                return None

            # 3. Lấy results
            results_url = f"{api_url}/{search_id}/results"
            final_headers = headers.copy()
            final_headers["Range"] = "items=0-99999"
            results_resp = requests.get(results_url, headers=final_headers, verify=False)
            if results_resp.status_code != 200:
                logger.error(f"Failed to get results: {results_resp.status_code}, {results_resp.text}")
                return None

            events = results_resp.json().get(db_name, [])
            if not events:
                logger.info("No events in results.")
                return None

            # 4. Ghi ra file GZIP
            with gzip.open(output_gz_file, "wt", encoding="utf-8") as f:
                json.dump(events, f)
            logger.info(f"Wrote {len(events)} events to gzip file: {output_gz_file}")
            return output_gz_file

        except Exception as e:
            logger.error(f"Error in QRadarAPIClient.get_events: {e}")
            return None

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
            raise ValueError(f"Unsupported QRadar input type: {self.input_type}")

    def collect_data(self, start_time: datetime, end_time: datetime):
        logger.info(f"Collecting data from QRadar (type: {self.input_type}) from {start_time} to {end_time}")

        # Build tmp dir and prefix như bên CortexXDR
        tmp_dir = self.config.get("QRadar.qradar.tmp_dir", "./tmp/qradar")
        prefix_filename = self.config.get("QRadar.qradar.prefix_filename", "qradar_data")

        # Đảm bảo thư mục tồn tại
        os.makedirs(tmp_dir, exist_ok=True)

        # Build file name (dạng ISO, tránh ký tự đặc biệt cho file path)
        s_str = start_time.strftime("%Y%m%dT%H%M%S")
        e_str = end_time.strftime("%Y%m%dT%H%M%S")
        temp_gz_file = f"{tmp_dir}/{prefix_filename}_{s_str}_{e_str}.json.gz"

        try:
            if self.input_type == "api_events":
                query_template = self.config.get("QRadar.qradar.api.aql_query_template_events")
                query = query_template.format(
                    start_time=start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    end_time=end_time.strftime("%Y-%m-%d %H:%M:%S"))
                db_name = self.config.get("QRadar.qradar.api.db_name", "flows")
                logger.debug(f"Executing AQL query: {query}")
                result = self.api_client.get_events(query, temp_gz_file, db_name)  # Ghi ra file GZIP luôn
                # Nếu hàm get_events trả về tên file thì đã ghi xong
                return result

            elif self.input_type == "api_offenses":
                query_template = self.config.get("QRadar.qradar.api.aql_query_template_offenses")
                query = query_template.format(
                    start_time=start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    end_time=end_time.strftime("%Y-%m-%d %H:%M:%S"))
                logger.debug(f"Executing AQL query: {query}")
                # Nếu muốn offenses cũng ghi ra GZIP file, bạn cần sửa get_offenses tương tự get_events
                result = self.api_client.get_offenses(query, temp_gz_file)
                return result

            elif self.input_type == "syslog":
                logger.warning("Syslog collection is not yet implemented.")
                return None

            else:
                logger.error(f"Unsupported QRadar input type: {self.input_type}")
                return None

        except Exception as e:
            logger.error(f"Error during QRadar data collection: {e}")
            return None




