
import logging
import os
import time
import json
from datetime import datetime, timedelta
from typing import List, Tuple
from datetime import datetime, timedelta

from sdc_tool.config_parser import ConfigParser
from sdc_tool.qradar_source import QRadarSource
from sdc_tool.cortex_xdr_source import CortexXDRSource
from sdc_tool.hdfs_sink import HDFSSink
from sdc_tool.local_file_sink import LocalFileSink

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class SecurityDataCollector:
    def __init__(self, config_file):
        self.config_parser = ConfigParser(config_file)
        self.config = self.config_parser # Alias for easier access

        self.source_identifier, self.sink_identifier = self.config_parser.get_pipeline_config()

        self._setup_logging()
        self._initialize_components()
        self.state_file_path = self.config.get("General.state_file_path")

    def _setup_logging(self):
        log_file_path = self.config.get("General.log_file_path")
        log_level_str = self.config.get("General.log_level", "INFO").upper()
        log_level = getattr(logging, log_level_str, logging.INFO)

        # Remove default handlers to avoid duplicate logs
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        # File handler
        if log_file_path:
            os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
            file_handler = logging.FileHandler(log_file_path)
            file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
            logging.root.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logging.root.addHandler(console_handler)

        logging.root.setLevel(log_level)
        logger.info(f"Logging configured. Level: {log_level_str}, File: {log_file_path or 'N/A'}")

    def _initialize_components(self):
        # Initialize Source
        if self.source_identifier == "qradar":
            self.source = QRadarSource(self.config)
        elif self.source_identifier == "cortex_xdr":
            self.source = CortexXDRSource(self.config)
        else:
            raise ValueError(f"Unsupported source identifier: {self.source_identifier}")

        # Initialize Sink
        if self.sink_identifier == "hdfs":
            self.sink = HDFSSink(self.config)
        elif self.sink_identifier == "local_file":
            self.sink = LocalFileSink(self.config)
        else:
            raise ValueError(f"Unsupported sink identifier: {self.sink_identifier}")

    def _load_last_collection_time(self):
        if os.path.exists(self.state_file_path):
            try:
                with open(self.state_file_path, "r") as f:
                    state = json.load(f)
                    # Use the specific pipeline's last collected timestamp
                    pipeline_key = f"{self.source_identifier}_{self.sink_identifier}"
                    if pipeline_key in state:
                        return datetime.fromisoformat(state[pipeline_key])
            except (json.JSONDecodeError, FileNotFoundError) as e:
                logger.warning(f"Could not read state file {self.state_file_path}: {e}. Starting from initial timestamp.")
        
        # Fallback to initial_collection_timestamp from config
        if self.source_identifier == "qradar":
            initial_timestamp_str = self.config.get("QRadar.qradar.initial_collection_timestamp")
        elif self.source_identifier == "cortex_xdr":
            initial_timestamp_str = self.config.get("CortexXDR.cortex_xdr.initial_collection_timestamp")
        else:
            initial_timestamp_str = None # Should not happen if source is valid

        if initial_timestamp_str:
            return datetime.fromisoformat(initial_timestamp_str.replace(" ", "T"))
        
        logger.warning("No last collection time found and no initial timestamp configured. Starting from a very old default.")
        return datetime(1970, 1, 1) # Default to epoch if nothing else is found

    def _save_last_collection_time(self, timestamp: datetime):
        os.makedirs(os.path.dirname(self.state_file_path), exist_ok=True)
        state = {}
        if os.path.exists(self.state_file_path):
            try:
                with open(self.state_file_path, "r") as f:
                    state = json.load(f)
            except json.JSONDecodeError:
                logger.warning(f"State file {self.state_file_path} is corrupt. Overwriting.")
        
        pipeline_key = f"{self.source_identifier}_{self.sink_identifier}"
        state[pipeline_key] = timestamp.isoformat()

        with open(self.state_file_path, "w") as f:
            json.dump(state, f, indent=4)
        logger.info(f"Saved last collection time ({timestamp}) for pipeline {pipeline_key} to {self.state_file_path}")

    def _split_time_windows(start_time_ms: int, interval_minutes: int) -> List[Tuple[int, int]]:
        """
        Chia khoảng thời gian thành các block interval từ start_time đến mốc gần nhất trước hiện tại.
        
        :param start_time_ms: Thời điểm bắt đầu (milisecond)
        :param interval_minutes: Độ dài mỗi block (phút)
        :return: Danh sách các cặp (start, end) dạng milisecond
        """
        # Convert input time
        start_time = datetime.fromtimestamp(start_time_ms / 1000)
        now = datetime.now()
        
        # Xác định mốc gần nhất so với interval
        interval_sec = interval_minutes * 60
        now_ts = int(now.timestamp())
        last_mark_ts = (now_ts // interval_sec) * interval_sec
        last_mark = datetime.fromtimestamp(last_mark_ts)
        
        # Nếu start_time > last_mark thì trả về rỗng
        if start_time >= last_mark:
            return []
        
        result = []
        cur_start = start_time
        while cur_start < last_mark:
            cur_end = cur_start + timedelta(minutes=interval_minutes)
            if cur_end > last_mark:
                cur_end = last_mark
            result.append((
                int(cur_start.timestamp() * 1000),
                int(cur_end.timestamp() * 1000)
            ))
            cur_start = cur_end
        return result

    def run(self):
        logger.info(f"Starting Security Data Collector for pipeline: {self.source_identifier} > {self.sink_identifier}")
        
        last_collected_time = self._load_last_collection_time()
        logger.info(f"Last collected time: {last_collected_time}")

        # For simplicity, we'll collect data up to now. In a real scenario, this would be a loop
        # with a defined collection interval.
        current_time = datetime.now()
        
        # Define a collection window (e.g., 1 hour)
        collection_window_minutes = self.config.get("General.collection_window_minutes", 10) # 1 hour
        
        # Break down the collection into smaller blocks if the total time is too large
        # This logic needs to be refined based on actual requirements for chunking
        # For now, let's assume a single chunk for demonstration
        time_blocks = self._split_time_windows(int(last_collected_time.timestamp() * 1000), collection_window_minutes)
        
        # Example: Collect data in 1-hour chunks
        # The actual chunking logic should be based on 'chu kỳ đồng bộ' and 'giới hạn ngưỡng số event tối đa'
        # as described in the requirements.
        # Lấy thời gian bắt đầu (ms)

        for start_ms, end_ms in time_blocks:
            try:
                # Thu thập dữ liệu từ source
                collected_data = self.source.collect_data(start_ms, end_ms)
                if collected_data:
                    logger.info(f"Collected {len(collected_data)} records from {self.source_identifier} ({start_ms} - {end_ms}).")
                    # self.sink.write_data(
                    #     collected_data,
                    #     self.source_identifier,
                    #     getattr(self.source, "input_type", "default")
                    # )
                    self._save_last_collection_time(end_ms)
                else:
                    logger.info(f"No new data collected from {self.source_identifier} for block {start_ms} - {end_ms}.")
            except Exception as e:
                logger.error(f"Error during data collection or sinking for block {start_ms} - {end_ms}: {e}")

        
        # For this iteration, we'll just collect one chunk from last_collected_time to current_time
        # In a real application, this would be a loop processing time blocks.
        
        # This is a simplified approach. The actual implementation should consider:
        # - Configurable sync cycle
        # - Max event threshold
        # - Handling API rate limits and pagination
        
        # For now, let's collect data from the last collected time up to the current time
        # in one go, or in chunks if the time difference is large.
        
        # Let's assume a fixed interval for now, e.g., 1 hour or until current time
        # This needs to be driven by the 'chu kỳ đồng bộ' from the config.
        # The config doesn't explicitly define 'chu kỳ đồng bộ' as a general setting,
        # but mentions it in the 'Mô hình triển khai' section.
        # For now, let's just collect from last_collected_time to current_time.
        
        # Placeholder for actual collection loop based on sync cycle and event limits
        # For this example, we'll just do one collection run.
        
        # The requirement states: 


        # "người dùng cấu hình chu kỳ đồng bộ, tại thời điểm ứng dụng khởi chạy thực hiện đọc thời điểm đồng bộ thành công trước đó đã lưu file, tính thời điểm gần nhất theo chu kỳ đồng bộ, chia khung thời gian đồng truy vấn đồng bộ theo các block thời gian với khung tối đa bằng chu kỳ đồng bộ và thực hiện kéo dữ liệu lần lượt trong từng khung thời gian với cấu hình giới hạn ngưỡng số event tối đa cấu hình trước."

        # For simplicity in this initial implementation, we will collect data from the last collected time
        # up to the current time in one go. A more robust implementation would involve:
        # 1. Reading a 'sync_interval_seconds' from config.ini (not yet defined in example config)
        # 2. Iteratively collecting data in chunks up to 'sync_interval_seconds' or until current_time
        # 3. Handling 'max_event_threshold' (not yet defined in example config) for each API call

        # For now, let's assume a simple one-time collection from last_collected_time to now.
        # The actual chunking and thresholding will be more complex and depend on the API limits.

        # try:
        #     collected_data = self.source.collect_data(last_collected_time, current_time)
        #     if collected_data:
        #         logger.info(f"Collected {len(collected_data)} records from {self.source_identifier}.")
        #         self.sink.write_data(collected_data, self.source_identifier, self.source.input_type if hasattr(self.source, 'input_type') else 'default')
        #         self._save_last_collection_time(current_time)
        #     else:
        #         logger.info(f"No new data collected from {self.source_identifier}.")
        # except Exception as e:
        #     logger.error(f"Error during data collection or sinking: {e}")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Security Data Collector")
    parser.add_argument("--config", type=str, default="config.ini", help="Path to the configuration file")
    args = parser.parse_args()

    # Ensure the config file exists for the initial run
    if not os.path.exists(args.config):
        # If config.ini doesn't exist, try to use config.ini.example
        example_config_path = os.path.join(os.path.dirname(__file__), "..", "config.ini.example")
        if os.path.exists(example_config_path):
            import shutil
            shutil.copy(example_config_path, args.config)
            logger.info(f"Copied example config to {args.config}. Please review and update it.")
        else:
            logger.error(f"Config file not found at {args.config} and no example config found.")
            exit(1)

    sdc = SecurityDataCollector(args.config)
    sdc.run()

if __name__ == "__main__":
    main()


