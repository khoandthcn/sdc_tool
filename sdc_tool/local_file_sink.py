import logging
import os
import gzip
import json
from datetime import datetime

from sdc_tool.base_sink import BaseSink

logger = logging.getLogger(__name__)

class LocalFileSink(BaseSink):
    def __init__(self, config):
        super().__init__(config)
        self.base_path = self.config.get("LocalFile.local_file.base_path")
        self.max_records_per_file = int(self.config.get("LocalFile.local_file.max_records_per_file", 100000))
        self.max_file_size_mb = int(self.config.get("LocalFile.local_file.max_file_size_mb", 256))

    def write_data(self, data, source_identifier, input_type):
        if not data:
            logger.info("No data to write to local file.")
            return

        # Construct path: base_path/source_identifier/input_type/yyyyMMdd/
        today_str = datetime.now().strftime("%Y%m%d")
        output_dir = os.path.join(self.base_path, source_identifier, input_type, today_str)
        os.makedirs(output_dir, exist_ok=True)

        # Determine filename (simple increment for now, could be more robust)
        file_count = 0
        while True:
            timestamp_str = datetime.now().strftime("%H%M%S")
            filename = os.path.join(output_dir, f"data_{timestamp_str}_{file_count:04d}.json.gz")
            if not os.path.exists(filename):
                break
            file_count += 1

        logger.info(f"Writing data to local file: {filename}")
        try:
            # If data is already gzipped bytes, write directly
            if isinstance(data, bytes):
                with open(filename, "wb") as f:
                    f.write(data)
            else:
                # Otherwise, assume it's a list of records and gzip it
                with gzip.open(filename, "wt", encoding="utf-8") as f:
                    for record in data:
                        f.write(json.dumps(record) + "\n")
            logger.info(f"Successfully wrote data to {filename}")
        except Exception as e:
            logger.error(f"Error writing to local file {filename}: {e}")
            raise


