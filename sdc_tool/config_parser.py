import configparser
import os

class ConfigParser:
    def __init__(self, config_file_path):
        self.config = configparser.ConfigParser()
        if not os.path.exists(config_file_path):
            raise FileNotFoundError(f"Config file not found: {config_file_path}")
        self.config.read(config_file_path)

    def get(self, section_key, default=None):
        section, key = section_key.split(".", 1)
        if section in self.config and key in self.config[section]:
            return self.config[section][key]
        return default

    def getboolean(self, section_key, default=None):
        section, key = section_key.split(".", 1)
        if section in self.config and key in self.config[section]:
            return self.config[section].getboolean(key)
        return default

    def getint(self, section_key, default=None):
        section, key = section_key.split(".", 1)
        if section in self.config and key in self.config[section]:
            return self.config[section].getint(key)
        return default

    def getfloat(self, section_key, default=None):
        section, key = section_key.split(".", 1)
        if section in self.config and key in self.config[section]:
            return self.config[section].getfloat(key)
        return default

    def get_pipeline_config(self):
        if "Pipeline" not in self.config or "pipeline" not in self.config["Pipeline"]:
            raise ValueError("Pipeline section or pipeline definition not found in config.ini")
        pipeline_str = self.config["Pipeline"]["pipeline"]
        source_id, sink_id = [p.strip() for p in pipeline_str.split(">")]
        return source_id, sink_id

    def get_section(self, section_name):
        if section_name in self.config:
            return self.config[section_name]
        return {}


