
import abc

class BaseSink(abc.ABC):
    def __init__(self, config):
        self.config = config

    @abc.abstractmethod
    def write_data(self, data, source_identifier, input_type):
        pass


