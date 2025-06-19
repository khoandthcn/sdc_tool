import abc

class BaseSource(abc.ABC):
    def __init__(self, config):
        self.config = config

    @abc.abstractmethod
    def collect_data(self, start_time, end_time):
        pass


