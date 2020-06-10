import abc


class DataModel(abc.ABC):
    """
    Defines an interface for a DataModel.
    """

    def __init__(self, path):
        self.path = path
        self.time = None
        self.handle = None

    @abc.abstractmethod
    def __enter__(self):
        """
        Context manager entry
        """

    @abc.abstractmethod
    def __exit__(self, *args):
        """
        Context manager exit
        """

    @abc.abstractmethod
    def __getitem__(self, item):
        """
        Return a variable.
        """

    @abc.abstractmethod
    def get(self):
        """
        Get some data from the models file
        """

    @abc.abstractmethod
    def find(self):
        """
        Return what is available in the model's file, given some hint of what
        to look for
        """
