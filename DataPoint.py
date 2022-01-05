"""
This class represents a single data point in the set being converted to JSON
"""
import json

class DataPoint:

    SEQUENCE_ID = None
    GROUP       = None
    ADDRESS_1   = None
    STREET      = None
    CITY        = None
    ZIP         = None
    COUNTRY     = None
    CLIENT_ID   = None

    def __init__(self, data):
        """
        Constructor
        :param data: list of strings with all data for this data point
        """
        self.SEQUENCE_ID, self.GROUP, self.ADDRESS_1, self.STREET, self.CITY, self.ZIP, self.COUNTRY, \
            self.CLIENT_ID = data

    def combined_street_address(self):
        """
        combines STREET, CITY, ZIP, and COUNTRY and returns a string
        :return: combined string
        """
        return f"{self.STREET}|{self.CITY}|{self.ZIP}|{self.COUNTRY}"