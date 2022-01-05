"""
This class is the driver for the Data Splitting and Grouping operations
"""
import collections
import sys
import os
import json
import DataPoint

class DataSplittingGrouping:

    SOURCE_FILE = None

    def __init__(self, filename):
        """
        Constructor
        NOTE: source file must be in input directory
        :param filename: name of data file to work with
        """
        self.SOURCE_FILE = filename
        self.OUTPUT_FOLDER = os.path.join(os.getcwd(), "results")
        self.INPUT_FOLDER = os.path.join(os.getcwd(), "input")

    def convert_to_json(self):
        """
        converts the source file for this object into output json files
        :return:
        """
        data = self.read_input_file()
        data = self.format_input_before_processing(data)
        data = self.consolidate_similar_addresses(data)
        data = self.group_by_group_identifier(data)
        data = self.group_by_country(data)
        self.output_all_json_results(data)

    def read_input_file(self):
        """
        reads source file
        :return:
        """
        #data_file = os.path.join(os.getcwd(), "input", "data_splitting_assessment.csv")
        with open(os.path.join(self.INPUT_FOLDER, self.SOURCE_FILE)) as f:
            all_rows = f.readlines()
        return [row.strip().split(",") for row in all_rows]

    def format_input_before_processing(self, raw_data):
        """
        Formats the raw data into dictionary with sequence id as key
            removes the header row
        :param raw_data: raw data as a list
        :return: dictionary of DataPoints
        """
        data_points = [DataPoint.DataPoint(data_row) for data_row in raw_data][1:]
        sequenced_data = {}
        for data in data_points:
            sequenced_data[data.SEQUENCE_ID] = data
        return sequenced_data

    def find_non_unique_addresses(self, addresses):
        """
        Returns a list of addresses that appear more than once in the provided list
        :param addresses: list of addresses
        :return: list of non-unique addresses
        """
        c = collections.Counter(addresses)
        return [k for k, v in c.items() if v > 1]

    def consolidate_similar_addresses(self, data):
        """
        Combines DataPoints with the same address under a single id
        :return: combined dictionary
        """

        non_unique_addresses = self.find_non_unique_addresses(
            v.combined_street_address() for k, v in data.items())

        for address in non_unique_addresses:
            ids_this_address = []
            combined_data_point = []
            for seq_id, data_point in data.items():
                if isinstance(data_point, list):
                    continue
                if data_point.combined_street_address() == address:
                    ids_this_address.append(seq_id)
                    combined_data_point.append(data_point)
            data[min(ids_this_address)] = combined_data_point
        return data

    def group_by_group_identifier(self, data):
        """
        groups the data points by group identifier
        :param: dictionary by sequence_id
        :return: dictionary grouped by group identifier
        """
        all_groups = {}
        for seq_id, datapoint in data.items():

            if isinstance(datapoint, list):
                group_id = datapoint[0].GROUP
            else:
                group_id = datapoint.GROUP
            if group_id in all_groups:
                all_groups[group_id].append(datapoint)
            else:
                all_groups[group_id] = [datapoint]
        return all_groups

    def group_by_country(self, data):
        """
        groups the data points by country
        :param data: data points grouped by group id and sequence id
        :return: data grouped by group_id, country, sequence_id
        """
        groups_by_country = {}
        for group in data:
            groups_by_country[group] = {}
            for point in data[group]:
                if isinstance(point, list):
                    country = point[0].COUNTRY
                    seq_id = point[0].SEQUENCE_ID
                else:
                    country = point.COUNTRY
                    seq_id = point.SEQUENCE_ID
                if country in groups_by_country[group]:
                    groups_by_country[group][country].append({seq_id : point})
                else:
                    groups_by_country[group][country] = [{seq_id : point}]
        return groups_by_country

    def output_all_json_results(self, data):
        """
        outputs data to all expected json files
        :param data: datapoints after being processed and grouped
        :return:
        """

        for group, countries in data.items():
            for country in countries:
                self.output_group_country_json(data[group][country], group, country)

        self.output_final_json(data)

    def output_group_country_json(self, data, group, country):
        """
        outputs a single json file for the provided group - country combination
        :param data: data to include in the file
        :param group: group for the file
        :param country: country for the file
        :return:
        """
        with open(os.path.join(self.OUTPUT_FOLDER, f"{group}_{country}.json"), "w") as f:
            f.write(json.dumps({group: {country: data}}, default=vars,
                                      sort_keys="SEQUENCE_ID", indent=4))

    def output_final_json(self, data):
        """
        outputs final json with all data
        :param data: datapoints after being processed and grouped
        :return:
        """
        with open(os.path.join(self.OUTPUT_FOLDER, "final.json"), "w") as out_file:
            out_file.write(json.dumps(data, default=vars, sort_keys="SEQUENCE_ID", indent=4))


if __name__ == "__main__":
    args = sys.argv[1:]
    #args = ["data_splitting_assessment.csv"]
    d = DataSplittingGrouping(args[0])
    d.convert_to_json()
