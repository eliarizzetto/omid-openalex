import unittest
import os
from os.path import join, exists
from omid_openalex.mapping import Mapping
import shutil
import csv

class TestMapping(unittest.TestCase):

    def setUp(self):
        self.CWD_ABS = os.path.dirname(os.path.abspath(__file__))
        self.input_dir = join(self.CWD_ABS, "mapping", "input_data")
        self.db_path = join(self.input_dir, "test_db.db")
        self.expected_output_dir = join(self.CWD_ABS, "mapping", "expected_output")
        self.actual_output_dir = join(self.CWD_ABS, "mapping", "actual_output")  # Temporary output directory for testing
        self.process = Mapping()

    def test_mapping_with_res_type_field(self):
        data = self.input_dir
        db_path = self.db_path
        actual = self.actual_output_dir
        expected = self.expected_output_dir

        self.process.map_omid_openalex_ids(data, db_path, actual, res_type_field=True)

        for root, dirs, files in os.walk(actual):
            for file_name in files:
                actual_file = join(root, file_name)
                expected_file = join(expected, file_name)

                with open(expected_file, 'r', encoding='utf-8') as expected, open(actual_file, 'r', encoding='utf-8') as actual:
                    expected_content = set(tuple(row) for row in csv.DictReader(expected))
                    actual_content = set(tuple(row) for row in csv.DictReader(actual))
                    print(expected_file, actual_file)
                    self.assertEqual(expected_content, actual_content, f"File content mismatch: {expected_file} and {actual_file}")

    def tearDown(self):
        actual_dir = self.actual_output_dir
        if exists(actual_dir):
            shutil.rmtree(actual_dir)
            print(f"Removed {actual_dir}")


if __name__ == '__main__':
    unittest.main()
