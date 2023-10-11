#!python
# Copyright (c) 2023 Elia Rizzetto <elia.rizzetto@studio.unibo.it>.
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

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
        self.input_dir_all_rows = join(self.CWD_ABS, "mapping", "input_all_rows")
        self.db_path = join(self.input_dir, "test_db.db")
        self.expected_output_dir = join(self.CWD_ABS, "mapping", "expected_output")
        self.actual_output_dir = join(self.CWD_ABS, "mapping", "actual_output")  # Temporary output directory for testing
        self.expected_multi_mapped_dir = join(self.CWD_ABS, "mapping", "expected_multi_mapped")
        self.actual_multi_mapped_dir = join(self.CWD_ABS, "mapping", "actual_multi_mapped_out")
        self.expected_dir_all_rows = join(self.CWD_ABS, "mapping", "expected_all_rows")
        self.actual_dir_all_rows = join(self.CWD_ABS, "mapping", "actual_all_rows")
        self.process = Mapping()

    def test_mapping_with_res_type_field(self):
        data = self.input_dir
        db_path = self.db_path
        actual = self.actual_output_dir
        expected = self.expected_output_dir
        actual_multi_mapped_dir = self.actual_multi_mapped_dir
        expected_multi_mapped_dir = self.expected_multi_mapped_dir

        self.process.map_omid_openalex_ids(data, db_path, actual, actual_multi_mapped_dir, type_field=True)

        for root, dirs, files in os.walk(actual):
            for file_name in files:
                actual_file = join(root, file_name)
                expected_file = join(expected, file_name)

                self.assertFilesEqual(expected_file, actual_file)

        for file in os.listdir(actual_multi_mapped_dir):
            actual_file = join(actual_multi_mapped_dir, file)
            expected_file = join(expected_multi_mapped_dir, file)

            with open(expected_file, 'r', encoding='utf-8') as expected, open(actual_file, 'r', encoding='utf-8') as actual:
                expected_content = set(i for row in csv.DictReader(expected) for v in row.values() for i in v.split())
                actual_content = set(i for row in csv.DictReader(actual) for v in row.values() for i in v.split())
                self.assertEqual(expected_content, actual_content, f"File content mismatch: {expected_file} and {actual_file}")

    # todo: write test (and test data) for mapping with process_all set to False
    def test_mapping_all_rows(self):
        data = self.input_dir_all_rows
        db_path = self.db_path
        actual = self.actual_dir_all_rows
        expected = self.expected_dir_all_rows
        actual_multi_mapped_dir = self.actual_multi_mapped_dir

        self.process.map_omid_openalex_ids(data, db_path, actual, actual_multi_mapped_dir, type_field=True, all_rows=True)

        actual_out_file = join(actual, 'test_process_all.csv')
        expected = join(expected, 'test_process_all.csv')

        self.assertFilesEqual(expected, actual_out_file)

    def assertFilesEqual(self, expected_file, actual_file):
        with open(expected_file, 'r', encoding='utf-8') as expected, open(actual_file, 'r', encoding='utf-8') as actual:
            # convert output files to sets of tuples for comparing them (order of rows is slightly messed
            #   up in the output files, due to the fact that the function reads zipped files; the order doesnt matter).
            expected_content = set(tuple(row.items()) for row in csv.DictReader(expected))
            actual_content = set(tuple(row.items()) for row in csv.DictReader(actual))
            self.assertEqual(expected_content, actual_content, f"File content mismatch: {expected_file} and {actual_file}")

    def tearDown(self):
        actual_dir = self.actual_output_dir
        if exists(actual_dir):
            shutil.rmtree(actual_dir)
            print(f"Removed {actual_dir}")
        actual_dir_multi_mapped = self.actual_multi_mapped_dir
        if exists(actual_dir_multi_mapped):
            shutil.rmtree(actual_dir_multi_mapped)
            print(f"Removed {actual_dir_multi_mapped}")
        actual_dir_all_rows = self.actual_dir_all_rows
        if exists(actual_dir_all_rows):
            shutil.rmtree(actual_dir_all_rows)
            print(f"Removed {actual_dir_all_rows}")



if __name__ == '__main__':
    unittest.main()
