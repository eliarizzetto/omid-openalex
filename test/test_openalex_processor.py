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
import shutil
import csv
from os.path import join
from oc_alignoa.mapping import OpenAlexProcessor


class TestOpenAlexProcessor(unittest.TestCase):

    def setUp(self):
        self.CWD_ABS = os.path.dirname(os.path.abspath(__file__))
        self.openalex_processor = OpenAlexProcessor()
        self.test_data_dir = join(self.CWD_ABS, 'openalex_processor', 'input_data')
        self.expected_output_dir = join(self.CWD_ABS, 'openalex_processor', 'expected_output')
        self.actual_output_dir = join(self.CWD_ABS, 'openalex_processor', 'actual_output')
        # self.db_file = join(self.CWD_ABS, self.actual_output_dir, 'test_db.db')

        self.works_inp_dir = join(self.test_data_dir, 'works')
        self.works_out_dir = join(self.actual_output_dir, 'ids_tables', 'works')

        self.sources_inp_dir = join(self.test_data_dir, 'sources')
        self.sources_out_dir = join(self.actual_output_dir, 'ids_tables', 'sources')

        self.expected_out_file_works = join(self.expected_output_dir, 'works', 'updated_date_test', 'reduced_part_test.csv')
        self.expected_out_file_sources = join(self.expected_output_dir, 'sources', 'updated_date_test', 'reduced_part_test.csv')
        self.pids_to_search = ['doi:10.1109/ieeestd.2011.5712778', 'pmid:7936277', 'issn:2308-4898']

    def test_create_openalex_ids_table(self):
        # Create an instance of OpenAlexProcessor
        processor = self.openalex_processor

        # Test the create_openalex_ids_table method
        processor.create_openalex_ids_table(self.works_inp_dir, self.works_out_dir, 'work')
        processor.create_openalex_ids_table(self.sources_inp_dir, self.sources_out_dir, 'source')

        # Assert that the output CSV file has been created and contains expected data
        works_output_file = join(self.works_out_dir, '0.csv')
        source_output_file = join(self.sources_out_dir, '0.csv')
        self.assertTrue(os.path.exists(works_output_file))
        self.assertTrue(os.path.exists(source_output_file))

        self.assertFilesEqual(self.expected_out_file_works, works_output_file)
        self.assertFilesEqual(self.expected_out_file_sources, source_output_file)
    ## Can't test this method because the database file cannot be deleted after the test in tearDown(), or
    ##   else the database will not be accessible, for some reason.
    # def test_create_id_db_table(self):
    #     # Create an instance of OpenAlexProcessor
    #     processor = self.openalex_processor
    #
    #     # Define some test data and directories
    #     works_inp_dir = self.works_out_dir
    #     sources_inp_dir = self.sources_out_dir
    #
    #     processor.create_id_db_table(works_inp_dir, self.db_file, 'doi', 'work')
    #     processor.create_id_db_table(works_inp_dir, self.db_file, 'pmid', 'work')
    #     processor.create_id_db_table(works_inp_dir, self.db_file, 'pmcid', 'work')
    #     processor.create_id_db_table(sources_inp_dir, self.db_file, 'issn', 'source')
    #     processor.create_id_db_table(sources_inp_dir, self.db_file, 'wikidata', 'source')
    #
    #     # Check if the database table has been created and data has been inserted
    #     # with sqlite3.connect(self.db_file) as conn:
    #     conn = sqlite3.connect(self.db_file)
    #     cursor = conn.cursor()
    #     cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
    #     res = cursor.fetchone()[0]
    #     self.assertEqual(res, 5, "Number of tables in the database is not 5")
    #     conn.close()

    def assertFilesEqual(self, expected_file, actual_file):
        with open(expected_file, 'r', encoding='utf-8') as expected, open(actual_file, 'r', encoding='utf-8') as actual:
            # convert output files to sets of tuples for comparing them (order of rows is slightly messed
            #   up in the output files, due to the fact that the function reads zipped files; the order doesnt matter).
            expected_content = set(tuple(row.items()) for row in csv.DictReader(expected))
            actual_content = set(tuple(row.items()) for row in csv.DictReader(actual))
            self.assertEqual(expected_content, actual_content,f"File content mismatch: {expected_file} and {actual_file}")

    def tearDown(self):
        actual_output_dir = self.actual_output_dir
        actual_multi_mapped_dir = join(self.actual_output_dir, 'multi_mapped')

        if os.path.exists(actual_output_dir):
            shutil.rmtree(actual_output_dir)  # Remove the actual output directory
            print(f"Removed {actual_output_dir}")
        if os.path.exists(actual_multi_mapped_dir):
            shutil.rmtree(actual_multi_mapped_dir)
            print(f"Removed {actual_multi_mapped_dir}")

if __name__ == '__main__':
    unittest.main()
