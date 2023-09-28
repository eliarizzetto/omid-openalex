import unittest
import os
from os.path import join
import shutil
from omid_openalex.mapping import MetaProcessor
import csv


class MetaProcessorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.CWD_ABS = os.path.dirname(os.path.abspath(__file__))
        self.meta_processor = MetaProcessor()
        self.meta_row1 = {'id': 'omid:br/060924 doi:10.4230/lipics.fun.2021.11', 'title': 'Efficient Algorithms For Battleship', 'author': 'Crombez, Loïc [omid:ra/062503694315 orcid:0000-0002-9542-5276]; Fonseca, Guilherme D. Da [omid:ra/0612046462 orcid:0000-0002-9807-028X]; Gerard, Yan [omid:ra/0612046189 orcid:0000-0002-2664-0650]', 'issue': '', 'volume': '', 'venue': 'LIPIcs : Leibniz International Proceedings In Informatics [omid:br/060182 issn:1868-8969]', 'page': '', 'pub_date': '2020', 'type': 'report', 'publisher': 'Schloss Dagstuhl - Leibniz-Zentrum Für Informatik [omid:ra/0607497]', 'editor': 'Farach, Martin [omid:ra/0614045842 orcid:0000-0003-3616-7788]; Prencipe, Giuseppe [omid:ra/0625025102 orcid:0000-0001-5646-7388]; Uehara, Ryuhei [omid:ra/0617014675 orcid:0000-0003-0895-3765]'}
        self.meta_row2_no_type = {'id': 'omid:br/06070 issn:2703-1012 issn:2703-1004', 'title': 'Musik Und Klangkultur', 'author': '', 'issue': '', 'volume': '', 'venue': '', 'page': '', 'pub_date': '', 'type': '', 'publisher': '', 'editor': ''}
        self.meta_row3 = {'id': 'omid:br/06083 doi:10.4324/9781315372884 doi:10.1201/9781315372884 isbn:9781482244342 isbn:9781482244359', 'title': 'Super-Resolution Imaging In Biomedicine', 'author': '[omid:ar/0605548]; [omid:ar/0605550]; [omid:ar/0605549]', 'issue': '', 'volume': '', 'venue': 'Series In Cellular And Clinical Imaging [omid:br/060155 doi:10.1201/crcsercelcli issn:2372-3939]', 'page': '', 'pub_date': '2016-11-03', 'type': 'reference book', 'publisher': 'Informa Uk Limited [omid:ra/0610116005 crossref:301]', 'editor': 'Diaspro, Alberto [omid:ra/0603396]; Van Zandvoort, Marc A. M. J. [omid:ra/0603397]'}
        self.test_data_dir = join(self.CWD_ABS, 'preprocess_meta_tables', 'input_data')
        self.expected_output_dir = join(self.CWD_ABS, 'preprocess_meta_tables', 'expected_output')
        self.actual_output_dir = join(self.CWD_ABS, 'preprocess_meta_tables', 'actual_output')

    def test_get_entity_ids(self):
        output = self.meta_processor.get_entity_ids(self.meta_row1)
        expected_output = {'omid': 'omid:br/060924', 'ids': 'doi:10.4230/lipics.fun.2021.11', 'type': 'report'}
        self.assertEqual(output, expected_output)

    def test_get_entity_ids2(self):
        output = self.meta_processor.get_entity_ids(self.meta_row2_no_type)
        expected_output = {'omid': 'omid:br/06070', 'ids': 'issn:2703-1012 issn:2703-1004', 'type': ''}
        self.assertEqual(output, expected_output)

    def test_get_venue_ids(self):
        output = self.meta_processor.get_venue_ids(self.meta_row3)
        expected_output = {'omid': 'omid:br/060155', 'ids': 'doi:10.1201/crcsercelcli issn:2372-3939'}
        self.assertEqual(output, expected_output)

    def test_preprocess_meta_tables(self):
        # Define test input and actual output paths
        test_input_dir = self.test_data_dir
        actual_output_dir = self.actual_output_dir

        # Call the preprocess_meta_tables method
        self.meta_processor.preprocess_meta_tables(test_input_dir, actual_output_dir)

        # Define paths to expected output files
        expected_primary_ents_file = join(self.expected_output_dir, 'primary_ents', 'test.csv')
        expected_venues_file = join(self.expected_output_dir, 'venues', 'test.csv')
        expected_resp_ags_file = join(self.expected_output_dir, 'resp_ags', 'test.csv')

        # Define paths to actual output files
        actual_primary_ents_file = join(actual_output_dir, 'primary_ents', 'test.csv')
        actual_venues_file = join(actual_output_dir, 'venues', 'test.csv')
        actual_resp_ags_file = join(actual_output_dir, 'resp_ags', 'test.csv')

        # Perform assertions to check if the actual output files have been created at the intended path
        self.assertTrue(os.path.exists(actual_primary_ents_file), "Expected primary entities file should exist")
        self.assertTrue(os.path.exists(actual_venues_file), "Expected venues file should exist")
        self.assertTrue(os.path.exists(actual_resp_ags_file), "Expected responsible agents file should exist")

        # # Compare the content of actual and expected output files
        self.assertFilesEqual(expected_primary_ents_file, actual_primary_ents_file)
        self.assertFilesEqual(expected_venues_file, actual_venues_file)
        self.assertFilesEqual(expected_resp_ags_file, actual_resp_ags_file)

    # todo: write test (and test data) for preprocess_meta_tables with process_all set to False

    def assertFilesEqual(self, expected_file, actual_file):
        with open(expected_file, 'r', encoding='utf-8') as expected, open(actual_file, 'r', encoding='utf-8') as actual:
            # convert output files to sets of tuples for comparing them (order of rows is slightly messed
            #   up in the output files, due to the fact that the function reads zipped files; the order doesnt matter).
            expected_content = set(tuple(row.items()) for row in csv.DictReader(expected))
            actual_content = set(tuple(row.items()) for row in csv.DictReader(actual))
            self.assertEqual(expected_content, actual_content,f"File content mismatch: {expected_file} and {actual_file}")

    def tearDown(self):
        actual_output_dir = self.actual_output_dir
        if os.path.exists(actual_output_dir):
            shutil.rmtree(actual_output_dir)
            print(f"Removed {actual_output_dir}")


if __name__ == '__main__':
    unittest.main()

