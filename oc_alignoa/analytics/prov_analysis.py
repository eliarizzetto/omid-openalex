import json
from datetime import datetime
from zipfile import ZipFile
import sqlite3 as sql
from tqdm import tqdm
import logging
from oc_alignoa.utils import read_csv_tables, MultiFileWriter
from collections import defaultdict
from os import makedirs
from os.path import dirname
import csv
import argparse
import yaml

class ProvenanceAnalyser:

    def __init__(
            self,
            br_rdf_path,
            prov_db_path,
            meta_tables_csv,
            omid_db_path,
            extra_br_out_dir,
            non_mapped_dir,
            results_out_path ='provenance_analysis_results.json'
    ):
        self.br_rdf_path = br_rdf_path
        self.prov_db_path = prov_db_path
        self.meta_tables_csv = meta_tables_csv
        self.omid_db_path = omid_db_path
        self.extra_br_out_dir = extra_br_out_dir
        self.results_out_path = results_out_path
        self.dirs_to_analyse = [extra_br_out_dir, non_mapped_dir]
        self.URI_TYPE_DICT = {
            'http://purl.org/spar/doco/Abstract': 'abstract',
            'http://purl.org/spar/fabio/ArchivalDocument': 'archival document',
            'http://purl.org/spar/fabio/AudioDocument': 'audio document',
            'http://purl.org/spar/fabio/Book': 'book',
            'http://purl.org/spar/fabio/BookChapter': 'book chapter',
            'http://purl.org/spar/fabio/ExpressionCollection': 'book section',
            'http://purl.org/spar/fabio/BookSeries': 'book series',
            'http://purl.org/spar/fabio/BookSet': 'book set',
            'http://purl.org/spar/fabio/ComputerProgram': 'computer program',
            'http://purl.org/spar/doco/Part': 'book part',
            'http://purl.org/spar/fabio/Expression': '',
            'http://purl.org/spar/fabio/DataFile': 'dataset',
            'http://purl.org/spar/fabio/DataManagementPlan': 'data management plan',
            'http://purl.org/spar/fabio/Thesis': 'dissertation',
            'http://purl.org/spar/fabio/Editorial': 'editorial',
            'http://purl.org/spar/fabio/Journal': 'journal',
            'http://purl.org/spar/fabio/JournalArticle': 'journal article',
            'http://purl.org/spar/fabio/JournalEditorial': 'journal editorial',
            'http://purl.org/spar/fabio/JournalIssue': 'journal issue',
            'http://purl.org/spar/fabio/JournalVolume': 'journal volume',
            'http://purl.org/spar/fabio/Newspaper': 'newspaper',
            'http://purl.org/spar/fabio/NewspaperArticle': 'newspaper article',
            'http://purl.org/spar/fabio/NewspaperIssue': 'newspaper issue',
            'http://purl.org/spar/fr/ReviewVersion': 'peer_review',
            'http://purl.org/spar/fabio/AcademicProceedings': 'proceedings',
            'http://purl.org/spar/fabio/Preprint': 'preprint',
            'http://purl.org/spar/fabio/Presentation': 'presentation',
            'http://purl.org/spar/fabio/ProceedingsPaper': 'proceedings article',
            'http://purl.org/spar/fabio/ReferenceBook': 'reference book',
            'http://purl.org/spar/fabio/ReferenceEntry': 'reference entry',
            'http://purl.org/spar/fabio/ReportDocument': 'report',
            'http://purl.org/spar/fabio/RetractionNotice': 'retraction notice',
            'http://purl.org/spar/fabio/Series': 'series',
            'http://purl.org/spar/fabio/SpecificationDocument': 'standard',
            'http://purl.org/spar/fabio/WebContent': 'web content'
        }

    def _get_provenance_data(self):

        with ZipFile(self.br_rdf_path) as archive:
            for filepath in archive.namelist():
                if filepath.endswith('prov/se.zip'):
                    with ZipFile(archive.open(filepath)) as prov_archive:
                        for prov_file in prov_archive.namelist():
                            if prov_file.endswith('se.json'):
                                with prov_archive.open(prov_file) as f:
                                    data: list = json.load(f)
                                    for obj in data:
                                        yield obj

    @staticmethod
    def _get_entity_prov(prov_graph):
        out_row = dict()
        out_row['source'] = set()
        out_row['br'] = ''
        for snapshot in prov_graph['@graph']:
            primary_source = snapshot.get('http://www.w3.org/ns/prov#hadPrimarySource')  # list|None
            if primary_source:
                for i in primary_source:
                    out_row['source'].add(i['@id'])

        out_row['source'] = list(out_row['source'])
        if not out_row['source']:
            logging.info(f'No primary source found for this entity. Entity processed: \n{prov_graph}')
            return None
        while not out_row['br']:
            for snapshot in prov_graph['@graph']:
                if snapshot.get('http://www.w3.org/ns/prov#specializationOf'):
                    out_row['br'] = snapshot['http://www.w3.org/ns/prov#specializationOf'][0]['@id']
                    return out_row
        else:
            return None

    def populate_prov_db(self):
        """
        Creates a database with only one table and two columns: the URI of the bibliographic resource (br_uri, str)
        which is the primary key, and the primary source(s) for that bibliographic resource (source_uri, a list encoded
        as JSON string).
        The database is not normalized, but it is easier and faster to query for bibliographic resources and related
        sources. The results obtained by querying for a given bibliographic resource URI must be further processed
        on the Python side.

        :return: None
        """
        makedirs(dirname(self.prov_db_path), exist_ok=True)
        with sql.connect(self.prov_db_path) as conn:
            cur = conn.cursor()
            cur.execute('CREATE TABLE IF NOT EXISTS Provenance (br_uri TEXT PRIMARY KEY, source_uri TEXT)')
            for prov_graph in tqdm(self._get_provenance_data(), desc='Populating provenance database', unit='entity'):
                entity_prov = self._get_entity_prov(prov_graph)
                if entity_prov:
                    cur.execute('INSERT INTO Provenance VALUES (?, ?)', (entity_prov['br'], json.dumps(entity_prov['source'])))
            conn.commit()

    def populate_omid_db(self):
        """
        Creates a flat-file database with only one table and one column: the OMID of the bibliographic resource (omid, str).
        :return:
        """
        makedirs(dirname(self.omid_db_path), exist_ok=True)
        with sql.connect(self.omid_db_path) as conn:
            cur = conn.cursor()
            cur.execute('DROP TABLE IF EXISTS omid')
            cur.execute('CREATE TABLE Omid (omid TEXT PRIMARY KEY)')
            conn.commit()

            for row in read_csv_tables(self.meta_tables_csv):
                curr_omid = row['omid']
                cur.execute('INSERT INTO Omid VALUES (?)', (curr_omid,))
            conn.commit()

    def get_br_data_from_rdf(self):
        with ZipFile(self.br_rdf_path) as archive:
            for filepath in archive.namelist():
                if 'prov' not in filepath and filepath.endswith('.zip'):
                    with ZipFile(archive.open(filepath)) as br_data_archive:
                        for file in br_data_archive.namelist():
                            if file.endswith('.json'):
                                with br_data_archive.open(file) as f:
                                    data: list = json.load(f)
                                    for obj in data:
                                        for br in obj['@graph']:
                                            yield br

    def _normalize_type_string(self, type_str: str) -> str:
        normalized = self.URI_TYPE_DICT.get(type_str)
        return normalized if normalized else type_str

    def _get_br_omid_and_type(self, br) -> dict:

        omid = f"omid:{br['@id'].removeprefix('https://w3id.org/oc/meta/')}"
        type = ''
        for i in br['@type']:
            if i != 'http://purl.org/spar/fabio/Expression':
                type = self._normalize_type_string(i)
                break
        return {'omid': omid, 'type': type}

    def write_extra_br_tables(self):
        """
        Writes CSV tables for OC Meta bibliographic resources that have not been processed by the mapping tool,
        either because there are no external IDs associated with them, or because they were not included in
        the CSV dump of OC Meta.
        :return:
        """
        makedirs(self.extra_br_out_dir, exist_ok=True)
        csv.field_size_limit(131072 * 12)
        fieldnames = ['omid', 'type', 'omid_only']

        with sql.connect(self.omid_db_path) as conn, MultiFileWriter(self.extra_br_out_dir, fieldnames=fieldnames) as writer:
            cur = conn.cursor()

            for br in tqdm(self.get_br_data_from_rdf(), desc='Writing non-processed entities to tables', unit='br'):
                lookup_omid = br['@id'].replace('https://w3id.org/oc/meta/', 'omid:')
                cur.execute('SELECT omid FROM Omid WHERE omid=?', (lookup_omid,))
                res = cur.fetchone()
                if not res:
                    out_row = self._get_br_omid_and_type(br)
                    if not br.get('http://purl.org/spar/datacite/hasIdentifier'):
                        out_row['omid_only'] = True

                    writer.write_row(out_row)

    @staticmethod
    def sort_prov_analysis_results(provenance_data: dict):
        """
        Sort the results of the analysis on provenance data by the sum of values in nested dictionaries in descending
        order. Each nested dictionary is also sorted by values in descending order.
        :param provenance_data:
        :return:
        """
        for key in provenance_data:
            provenance_data[key] = dict(
                sorted(provenance_data[key].items(), key=lambda x: sum(x[1].values()), reverse=True))

        # Sort the outer dictionary by the sum of values in nested dictionaries in descending order
        result = dict(
            sorted(provenance_data.items(), key=lambda x: sum([v2 for v in x[1].values() for v2 in v.values()]),
                   reverse=True))
        return result

    def analyse_provenance(self):

        res = defaultdict(lambda: defaultdict(lambda: {'omid_only': 0, 'other_pids': 0}))
        with sql.connect(self.prov_db_path) as conn:
            cur = conn.cursor()
            query = 'SELECT source_uri FROM Provenance WHERE br_uri = ?'

            for row in read_csv_tables(*self.dirs_to_analyse):
                br = row['omid'].replace('omid:', 'https://w3id.org/oc/meta/')
                cur.execute(query, (br,))
                query_res = cur.fetchone()
                if query_res:
                    source = ' '.join(tuple(set(json.loads(query_res[0]))))

                    if row.get('omid_only'):
                        # if no other ID than OMID, a value has been specified for this field, else it is empty or absent at all
                        res[row['type']][source]['omid_only'] += 1
                    else:
                        res[row['type']][source]['other_pids'] += 1
                else:
                    logging.warning(f'No provenance information found for {row["omid"]}')

        for k, v in res.items():
            res[k] = dict(v)
            for k2, v2 in res[k].items():
                res[k][k2] = dict(v2)

        res = self.sort_prov_analysis_results(dict(res))
        logging.info(f'Provenance analysis results: {res}')

        with open(self.results_out_path, 'w', encoding='utf-8') as fileout:
            json.dump(res, fileout, indent=4)

        return dict(res)


if __name__ == '__main__':
    log_file = f'prov_analysis_{datetime.now().strftime("%Y-%m-%d")}.log'
    logging.basicConfig(level=logging.WARNING, filename=log_file, filemode='w')

    parser = argparse.ArgumentParser(description='Provenance Analysis Tool')
    parser.add_argument('-c', '--config', default='prov_config.yaml', help='Path to the YAML configuration file')
    args = parser.parse_args()

    with open(args.config, 'r') as file:
        config_data = yaml.safe_load(file)

    analyser = ProvenanceAnalyser(
        config_data['br_rdf_path'],
        config_data['prov_db_path'],
        config_data['meta_tables_csv'],
        config_data['omid_db_path'],
        config_data['extra_br_out_dir'],
        config_data['non_mapped_dir'],
        results_out_path=config_data['results_out_path']
    )

    print('Populating provenance database...')
    analyser.populate_prov_db()
    print('Populating OMID database...')
    analyser.populate_omid_db()
    print('Writing extra BR tables...')
    analyser.write_extra_br_tables()
    print('Analysing provenance...')
    analyser.analyse_provenance()
