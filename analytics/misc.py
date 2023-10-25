import sqlite3 as sql
from tqdm import tqdm
import logging
from os import listdir, makedirs
from os.path import join
from zipfile import ZipFile
from csv import DictReader, DictWriter
from io import TextIOWrapper
import json
import re

URI_TYPE_DICT = {
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
    'http://purl.org/spar/fabio/WebContent': 'web content'}

def read_compressed_meta_dump(csv_dump_path:str):
    with ZipFile(csv_dump_path) as archive:
        for csv_file in tqdm(archive.namelist()):
            if csv_file.endswith('.csv'):
                with archive.open(csv_file, 'r') as f:
                    reader = DictReader(TextIOWrapper(f, encoding='utf-8'), dialect='unix')
                    for row in reader:
                        yield row

def populate_omid_db(omid_db_path:str, csv_dump_path:str):

    with sql.connect(omid_db_path) as conn:
        cur = conn.cursor()
        cur.execute('DROP TABLE IF EXISTS omid')
        cur.execute('CREATE TABLE Omid (omid TEXT PRIMARY KEY)')
        conn.commit()

        for row in tqdm(read_compressed_meta_dump(csv_dump_path)):
            for pid in row['id'].split():
                if pid.startswith('omid:'):
                    cur.execute('INSERT INTO Omid VALUES (?)', (pid,))
        conn.commit()


def get_br_data_from_rdf(br_rdf_path:str):
    with ZipFile(br_rdf_path) as archive:
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

def normalize_type_string(type_str:str)->str:
    # type_str = type_str.removeprefix('http://purl.org/spar/fabio/')
    # words = re.findall(r'[A-Z][^A-Z]*', type_str)
    # normalized = ' '.join(words).lower()
    normalized = URI_TYPE_DICT.get(type_str)
    return normalized if normalized else type_str

def get_br_omid_and_type(br)->dict:

    omid = f"omid:{br['@id'].removeprefix('https://w3id.org/oc/meta/')}"
    type = ''
    for i in br['@type']:
        if i != 'http://purl.org/spar/fabio/Expression':
            type = normalize_type_string(i)
            break
    return {'omid': omid, 'type': type}

def write_extra_br_tables(br_rdf_path: str, omid_db_path: str, out_dir: str, max_rows_per_file=10000):
    makedirs(out_dir, exist_ok=True)
    file_name = 0
    rows_written = 0
    current_file = None

    def open_new_file():
        nonlocal file_name, current_file
        if current_file:
            current_file.close()
        current_file = open(join(out_dir, f'{file_name}.csv'), 'w', encoding='utf-8', newline='')
        writer = DictWriter(current_file, fieldnames=['omid', 'type'])
        writer.writeheader()
        return writer

    writer = open_new_file()

    with sql.connect(omid_db_path) as conn:
        cur = conn.cursor()

        for br in tqdm(get_br_data_from_rdf(br_rdf_path)):
            lookup_omid = br['@id']
            cur.execute('SELECT omid FROM Omid WHERE omid=?', (lookup_omid,))
            res = cur.fetchone()
            if not res:
                out_row = get_br_omid_and_type(br)
                writer.writerow(out_row)
                rows_written += 1

            if rows_written >= max_rows_per_file:
                file_name += 1
                writer = open_new_file()
                rows_written = 0

    if current_file:
        current_file.close()

