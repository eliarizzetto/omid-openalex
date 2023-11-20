import csv
import sqlite3 as sql
from tqdm import tqdm
import logging
from os import listdir, makedirs, walk
from os.path import join, isdir
from zipfile import ZipFile
from csv import DictReader, DictWriter
from io import TextIOWrapper
import json
from collections import defaultdict
import re
from pprint import pprint
import gzip


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
    csv.field_size_limit(131072 * 12)
    with ZipFile(csv_dump_path) as archive:
        for csv_file in tqdm(archive.namelist()):
            if csv_file.endswith('.csv'):
                with archive.open(csv_file, 'r') as f:
                    reader = DictReader(TextIOWrapper(f, encoding='utf-8'), dialect='unix')
                    for row in reader:
                        yield row

def read_compressed_openalex_dump(in_dir:str):

    logging.info(f'Processing input folder {in_dir} for OpenAlex table creation')
    input_files = [join(root, file) for root, dirs, files in walk(in_dir) for file in files if file.endswith('.gz')]

    for f in tqdm(input_files):
        logging.info(f'Processing file {f}')
        with gzip.open(f, 'r') as inp_jsonl:
            for line in inp_jsonl:
                try:
                    line = json.loads(line)
                    yield line
                except json.decoder.JSONDecodeError as e:
                    logging.error(f'Error while processing {f}: {e}.\n Critical entity: {line}')
                    print(f'Error while processing {f}: {e}.\n Critical entity: {line}')
                    continue

def populate_omid_db(omid_db_path:str, meta_tables_csv:str):
    """
    Creates a flat-file database with only one table and one column: the OMID of the bibliographic resource (omid, str).

    :param omid_db_path:
    :param meta_tables_csv: the path to the folder storing the CSV files resulting from the pre-processing of the CSV Meta dump.
    :return:
    """
    with sql.connect(omid_db_path) as conn:
        cur = conn.cursor()
        cur.execute('DROP TABLE IF EXISTS omid')
        cur.execute('CREATE TABLE Omid (omid TEXT PRIMARY KEY)')
        conn.commit()

        for row in tqdm(read_output_tables(meta_tables_csv)):
            curr_omid = row['omid']
            cur.execute('INSERT INTO Omid VALUES (?)', (curr_omid,))
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
    csv.field_size_limit(131072 * 12)

    def open_new_file():
        nonlocal file_name, current_file
        if current_file:
            current_file.close()
        current_file = open(join(out_dir, f'{file_name}.csv'), 'w', encoding='utf-8', newline='')
        writer = DictWriter(current_file, fieldnames=['omid', 'type', 'omid_only'], dialect='unix')
        writer.writeheader()
        return writer

    writer = open_new_file()

    with sql.connect(omid_db_path) as conn:
        cur = conn.cursor()

        for br in tqdm(get_br_data_from_rdf(br_rdf_path)):
            lookup_omid = br['@id'].replace('https://w3id.org/oc/meta/', 'omid:')
            cur.execute('SELECT omid FROM Omid WHERE omid=?', (lookup_omid,))
            res = cur.fetchone()
            if not res:
                out_row = get_br_omid_and_type(br)
                if not br.get('http://purl.org/spar/datacite/hasIdentifier'):
                    out_row['omid_only'] = True

                writer.writerow(out_row)
                rows_written += 1

            if rows_written >= max_rows_per_file:
                file_name += 1
                writer = open_new_file()
                rows_written = 0

    if current_file:
        current_file.close()

# def read_output_tables(dir:str):
#     """
#     Reads the output CSV non-compressed tables and yields the rows.
#     :param dir:
#     :return:
#     """
#     for file in listdir(dir):
#         if file.endswith('.csv'):
#             with open(join(dir, file), 'r', encoding='utf-8') as f:
#                 reader = DictReader(f, dialect='unix')
#                 for row in reader:
#                     yield row


def read_output_tables(*dirs):
    """
    Reads the output CSV non-compressed tables from one or more directories and yields the rows.
    :param dirs: One or more directories to read files from, provided as variable-length arguments.
    :return: Yields rows from all CSV files in the specified directories.
    """
    for directory in dirs:
        if isinstance(directory, str):
            for file in listdir(directory):
                if file.endswith('.csv'):
                    with open(join(directory, file), 'r', encoding='utf-8') as f:
                        reader = DictReader(f, dialect='unix')
                        for row in reader:
                            yield row
        else:
            raise ValueError("Each argument must be a string representing a directory path.")

def analyse_provenance(db_path, *dirs):
    res = defaultdict(lambda: defaultdict(int))
    omid_only_distr = defaultdict((lambda :defaultdict(lambda: defaultdict(int))))
    with sql.connect(db_path) as conn:
        cur = conn.cursor()
        query = 'SELECT source_uri FROM Provenance WHERE br_uri = ?'

        for row in tqdm(read_output_tables(*dirs)):
            br = row['omid'].replace('omid:', 'https://w3id.org/oc/meta/')
            cur.execute(query, (br,))
            query_res = cur.fetchone()
            if query_res:
                source = ' '.join(tuple(set(json.loads(query_res[0]))))
                res[row['type']][source] +=1

                if row.get('omid_only'):
                # if no other ID than OMID, a value has been specified for this field, else it is empty or absent at all
                    omid_only_distr[row['type']][source]['omid_only'] += 1
                else:
                    omid_only_distr[row['type']][source]['other_pids'] += 1
            else:
                logging.warning(f'No provenance information found for {row["omid"]}')

    for k, v in res.items():
        res[k] = dict(v)

    for k, v in omid_only_distr.items():
        omid_only_distr[k] = dict(v)
        for k2, v2 in omid_only_distr[k].items():
            omid_only_distr[k][k2] = dict(v2)

    logging.info(f'Provenance analysis results: {dict(res)}')
    logging.info(f'OMID-only resources count by type (these BRs have no other PIDs): {dict(omid_only_distr)}')
    return dict(res), dict(omid_only_distr)



if __name__ == '__main__':
    csv.field_size_limit(131072 * 12)

    # # create omid db
    omid_db_path = 'E:/omid.db'
    meta_tables_csv = "E:/meta_reduced_oct23/primary_ents"
    populate_omid_db(omid_db_path=omid_db_path, meta_tables_csv=meta_tables_csv)

    # # Write tables of OMIDs that are not in the CSV (then count it)
    br_rdf_path = 'E:/br.zip'
    out_dir = 'E:/extra_br_tables'
    write_extra_br_tables(br_rdf_path=br_rdf_path, omid_db_path=omid_db_path, out_dir=out_dir, max_rows_per_file=10000)

    # # Analyse provenance counts
    logging.basicConfig(level=logging.INFO, filename='E:/provenance_analysis_11nov.log', filemode='w')
    pprint(analyse_provenance("E:/provenance.db", 'E:/extra_br_tables', 'E:/mapping_oct_23/non_mapped'))


