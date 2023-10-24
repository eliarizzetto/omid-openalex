import sqlite3 as sql
from tqdm import tqdm
import logging
from os import listdir
from zipfile import ZipFile
from csv import DictReader, DictWriter
from io import TextIOWrapper
import json
import re



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
    type_str = type_str.removeprefix('http://purl.org/spar/fabio/')
    words = re.findall(r'[A-Z][^A-Z]*', type_str)
    normalized = ' '.join(words).lower()
    return normalized

def get_br_omid_and_type(br)->dict:
    omid = f"omid:{br['@id'].removeprefix('https://w3id.org/oc/meta/')}"
    type = ''
    for i in br['@type']:
        if i != 'http://purl.org/spar/fabio/Expression':
            type = normalize_type_string(i)
            break
    return {'omid': omid, 'type': type}


def write_extra_br_tables(br_rdf_path:str, omid_db_path:str, out_dir:str):
    # create new csv file each 100000 rows
    row_count = 0
    with sql.connect(omid_db_path) as conn:
        cur = conn.cursor()
        for br in tqdm(get_br_data_from_rdf(br_rdf_path)):
            lookup_omid = br['@id']
            cur.execute('SELECT omid FROM Omid WHERE omid=?', (lookup_omid,))
            res = cur.fetchone()
            if not res:
                ... # scrivi su file csv se numero righe < 100000 (da impostare)
                ... # out_row = get_br_omid_and_type(br)
