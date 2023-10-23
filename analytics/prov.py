import json
from pprint import pprint
from zipfile import ZipFile
import sqlite3 as sql
from tqdm import tqdm
import logging

class ProvenanceAnalyser:

    def __init__(self, dump_path='', db_path=''):
        self.dump_path = dump_path
        self.db_path = db_path

    def get_provenance_data(self):

        with ZipFile(self.dump_path) as archive:
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
    def get_entity_prov(prov_graph):

        for subgraph in prov_graph['@graph']:
            # ATTENTION: This assumes that there is ONLY ONE valid provenance subgraph per bibliographic entity.
            if not subgraph.get('http://www.w3.org/ns/prov#invalidatedAtTime'):
                br_uri = subgraph.get('http://www.w3.org/ns/prov#specializationOf')[0]['@id']
                if subgraph.get('http://www.w3.org/ns/prov#hadPrimarySource'):
                    prov_field = [i['@id'] for i in subgraph['http://www.w3.org/ns/prov#hadPrimarySource']]
                else:
                    logging.info(f'No primary source found for this entity. Entity processed: \n{prov_graph}')
                    return None # if there is no primary source, there is no valid provenance to look at

                return {'br': br_uri, 'source': prov_field}

    def populate_flat_file_db(self):
        """
        Creates a database with only one table and two columns: the URI of the bibliographic resource (br_uri, str)
        which is the primary key, and the primary source(s) for that bibliographic resource (source_uri, list encoded
        as JSON string).
        The database is not normalized, but it is easier and faster to query for bibliographic resources and related
        sources. The results obtained by querying for a given bibliographic resource URI must be further processed
        on the Python side.

        :return: None
        """
        with sql.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute('CREATE TABLE IF NOT EXISTS Provenance (br_uri TEXT PRIMARY KEY, source_uri TEXT)')
            for prov_graph in tqdm(self.get_provenance_data()):
                entity_prov = self.get_entity_prov(prov_graph)
                if entity_prov:
                    cur.execute('INSERT INTO Provenance VALUES (?, ?)', (entity_prov['br'], json.dumps(entity_prov['source'])))
            conn.commit()

    def populate_prov_db(self):
        """
        DEPRECATED. Use populate_flat_file_db instead.
        Creates a (normalized) database with two tables: BibliographicResource and Source. BibliographicResource
        has two columns: id (INTEGER PRIMARY KEY) and uri (TEXT NOT NULL). Source has three columns: id (INTEGER
        PRIMARY KEY), br_id (INTEGER NOT NULL, FOREIGN KEY REFERENCES BibliographicResource(id)), and source_uri
        (TEXT NOT NULL). It is possible to query for bibliographic resources and related sources using JOINs, but
        it is much slower than querying a flat file database.
        :return: None
        """

        with sql.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute('PRAGMA foreign_keys = ON')  # Enable foreign key support
            cur.execute("""
            CREATE TABLE IF NOT EXISTS BibliographicResource (
                uri TEXT PRIMARY KEY
            )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS Source (
                    id INTEGER PRIMARY KEY,
                    br_uri TEXT NOT NULL,
                    source_uri TEXT NOT NULL,
                    FOREIGN KEY (br_uri) REFERENCES BibliographicResource(uri)
                )
            """)
            for prov_graph in tqdm(self.get_provenance_data()):
                entity_prov = self.get_entity_prov(prov_graph)
                if entity_prov:
                    cur.execute('INSERT INTO BibliographicResource (uri) VALUES (?)', (entity_prov['br'],))
                    for source_url in entity_prov['source']:
                        cur.execute('INSERT INTO Source (br_uri, source_uri) VALUES (?, ?)', (entity_prov['br'], source_url))
            conn.commit()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, filename='prov.log', filemode='w')
    pa = ProvenanceAnalyser(dump_path='E:/br.zip', db_path='provenance.db')
    pa.populate_flat_file_db()