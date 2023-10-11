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

from os.path import join, splitext, basename, isdir
from os import listdir, makedirs, walk
import csv
import json
from io import TextIOWrapper
from zipfile import ZipFile
from typing import Generator, Literal, Union
import logging
import gzip
import sqlite3 as sql
from csv import DictReader, DictWriter
from tqdm import tqdm
import time
import pandas as pd


class MetaProcessor:
    def __init__(self):
        pass

    @staticmethod
    def get_entity_ids(row: dict) -> Union[dict, None]:
        """
        Extracts the IDs in the value of the 'id' field from a row of the OC Meta dump.
        :param row: a row of the OC Meta dump
        :return: row with three fields: the OMID; a string with the PIDs of the entity in the 'id' field separated by
        a single whitespace; and the type of resource. None if the entity in the 'id' field has no external PIDs.
        """
        output_row = dict()
        output_row['omid'] = ''
        output_row['ids'] = []
        output_row['type'] = row['type']

        # get resource's omid and other IDs
        for id in row['id'].split():
            if id.startswith('omid:'):
                output_row['omid'] = id
            else:  # i.e., if prefix is one of: 'doi:','pmid:','pmcid:','issn:','isbn:','wikidata:'
                output_row['ids'].append(id)
        if output_row['ids']:  # if the resource has at least one external ID that is supported by OpenAlex
            output_row['ids'] = ' '.join(output_row['ids'])
            return output_row

    @staticmethod
    def get_venue_ids(row: dict) -> Union[dict, None]:
        """
        Extracts the IDs in the value of the 'venue' field from a row of the OC Meta dump.
            :param row: a row of the OC Meta dump
            :return: row with two fields: the OMID, and a string with the PIDs of the entity in the venue field
            separated by a single whitespace. None if the entity in the venue field has no external IDs
            supported by OpenAlex or is empty.
        """
        output_row = dict()
        output_row['omid'] = ''
        output_row['ids'] = []

        v = row['venue']
        if v:
            venue_ids = v[v.index('[') + 1:v.index(']')].strip().split()
            for id in venue_ids:
                if id.startswith('omid:'):
                    output_row['omid'] = id
                else:  # i.e., if prefix is one of: 'doi:','pmid:','pmcid:','issn:','isbn:','wikidata:'
                    output_row['ids'].append(id)

            if output_row['ids']:  # if the entity in venue has at least one external ID that is supported by OpenAlex
                output_row['ids'] = ' '.join(output_row['ids'])
                return output_row
        else:
            return None

    @staticmethod
    def get_ra_ids(row: dict, field: Literal['author', 'publisher', 'editor']) -> Generator[dict, None, None]:
        """
        Extracts the IDs in the value of the 'author', 'publisher', or 'editor' field from a row of the OC Meta dump.
        :param row: a row of the OC Meta dump
        :param field: the field of the row to be processed (one among 'author', 'publisher', 'editor')
        :return: a generator of rows with three fields: the OMID, the PIDs of the entity in the field, and the role of
        the entity in the field (one among 'author', 'publisher', 'editor'). None if the entity in the field has no
        external IDs supported by OpenAlex or is empty.
        """
        output_row = dict()
        if row[field]:
            if field == 'publisher':  # no separator in the publisher field -> only one entity!
                entities = [row[field]]
            else:  # author and editor fields can contain multiple entities, separated by '; '
                entities = row[field].split('; ')
            for ra_entity in entities:
                output_row['omid'] = ''
                output_row['ids'] = []
                output_row['ra_role'] = field
                ra_entity = ra_entity.strip()
                try:
                    start = ra_entity.index('[') + 1
                    end = ra_entity.index(']')
                    for ra_id in ra_entity[start:end].strip().split():
                        if ra_id.startswith('omid:'):
                            output_row['omid'] = ra_id
                        else:
                            output_row['ids'].append(ra_id)
                    if output_row['ids']:
                        output_row['ids'] = ' '.join(output_row['ids'])
                        yield output_row
                except ValueError:
                    logging.error(
                        f'Error: {field} field of row {row} is not in the expected format. The entity corresponding to {ra_entity} is not processed.')
                    continue

    def preprocess_meta_tables(self, meta_in:str, meta_ids_out:str, all_rows:bool = True) -> None:
        """
        Preprocesses the OC Meta tables to create reduced tables with essential metadata. For each entity represented in a
        row in the original table, the reduced output table contains the OMID ('omid' field) and the PIDs ('ids' field) of
        the entity, as well as the type of resource ('type' field).
        For the entity in the 'venue' field of a row in the original table, the reduced output table only contains the OMID
        and the PIDs of the entity ('omid' and 'ids' fields).
        For each entity in the 'author', 'publisher', and 'editor' fields of a row in the original table, the reduced output
        table contains the OMID and the PIDs of the entity ('omid' and 'ids' fields), as well as the role of the entity
        ('ra_role' field, with the value being one of 'author', 'publisher', or 'editor').
            :param all_rows: flag to indicate whether to process all rows or only those that do not already have an openalex ID
            :param meta_in: the directory where the OC Meta input tables are stored
            :param meta_ids_out: the directory where the reduced tables will be written in the form of multiple
            CSV file. Each CSV file will be named as the file it was created from, but prefixed with:
                * 'primary_ents_' if the table concerns the entity whose IDs are stored in the 'id' field of
                 the original table
                * 'venues_' if the table concerns the entity whose IDs are stored in the
                 'venue' field of the original table
                * 'resp_ags_' if the table concerns the entities whose IDs are stored in the 'author', 'publisher',
                    or 'editor' fields of the original table
            :return: None (writes the reduced tables to disk)
        """
        csv.field_size_limit(131072 * 4)  # quadruple the default limit for csv field size
        primary_ents_out_dir = join(meta_ids_out, 'primary_ents')
        makedirs(primary_ents_out_dir, exist_ok=True)
        venues_out_dir = join(meta_ids_out, 'venues')
        makedirs(venues_out_dir, exist_ok=True)
        resp_ags_out_dir = join(meta_ids_out, 'resp_ags')
        makedirs(resp_ags_out_dir, exist_ok=True)
        logging.info(f'Processing input folder {meta_in} for reduced OC Meta table creation')
        process_start_time = time.time()
        for root, dirs, files in walk(meta_in):
            for file in files:
                if file.endswith('.zip'):
                    archive_path = join(root, file)
                    with ZipFile(archive_path) as archive:
                        for csv_name in tqdm(archive.namelist()):
                            if csv_name.endswith('.csv'):
                                logging.info(f'Processing {csv_name}')
                                file_start_time = time.time()
                                primary_ents_out_path = join(primary_ents_out_dir, basename(csv_name))
                                venues_out_path = join(venues_out_dir, basename(csv_name))
                                resp_ags_out_path = join(resp_ags_out_dir, basename(csv_name))
                                out_venue_rows = set()  # stores rows dicts converted to tuples in a single file (venues)
                                out_ra_rows = set()  # stores rows dicts converted to tuples in a single file (resp_ags)
                                with archive.open(csv_name, 'r') as csv_file, open(primary_ents_out_path, 'w',
                                                                                   newline='',
                                                                                   encoding='utf-8') as primary_ents_out_file, open(
                                    venues_out_path, 'w', newline='', encoding='utf-8') as venues_out_file, open(
                                    resp_ags_out_path, 'w', newline='', encoding='utf-8') as resp_ags_out_file:
                                    primary_ents_writer = DictWriter(primary_ents_out_file, dialect='unix',
                                                                     fieldnames=['omid', 'ids', 'type'])
                                    venues_writer = DictWriter(venues_out_file, dialect='unix',
                                                               fieldnames=['omid', 'ids'])
                                    resp_ags_writer = DictWriter(resp_ags_out_file, dialect='unix',
                                                                 fieldnames=['omid', 'ids', 'ra_role'])
                                    primary_ents_writer.writeheader()
                                    venues_writer.writeheader()
                                    resp_ags_writer.writeheader()
                                    try:
                                        reader = DictReader(TextIOWrapper(csv_file, encoding='utf-8'), dialect='unix')
                                        for row in reader:

                                            # skip row if entity already has an openalex ID and all_rows is False
                                            if any(pid.startswith('openalex:') for pid in row['id'].split()) and all_rows is False:
                                                continue

                                            primary_entity_out_row: dict = self.get_entity_ids(row)
                                            venue_out_row: dict = self.get_venue_ids(row)

                                            # create a row for the resource uniquely identified by the OMID in the 'id' field
                                            if primary_entity_out_row:
                                                primary_ents_writer.writerow(
                                                    primary_entity_out_row)  # primary entities are unique -> write them directly to the output file

                                            # create a row for the resource identified by the OMID in the 'venue' field
                                            if venue_out_row:
                                                out_venue_rows.add(tuple(venue_out_row.items()))

                                            # create a row for each of the entities in the responsible agent fields ('author', 'publisher', 'editor' of the input row
                                            for field in ['author', 'publisher', 'editor']:
                                                for ra_out_row in self.get_ra_ids(row, field):
                                                    # todo: consider splitting authors, publishers, editors into separate tables
                                                    #  (and modifying the get_ra_ids function accordingly,
                                                    #  i.e. removing a then unnecessary 'ra_role' field in the output dictionary)

                                                    out_ra_rows.add(tuple(ra_out_row.items()))

                                        # this prevents duplicates inside the same file (not in the whole dataset)
                                        venues_writer.writerows(map(dict, out_venue_rows))
                                        resp_ags_writer.writerows(map(dict, out_ra_rows))

                                        logging.info(
                                            f'Processing {csv_name} took {time.time() - file_start_time} seconds')
                                    except csv.Error as e:
                                        logging.error(f'Error while processing {csv_name}: {e}')


class OpenAlexProcessor:

    def __init__(self):
        pass

    @staticmethod
    def get_work_ids(inp_entity: dict) -> Generator[dict, None, None]:
        """
        Extracts the IDs of an OpenAlex Work entity.
        :param inp_entity: the dict object representing the OpenAlex Work entity
        :return: a generator of dicts, as many as the external PIDs found for the resource, each with two fields:
        one storing the external PID (one among the ones supported by OC Meta) and the other storing the OpenAlex ID.
        None if the entity has no external PIDs supported by OC Meta.
        """
        ids = set()
        openalex_id = inp_entity['id'].removeprefix('https://openalex.org/')
        for k, v in inp_entity['ids'].items():
            if k == 'doi':
                ids.add('doi:' + v.removeprefix('https://doi.org/'))
            elif k == 'pmid':
                ids.add('pmid:' + v.removeprefix('https://pubmed.ncbi.nlm.nih.gov/'))
            elif k == 'pmcid':
                ids.add('pmcid:' + v.removeprefix('https://www.ncbi.nlm.nih.gov/pmc/articles/'))
        if ids:
            for item in ids:
                output_row = {'supported_id': item, 'openalex_id': openalex_id}
                yield output_row

    @staticmethod
    def get_source_ids(inp_entity: dict) -> Generator[dict, None, None]:
        """
        Extracts the IDs of an OpenAlex Source entity.
        :param inp_entity: the dict object representing the OpenAlex Source entity
        :return: a generator of dicts, as many as the external PIDs found for the resource, each with two fields:
        one storing the external PID (one among the ones supported by OC Meta) and the other storing the OpenAlex ID.
        None if the entity has no external PIDs supported by OC Meta.
        """
        ids = set()
        openalex_id = inp_entity['id'].removeprefix('https://openalex.org/')
        for k, v in inp_entity['ids'].items():
            if k == 'issn':  # ISSNs are stored in a list, not in a string like other ID types!
                ids.update(['issn:' + i for i in v])  # ISSNs are NOT recorded as URIs, so there is no prefix to remove
            elif k == 'wikidata':
                if v.startswith('http:'):
                    ids.add('wikidata:' + v.removeprefix('http://www.wikidata.org/entity/'))
                elif v.startswith('https:'):
                    ids.add('wikidata:' + v.removeprefix('https://www.wikidata.org/entity/'))

        issn = inp_entity['ids'].get('issn')
        issn_l = inp_entity['ids'].get('issn_l')

        if not issn and issn_l:
            ids.add('issn:' + issn_l)
        if ids:
            for item in ids:
                output_row = {'supported_id': item, 'openalex_id': openalex_id}
                yield output_row

    @staticmethod
    def get_author_ids(inp_entity: dict) -> Generator[dict, None, None]:
        """
        Extracts the IDs of an OpenAlex Author entity.
        :param inp_entity: the dict object representing the OpenAlex Author entity
        :return: a generator of dicts, as many as the external PIDs found for the resource, each with two fields:
        one storing the external PID (one among the ones supported by OC Meta) and the other storing the OpenAlex ID.
        None if the entity has no external PIDs supported by OC Meta.
        """
        ids = set()
        openalex_id = inp_entity['id'].removeprefix('https://openalex.org/')
        for k, v in inp_entity['ids'].items():
            if k == 'orcid':
                ids.add('orcid:' + v.removeprefix('https://orcid.org/'))
        if ids:
            for item in ids:
                output_row = {'supported_id': item, 'openalex_id': openalex_id}
                yield output_row

    @staticmethod
    def get_institution_ids(inp_entity: dict) -> Generator[dict, None, None]:
        """
        Extracts the IDs of an OpenAlex Institution entity.
        :param inp_entity: the dict object representing the OpenAlex Institution entity
        :return: a generator of dicts, as many as the external PIDs found for the resource, each with two fields:
        one storing the external PID (one among the ones supported by OC Meta) and the other storing the OpenAlex ID.
        None if the entity has no external PIDs supported by OC Meta.
        """
        ids = set()
        openalex_id = inp_entity['id'].removeprefix('https://openalex.org/')
        for k, v in inp_entity['ids'].items():
            if k == 'ror':
                ids.add('ror:' + v.removeprefix('https://ror.org/'))
            elif k == 'wikidata':
                if v.startswith('http:'):
                    ids.add('wikidata:' + v.removeprefix('http://www.wikidata.org/entity/'))
                elif v.startswith('https:'):
                    ids.add('wikidata:' + v.removeprefix('https://www.wikidata.org/entity/'))
        if ids:
            for item in ids:
                output_row = {'supported_id': item, 'openalex_id': openalex_id}
                yield output_row

    @staticmethod
    def get_publisher_ids(inp_entity: dict) -> Generator[dict, None, None]:
        """
        Extracts the IDs of an OpenAlex Publisher entity.
        :param inp_entity: the dict object representing the OpenAlex Publisher entity
        :return: a generator of dicts, as many as the external PIDs found for the resource, each with two fields:
        one storing the external PID (one among the ones supported by OC Meta) and the other storing the OpenAlex ID.
        None if the entity has no external PIDs supported by OC Meta.
        """
        ids = set()
        openalex_id = inp_entity['id'].removeprefix('https://openalex.org/')
        for k, v in inp_entity['ids'].items():
            if k == 'ror':
                ids.add('ror:' + v.removeprefix('https://ror.org/'))
            elif k == 'wikidata':
                if v.startswith('http:'):
                    ids.add('wikidata:' + v.removeprefix('http://www.wikidata.org/entity/'))
                elif v.startswith('https:'):
                    ids.add('wikidata:' + v.removeprefix('https://www.wikidata.org/entity/'))
        if ids:
            for item in ids:
                output_row = {'supported_id': item, 'openalex_id': openalex_id}
                yield output_row

    @staticmethod
    def get_funder_ids(inp_entity: dict) -> Generator[dict, None, None]:
        """
        Extracts the IDs of an OpenAlex Funder entity.
        :param inp_entity: the dict object representing the OpenAlex Funder entity
        :return: a generator of dicts, as many as the external PIDs found for the resource, each with two fields:
        one storing the external PID (one among the ones supported by OC Meta) and the other storing the OpenAlex ID.
        None if the entity has no external PIDs supported by OC Meta.
        """
        ids = set()
        openalex_id = inp_entity['id'].removeprefix('https://openalex.org/')
        for k, v in inp_entity['ids'].items():
            if k == 'ror':
                ids.add('ror:' + v.removeprefix('https://ror.org/'))
            elif k == 'wikidata':
                if v.startswith('http:'):
                    ids.add('wikidata:' + v.removeprefix('http://www.wikidata.org/entity/'))
                elif v.startswith('https:'):
                    ids.add('wikidata:' + v.removeprefix('https://www.wikidata.org/entity/'))
        if ids:
            for item in ids:
                output_row = {'supported_id': item, 'openalex_id': openalex_id}
                yield output_row

    def create_openalex_ids_table(self, inp_dir: str, out_dir: str, entity_type: Literal[
        'work', 'source', 'author', 'publisher', 'institution', 'funder']) -> None:

        if entity_type.lower().strip() == 'work':
            process_line = self.get_work_ids
        elif entity_type.lower().strip() == 'source':
            process_line = self.get_source_ids
        elif entity_type.lower().strip() == 'author':
            process_line = self.get_author_ids
        elif entity_type.lower().strip() == 'publisher':
            process_line = self.get_publisher_ids
        elif entity_type.lower().strip() == 'institution':
            process_line = self.get_institution_ids
        elif entity_type.lower().strip() == 'funder':
            process_line = self.get_funder_ids
        else:
            raise ValueError("ValueError: the entity type '{}' is not supported.".format(entity_type))

        logging.info(f'Processing input folder {inp_dir} for OpenAlex table creation')
        process_start_time = time.time()
        inp_subdirs = [name for name in listdir(inp_dir) if isdir(join(inp_dir, name))]
        for snapshot_folder_name in inp_subdirs:
            logging.info(f'Processing snapshot directory {snapshot_folder_name}')
            snapshot_folder_path = join(inp_dir, snapshot_folder_name)
            for compressed_jsonl_name in tqdm(listdir(snapshot_folder_path)):
                inp_path = join(snapshot_folder_path, compressed_jsonl_name)
                logging.info(f'Processing {compressed_jsonl_name}')
                file_start_time = time.time()
                out_folder_path = join(out_dir, snapshot_folder_name)
                makedirs(out_folder_path, exist_ok=True)
                out_filename = 'reduced_' + splitext(basename(compressed_jsonl_name))[0] + '.csv'
                out_filepath = join(out_folder_path, out_filename)
                with gzip.open(inp_path, 'r') as inp_jsonl, open(out_filepath, 'w', newline='',
                                                                 encoding='utf-8') as out_csv:
                    writer = DictWriter(out_csv, dialect='unix', fieldnames=['supported_id', 'openalex_id'])
                    writer.writeheader()
                    for line in inp_jsonl:
                        line = json.loads(line)
                        out_rows = process_line(
                            line)  # returns a generator of dicts, each corresponding to a row in the output csv
                        if out_rows:
                            for r in out_rows:
                                writer.writerow(r)
                logging.info(f'Processing {compressed_jsonl_name} took {time.time() - file_start_time} seconds')
        logging.info(
            f'Processing input folder {inp_dir} for OpenAlex table creation took {(time.time() - process_start_time) / 60} minutes')

    @staticmethod
    def create_id_db_table(inp_dir: str, db_path: str,
                           id_type: Literal['doi', 'pmid', 'pmcid', 'wikidata', 'issn'],
                           entity_type: Literal['work', 'source']) -> None:
        """
        Creates and indexes a database table containing the IDs of the specified type for the specified entity type.
        Creates a table (if it doesn't already exist) and names it with the name of the ID type passed as a parameter
        (one among "doi", "pmid", "pmcid, etc."). Then, for each csv file in the input directory, the file is converted
        to a pandas DataFrame and then appended to the database table. The DataFrames, each of which corresponds to
        a single file,are appended one at a time.
            :param inp_dir: the folder containing the csv files to be processed (the preliminary tables of the form: supported_id, openalex_id)
            :param db_path: the path to the database file
            :param id_type: the type of ID to be processed (one among "doi", "pmid", "pmcid", "wikidata", "issn")
            :param entity_type:
            :return:
        """

        table_name = f'{entity_type.capitalize()}s{id_type.capitalize()}'
        start_time = time.time()
        with sql.connect(db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if cursor.fetchone():
                raise ValueError(f"Table {table_name} already exists")

            for root, dirs, files in walk(inp_dir):
                for file in tqdm(files):
                    if file.endswith('.csv'):
                        csv_path = join(root, file)
                        file_df = pd.read_csv(csv_path)  # Read the CSV file into a DataFrame

                        # Select only the rows with the ID type specified as a parameter and create a new DataFrame
                        id_df = file_df[file_df['supported_id'].str.startswith(id_type)]

                        # Append the DataFrame's rows to the existing table in the database
                        id_df.to_sql(table_name, conn, if_exists='append', index=False)

            print('Creating index...')
            create_idx_query = "CREATE INDEX idx_{} ON {}(supported_id);".format(table_name.lower(), table_name)
            cursor.execute(create_idx_query)
            conn.commit()

        print(
            f"Creating and indexing the database table for {id_type.upper()}s took {(time.time() - start_time) / 60} minutes")


class Mapping:
    def __init__(self):
        pass

    @staticmethod
    def map_omid_openalex_ids(inp_dir:str, db_path:str, out_dir:str, multi_mapped_dir:str, type_field=True, all_rows=True) -> None:
        """
        Creates a mapping table between OMIDs and OpenAlex IDs.
        :param inp_dir: path to the folder containing the reduced OC Meta tables
        :param db_path: path to the database file
        :param out_dir: path to the folder where the mapping table should be saved
        :param type_field: if True, the mapping table will contain the type of the entity (use for IDs from the OC Meta
            'id' field) otherwise it will not (use for IDs from the OC Meta 'venue' field)
        :return: None
        """
        makedirs(multi_mapped_dir, exist_ok=True)
        makedirs(out_dir, exist_ok=True)
        multi_mapped_filepath = join(multi_mapped_dir, 'multi_mapped_omids.csv')
        with sql.connect(db_path) as conn, open(multi_mapped_filepath, 'w', newline='') as multi_mapped:
            if type_field:
                multi_mapped_writer = DictWriter(multi_mapped, dialect='unix', fieldnames=['omid', 'openalex_id', 'type'])
            else:
                multi_mapped_writer = DictWriter(multi_mapped, dialect='unix', fieldnames=['omid', 'openalex_id'])
            multi_mapped_writer.writeheader()
            cursor = conn.cursor()
            for root, dirs, files in walk(inp_dir):
                for file_name in tqdm(files):
                    if file_name.endswith('.csv'):
                        with open(join(root, file_name), 'r', encoding='utf-8') as inp_file, open(join(out_dir, file_name),
                                                                                                  'w',
                                                                                                  encoding='utf-8',
                                                                                                  newline='') as out_file:


                            reader = DictReader(inp_file)
                            if type_field:
                                writer = DictWriter(out_file, dialect='unix', fieldnames=['omid', 'openalex_id', 'type'])
                            else:
                                writer = DictWriter(out_file, dialect='unix', fieldnames=['omid', 'openalex_id'])
                            writer.writeheader()

                            for row in reader:
                                entity_ids: list = row['ids'].split()
                                oa_ids = set()

                                if any(x.startswith('openalex:') for x in entity_ids) and all_rows is False:
                                    continue  # skip to next row

                                # if there is an ISSN for the entity in OC Meta, look only for ISSN in OpenAlex
                                if any(x.startswith('issn:') for x in entity_ids):
                                    for pid in entity_ids:
                                        if pid.startswith('issn'):
                                            query = "SELECT openalex_id FROM SourcesIssn WHERE supported_id=?"
                                            cursor.execute(query, (pid,))
                                            for res in cursor.fetchall():
                                                oa_ids.add(res[0])
                                        else:
                                            continue

                                # if there is a DOI for the entity in OC Meta and no ISSNs, look only for DOI in OpenAlex
                                elif any(x.startswith('doi:') for x in entity_ids):
                                    for pid in entity_ids:
                                        if pid.startswith('doi:'):
                                            query = "SELECT openalex_id FROM WorksDoi WHERE supported_id=?"
                                            cursor.execute(query, (pid,))
                                            for res in cursor.fetchall():
                                                oa_ids.add(res[0])
                                        else:
                                            continue

                                # if there is no ISSN nor DOI for the entity in OC Meta, look for all the other IDs in OpenAlex
                                else:
                                    for pid in entity_ids:
                                        if pid.startswith('pmid:'):
                                            curr_lookup_table = 'WorksPmid'
                                        elif pid.startswith('pmcid:'):
                                            curr_lookup_table = 'WorksPmcid'
                                        elif pid.startswith('wikidata:'):
                                            curr_lookup_table = 'SourcesWikidata'
                                        else:
                                            # only PIDs for bibliographic resources supported by both OC Meta and OpenAlex are considered
                                            continue
                                        query = "SELECT openalex_id FROM {} WHERE supported_id=?".format(curr_lookup_table)
                                        cursor.execute(query, (pid,))
                                        for res in cursor.fetchall():
                                            oa_ids.add(res[0])

                                if oa_ids:
                                    if type_field:
                                        out_row = {'omid': row['omid'], 'openalex_id': ' '.join(oa_ids),
                                                   'type': row['type']}
                                    else:
                                        out_row = {'omid': row['omid'], 'openalex_id': ' '.join(oa_ids)}

                                    if len(oa_ids) > 1:
                                        # multi-mapped OMID
                                        ...
                                        multi_mapped_writer.writerow(out_row)
                                    else:
                                        writer.writerow(out_row)
