from os.path import join, abspath, splitext, basename, exists, isdir, isfile
from os import listdir, makedirs
import csv
from io import TextIOWrapper
from zipfile import ZipFile
from typing import Generator, Literal, Union, List, Dict, Callable
from tqdm import tqdm
import logging
import time
from datetime import datetime
import gzip
import json
import re


def reduce_oa_work_row(inp_entity: dict) -> Generator[dict, None, None]:
    output_row = dict()
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


def reduce_oa_source_row(inp_entity: dict) -> Generator[dict, None, None]:
    ids = set()
    openalex_id = inp_entity['id'].removeprefix('https://openalex.org/')
    for k, v in inp_entity['ids'].items():
        if k == 'issn':  # ISSNs are stored in a list, not in a string like other ID types!
            ids.update(['issn:' + i for i in v])  # ISSNs are NOT recorded as URIs, so there is no prefix to remove
        elif k == 'wikidata':
            if v.startswith('http://www.wikidata.org/entity/'):
                ids.add('wikidata:' + v.removeprefix('http://www.wikidata.org/entity/'))
            elif v.startswith('https://www.wikidata.org/entity/'):
                ids.add('wikidata:' + v.removeprefix('https://www.wikidata.org/entity/'))

    issn = inp_entity['ids'].get('issn')
    issn_l = inp_entity['ids'].get('issn_l')

    # if there are no ISSNs for this entity, but there's an ISSN-L,
    # add the ISSN-L as ISSN (i.e. with the 'issn:' prefix).
    # This is done because OC Meta does not manage ISSN-Ls.
    # todo: isn't this a bit dangerous? remember to double check! Or consider extending Meta to manage ISSN-Ls too...
    if not issn and issn_l:
        ids.add('issn:' + issn_l)
    if ids:
        for item in ids:
            output_row = {'supported_id': item, 'openalex_id': openalex_id}
            yield output_row


def get_entity_ids_and_type(row: dict) -> Union[dict, None]:
    output_row = dict()
    output_row['omid'] = ''
    output_row['ids'] = []
    output_row['type'] = row['type']

    # get resource's omid and other IDs
    for id in row['id'].split():
        if id.startswith('meta:'):  # todo: change 'meta' to 'omid'
            output_row['omid'] = id
        else:  # i.e., if prefix is one of: 'doi:','pmid:','pmcid:','issn:','isbn:','wikidata:'
            output_row['ids'].append(id)
    #  todo: add support for other IDs (e.g., arxiv, isbn, issn, etc.)?? First see how the Oc MEta dump is
    #   structured: you can consider creating one single table for OC Meta as a first step, and then separate
    #   single resources and venues in the processing phase (according to the 'type'), since single resources
    #   and venues are placed in the same dump in OC Meta, but in different directories in OpenAlex.

    if output_row['ids']:  # if the resource has at least one external ID that is supported by OpenAlex
        output_row['ids'] = ' '.join(output_row['ids'])
        return output_row


def get_venue_ids(row: dict) -> Union[dict, None]:
    """
    Extracts the IDs in the value of the 'venue' field from a row of the OC Meta dump.
        :param row: dict representing a row of the OC Meta dump
        :return: dict with the OMID and the PIDs of the entity in the venue field
    """
    output_row = dict()
    output_row['omid'] = ''
    output_row['ids'] = []
    # entity_type = row['type']
    # todo: maybe the value of type (corresponding to the type of the row entity, not the venue)
    #  is useful for determining which IDs to consider?? E.g. if type is 'journal article', then the venue should be
    #   a journal, so we should consider only journal IDs (ISSN) and not book IDs (DOIs).

    # get resource's omid and other IDs
    v = row['venue']
    venue_ids = v[v.index('[') + 1:v.index(']')].strip().split() # TODO: NOTE: THIS ASSUMES THAT THE VENUE FIELD IS ALWAYS IN THE SAME FORMAT, IE ALWAYS SQUARE BRACKETS AND ALWAYS ONE SINGLE VENUE!
    for id in venue_ids:
        if id.startswith('meta:'):  # todo: change 'meta' to 'omid'
            output_row['omid'] = id
        else:  # i.e., if prefix is one of: 'doi:','pmid:','pmcid:','issn:','isbn:','wikidata:'
            output_row['ids'].append(id)

    if output_row['ids']:  # if the entity in venue has at least one external ID that is supported by OpenAlex
        output_row['ids'] = ' '.join(output_row['ids'])
        return output_row


def get_ra_ids(row: dict, field: Literal['author', 'publisher', 'editor']) -> Generator[dict, None, None]:
    output_row = dict()

    for ra_entity in row[field].split('; '):
        output_row['omid'] = ''
        output_row['ids'] = []
        try:
            start = ra_entity.index('[') + 1
            end = ra_entity.index(']')
            for ra_id in ra_entity[start:end].strip().split():
                if ra_id.startswith('meta:'):  # todo: change 'meta' to 'omid'
                    output_row['omid'] = ra_id
                else:
                    output_row['ids'].append(ra_id)
            if output_row['ids']:
                output_row['ids'] = ' '.join(output_row['ids'])
                yield output_row
        except ValueError:
            continue

def preprocess_meta_tables(inp_dir: str, out_dir: str) -> None:
    """
    Preprocesses the OC Meta tables to create reduced tables with essential metadata. For each entity represented in a
    row in the original table, the reduced output table contains the OMID ('omid' field) and the PIDs ('ids' field) of
    the entity, as well as the type of resource ('type' field).
    For the entity in the 'venue' field of a row in the original table, the reduced output table only contains the OMID
    and the PIDs of the entity ('omid' and 'ids' fields).
        :param inp_dir: the directory where the OC Meta tables are stored
        :param out_dir: the directory where the reduced tables will be written, named with the same name as the original
         but prefixed with 'primary_ents_' if the table concerns the entity whose IDs are stored in the 'id' field of
         the original table, and prefixed with 'venues_' if the table concerns the entity whose IDs are stored in the
         'venue' field of the original table.
        :return: None (writes the reduced tables to disk)
    """
    csv.field_size_limit(131072 * 4)  # quadruple the default limit for csv field size
    makedirs(out_dir, exist_ok=True)
    logging.info(f'Processing input folder {inp_dir} for reduced OC Meta table creation')
    process_start_time = time.time()
    for snapshot_folder_name in listdir(abspath(inp_dir)):
        archive_path = join(abspath(inp_dir), snapshot_folder_name)
        with ZipFile(archive_path) as archive:
            files_pbar = tqdm(total=len(archive.namelist()), desc='Processing files in archive', unit='file',
                              disable=False)
            for csv_name in archive.namelist():
                logging.info(f'Processing {csv_name}')
                file_start_time = time.time()
                files_pbar.set_description(f'Processing {csv_name}')
                files_pbar.update()
                primary_ents_out_filename = 'primary_ents_' + basename(csv_name)
                venues_out_filename = 'venues_' + basename(csv_name)
                primary_ents_out_path = join(abspath(out_dir), primary_ents_out_filename)
                venues_out_path = join(abspath(out_dir), venues_out_filename)
                with archive.open(csv_name, 'r') as csv_file, open(primary_ents_out_path, 'w', newline='', encoding='utf-8') as primary_ents_out_file, open(venues_out_path, 'w', newline='', encoding='utf-8') as venues_out_file:
                    primary_ents_writer = csv.DictWriter(primary_ents_out_file, dialect='unix', fieldnames=['omid', 'ids', 'type'])
                    venues_writer = csv.DictWriter(venues_out_file, dialect='unix', fieldnames=['omid', 'ids'])
                    primary_ents_writer.writeheader()
                    venues_writer.writeheader()
                    try:
                        reader = list(csv.DictReader(TextIOWrapper(csv_file, encoding='utf-8'),
                                                     dialect='unix'))  # todo: check if this is the best way to do it: maybe leave it as a generator?
                        for row in reader:
                            primary_entity_out_row = get_entity_ids_and_type(row)
                            venue_out_row = get_venue_ids(row)
                            if primary_entity_out_row:
                                primary_ents_writer.writerow(primary_entity_out_row)
                            if venue_out_row:
                                venues_writer.writerow(venue_out_row)

                        logging.info(f'Processing {csv_name} took {time.time() - file_start_time} seconds')
                    except csv.Error as e:
                        logging.error(f'Error while processing {csv_name}: {e}')

            files_pbar.close()
            logging.info(
                f'Processing input folder {inp_dir} for reduced OC Meta table creation took {time.time() - process_start_time} seconds')


def create_oa_reduced_table(inp_dir: str, out_dir: str, entity_type: Literal['work', 'source']) -> None:
    # Literal['work', 'source', 'author', 'publisher', 'institution', 'funder']

    if entity_type.lower().strip() == 'work':
        process_line = reduce_oa_work_row
    elif entity_type.lower().strip() == 'source':
        process_line = reduce_oa_source_row
    # create and add functions for processing lines with other types of OA entities
    else:
        raise ValueError("ValueError: the entity type '{}' is not supported.".format(entity_type))

    logging.info(f'Processing input folder {inp_dir} for OpenAlex table creation')
    process_start_time = time.time()
    inp_subdirs = [name for name in listdir(inp_dir) if isdir(join(inp_dir, name))]
    for snapshot_folder_name in inp_subdirs:
        logging.info(f'Processing snapshot directory {snapshot_folder_name}')
        snapshot_folder_path = join(abspath(inp_dir), snapshot_folder_name)
        for compressed_jsonl_name in tqdm(listdir(snapshot_folder_path)):
            inp_path = join(snapshot_folder_path, compressed_jsonl_name)
            logging.info(f'Processing {compressed_jsonl_name}')
            file_start_time = time.time()
            out_folder_path = join(abspath(out_dir), snapshot_folder_name)
            makedirs(out_folder_path, exist_ok=True)
            out_filename = 'reduced_' + splitext(basename(compressed_jsonl_name))[0] + '.csv'
            out_filepath = join(out_folder_path, out_filename)
            with gzip.open(abspath(inp_path), 'r') as inp_jsonl, open(out_filepath, 'w', newline='',
                                                                      encoding='utf-8') as out_csv:
                writer = csv.DictWriter(out_csv, dialect='unix', fieldnames=['supported_id', 'openalex_id'])
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


if __name__ == '__main__':
    META_INPUT_FOLDER_PATH = join('D:/oc_meta_dump')
    META_OUTPUT_FOLDER_PATH = join('D:/reduced_meta_tables')
    OA_WORK_INPUT_FOLDER_PATH = join('D:/openalex_dump/data/works')
    OA_WORK_OUTPUT_FOLDER_PATH = join('D:/oa_work_tables')

    # preprocess_meta_tables(META_INPUT_FOLDER_PATH, META_OUTPUT_FOLDER_PATH, reduce_oa_work_row)
    logging.basicConfig(filename=f'../logs/create_mapping_tables{(str(datetime.date(datetime.now())))}.log',
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    # create_oa_reduced_table(OA_WORK_INPUT_FOLDER_PATH, OA_WORK_OUTPUT_FOLDER_PATH, reduce_oa_work_row)
