from omid_openalex.preprocess.mapping import create_id_db_table, map_omid_openalex_ids
from omid_openalex.preprocess.preprocess_dump import preprocess_meta_tables, create_oa_reduced_table
from os.path import join
import time
import logging

NEW_META_DUMP = 'E:/meta_latest_dump/'
NEW_OA_DUMP = 'E:/openalex_dump_latest/data'
NEW_OUT_DIR = 'E:/new_tables'
NEW_DB_PATH = 'new_db.db'

if __name__ == '__main__':
    logging.basicConfig(filename='process.log', level=logging.DEBUG)

    works_inp_dir = join(NEW_OA_DUMP, 'works')
    sources_inp_dir = join(NEW_OA_DUMP, 'sources')
    works_out_dir = join(NEW_OUT_DIR, 'works')
    sources_out_dir = join(NEW_OUT_DIR, 'sources')
    meta_out_dir = join(NEW_OUT_DIR, 'new_meta_tables')

    # CREATE PRELIMINARY TABLES
    # print('Creating table for OA Works...')
    # start = time.time()
    # create_oa_reduced_table(inp_dir=works_inp_dir, out_dir=works_out_dir, entity_type='work')
    # print('Works table created in', (time.time() - start)/3600, 'hours')
    # logging.info(f'Works table created in {(time.time() - start)/3600} hours')
    # print('Creating table for OA Sources...')
    # start = time.time()
    # create_oa_reduced_table(inp_dir=sources_inp_dir, out_dir=sources_out_dir, entity_type='source')
    # print('Sources table created in', (time.time() - start)/3600, 'hours')
    # logging.info(f'Sources table created in {(time.time() - start)/3600} hours')
    print('Creating Meta Tables...')
    start = time.time()
    preprocess_meta_tables(inp_dir=NEW_META_DUMP, out_dir=meta_out_dir)
    print('Meta tables created in', (time.time() - start)/3600, 'hours')
    logging.info(f'Meta tables created in {(time.time() - start)/3600} hours')

    ## CREATE DATABASE TABLES
    # print('Create OA ID tables...')
    # print('Creating DB table for DOIs')
    # start = time.time()
    # create_id_db_table(works_out_dir, NEW_DB_PATH, 'doi', 'work')
    # print('Table for DOIs created in', (time.time() - start)/60, 'minutes')
    # logging.info(f'Table for DOIs created in {(time.time() - start)/60} minutes')
    # start = time.time()
    # print('Creating DB table for PMIDs')
    # create_id_db_table(works_out_dir, NEW_DB_PATH, 'pmid', 'work')
    # print('Table for PMIDs created in', (time.time() - start)/60, 'minutes')
    # logging.info(f'Table for PMIDs created in {(time.time() - start)/60} minutes')
    # start = time.time()
    # print('Creating DB table for PMCIDs')
    # create_id_db_table(works_out_dir, NEW_DB_PATH, 'pmcid', 'work')
    # print('Table for PMCIDs created in', (time.time() - start)/60, 'minutes')
    # logging.info(f'Table for PMCIDs created in {(time.time() - start)/60} minutes')
    # start = time.time()
    # print('Creating DB table for Wikidata IDs')
    # create_id_db_table(sources_out_dir, NEW_DB_PATH, 'wikidata', 'source')
    # print('Table for Wikidata IDs created in', (time.time() - start)/60, 'minutes')
    # logging.info(f'Table for Wikidata IDs created in {(time.time() - start)/60} minutes')
    # start = time.time()
    # print('Creating DB table for ISSNs')
    # create_id_db_table(sources_out_dir, NEW_DB_PATH, 'issn', 'source')
    # print('Table for ISSNs created in', (time.time() - start)/60, 'minutes')
    # logging.info(f'Table for ISSNs created in {(time.time() - start)/60} minutes')