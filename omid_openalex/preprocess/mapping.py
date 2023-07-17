import sqlite3 as sql
from csv import DictReader, DictWriter
from tqdm import tqdm
from os.path import join
import os
import time
import pandas as pd
from typing import Literal

def create_id_db_table(inp_dir:str, db_path:str, id_type:Literal['doi', 'pmid', 'pmcid', 'wikidata', 'issn'], entity_type: Literal['work', 'source'])-> None:
    """
    Creates a table in the database containing the IDs of the specified type for the specified entity type.
    Creates a table (if it doesn't already exist) and names it with the name of the ID type passed as a parameter
    (one among "doi", "pmid", "pmcid, etc."). Then, for each csv file in the input directory, the file is converted
    to a pandas DataFrame and then appended to the database table. The DataFrames, each of which corresponds to
    a single file,are appended one at a time.
        :param inp_dir: the folder containing the csv files to be processed (the preliminary tables of the form: supported_id, openalex_id)
        :param db_path: the path to the database file
        :param id_type: the type of ID to be processed (one among "doi", "pmid" and "pmcid")
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

        for root, dirs, files in os.walk(inp_dir):
            for file in tqdm(files):
                if file.endswith('.csv'):
                    csv_path = os.path.join(root, file)
                    file_df = pd.read_csv(csv_path)  # Read the CSV file into a DataFrame

                    # Select only the rows with the ID type specified as a parameter and create a new DataFrame
                    id_df = file_df[file_df['supported_id'].str.startswith(id_type)]

                    # Append the DataFrame's rows to the existing table in the database
                    id_df.to_sql(table_name, conn, if_exists='append', index=False)

    print(f"Creating the database table for {id_type.upper()}s took {(time.time()-start_time)/60} minutes")

def map_omid_openalex_ids(inp_dir:str, db_path:str, out_dir: str) -> None:
    """
    Creates a mapping table between OMIDs and OpenAlex IDs.
    :param inp_dir: path to the folder containing the reduced OC Meta tables
    :param out_dir: path to the folder where the mapping table should be saved
    :return: None
    """
    os.makedirs(out_dir, exist_ok=True)
    with sql.connect(db_path) as conn:
        cursor = conn.cursor()
        for root, dirs, files in os.walk(inp_dir):
            for file_name in tqdm(files):
                with open(join(root, file_name), 'r', encoding='utf-8') as inp_file, open(join(out_dir, file_name), 'w', encoding='utf-8', newline='') as out_file:
                    reader = DictReader(inp_file)
                    writer = DictWriter(out_file, dialect='unix', fieldnames=['omid', 'openalex_id', 'type'])
                    writer.writeheader()

                    for row in reader:
                        entity_type = row['type']
                        entity_ids: list = row['ids'].split()
                        oa_ids = set()

                        # if there is an ISSN for the entity in OC Meta, look only for ISSN in OpenAlex
                        if any(x.startswith('issn:') for x in entity_ids):
                            for id in entity_ids:
                                if id.startswith('issn'):
                                    query = "SELECT openalex_id FROM SourcesIssn WHERE supported_id=?"
                                    cursor.execute(query, (id,))
                                    for res in cursor.fetchall():
                                        oa_ids.add(res[0])
                                else:
                                    continue

                        # if there is a DOI for the entity in OC Meta and no ISSNs, look only for DOI in OpenAlex
                        elif any(x.startswith('doi:') for x in entity_ids):
                            for id in entity_ids:
                                if id.startswith('doi:'):
                                    query = "SELECT openalex_id FROM WorksDoi WHERE supported_id=?"
                                    cursor.execute(query, (id,))
                                    for res in cursor.fetchall():
                                        oa_ids.add(res[0])
                                else:
                                    continue

                        # if there is no ISSN nor DOI for the entity in OC Meta, look for all the other IDs in OpenAlex
                        else:
                            for id in entity_ids:
                                if id.startswith('pmid:'):
                                    curr_lookup_table = 'WorksPmid'
                                elif id.startswith('pmcid:'):
                                    curr_lookup_table = 'WorksPmcid'
                                elif id.startswith('wikidata:'):
                                    curr_lookup_table = 'SourcesWikidata'
                                else:
                                    # only PIDs for bibliographic resources supported by both OC Meta and OpenAlex are considered
                                    continue
                                query = "SELECT openalex_id FROM {} WHERE supported_id=?".format(curr_lookup_table)
                                cursor.execute(query, (id,))
                                for res in cursor.fetchall():
                                    oa_ids.add(res[0])

                        if oa_ids:
                            out_row = {'omid': row['omid'], 'openalex_id': ' '.join(oa_ids), 'type': row['type']}
                            writer.writerow(out_row)

if __name__ == '__main__':
    ## Create all the database tables for the different ID types and entity types, for all the entities in the OpenAlex tables
    ## Uncomment the lines below to create the database tables
    # create_id_db_table('D:/reduced_meta_tables', 'oa_ids_tables.db', 'doi', 'work')
    # create_id_db_table('D:/reduced_meta_tables', 'oa_ids_tables.db', 'pmid', 'work')
    # create_id_db_table('D:/reduced_meta_tables', 'oa_ids_tables.db', 'pmcid', 'work')
    # create_id_db_table('D:/reduced_meta_tables', 'oa_ids_tables.db', 'issn', 'source')
    # create_id_db_table('D:/reduced_meta_tables', 'oa_ids_tables.db', 'wikidata', 'source')
    conn = sql.connect('oa_ids_tables.db')
    cursor = conn.cursor()
    tables_query = "SELECT name FROM sqlite_master WHERE type = 'table';"
    cursor.execute(tables_query)
    tables = [t[0] for t in cursor.fetchall()]
    for tbl in tables:
        idxs_query = "SELECT name, tbl_name FROM sqlite_master WHERE type = 'index' AND tbl_name = ?;"
        cursor.execute(idxs_query, (tbl,))
        res = cursor.fetchall()
        if not res:
            print("No indexes found on table {}. Creating indexes for table {} on 'supported_id' field...".format(tbl, tbl))
            create_idx_query = "CREATE INDEX idx_{} ON {}(supported_id);".format(tbl.lower(), tbl)
            cursor.execute(create_idx_query)
            print("Index created.")
    # Create the mapping table between OMIDs and OpenAlex IDs
    start_time = time.time()
    # map_omid_openalex_ids('E:/reduced_meta_tables', 'oa_ids_tables.db', 'E:/test_no_multi_map')
    print("Creating OMID-OpenAlexID map took: {} hours".format((time.time() - start_time)/3600))
    cursor.close()
    conn.close()
