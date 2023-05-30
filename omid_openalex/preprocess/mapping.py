import sqlite3 as sql
from csv import DictReader, DictWriter
from tqdm import tqdm
from os.path import join
import os
import time

def create_omid_openalex_mapping(inp_dir:str, db_path:str, out_dir: str) -> None:
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
                        oa_ids = set()
                        for id in row['ids'].split():
                            curr_id_type = id.split(':')[0]
                            if curr_id_type == 'doi':
                                curr_lookup_table = 'WorksDoi'
                            elif curr_id_type == 'pmid':
                                curr_lookup_table = 'WorksPmid'
                            elif curr_id_type == 'pmcid':
                                curr_lookup_table = 'WorksPmcid'
                            else:
                                # this function only supports the mapping of Works IDs (doi, pmid, pmcid) to OMIDs. There
                                # are no other ID types in the reduced OpenAlex Works tables, so we can safely skip
                                # processing them, although they have been left in the reduced OC Meta tables.
                                continue
                            query = "SELECT openalex_id FROM {} WHERE supported_id=?".format(curr_lookup_table)
                            cursor.execute(query, (id,))
                            for res in cursor.fetchall():
                                oa_ids.add(res[0])
                        if oa_ids:
                            out_row = {'omid': row['omid'], 'openalex_id': ' '.join(oa_ids), 'type': row['type']}
                            writer.writerow(out_row)

if __name__ == '__main__':
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
            create_idx_query = "CREATE INDEX idx_{}_works ON {}(supported_id);".format(tbl.split('Works')[1].lower(), tbl)
            cursor.execute(create_idx_query)
            print("Index created.")
    start_time = time.time()
    create_omid_openalex_mapping('D:/reduced_meta_tables', 'oa_ids_tables.db', 'D:/omid_openalex_mapping')
    print("Creating OMID-OpenAlexID map took: {} minutes".format((time.time() - start_time)/60))
    conn.close()