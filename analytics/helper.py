import pandas as pd
import dask.bag as db
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import os
from csv import DictReader
from os.path import join, isdir
from os import listdir, makedirs
from typing import Literal, List
from tqdm import tqdm
import time
import gzip
import json
import jsonlines
import warnings


def query_reduced_meta_tables(query_list: List[str], inp_dir: str, prefix='doi:'):
    """
    Retrieves from the OC Meta dump the full metadata about the bibliographic resource entities identified by the IDs
    in the input query_list . Returns a list of dicts, each dict being the FULL table row for a single bibliographic resource.
    """
    result = []
    query_list = [x.removeprefix(prefix) for x in query_list]

    for root, dirs, files in os.walk(inp_dir):
        for file_name in tqdm(files):
            with open(os.path.join(root, file_name), 'r', encoding='utf-8') as inp_file:
                reader = DictReader(inp_file)
                for idx, row in enumerate(reader):
                    for id in row['ids'].split():
                        if id.removeprefix(prefix) in query_list:
                            print(f'Found ID in row {idx} of file {file_name}: \n\n {row}', end='\n\n')
                            result.append(row)
    return result


def query_oa_dump(query_list: List[str], inp_dir: str, out_dir: str, ent_type:Literal['works', 'sources']) -> None:
    """
    DEPRECATED: use get_full_metadata_for_oaids instead.
    Retrieves from the OpenAlex dump the full metadata about the bibliographic resource entities identified by the IDs
    in the input query_list; stores the output to a CSV file. A reciprocally compatible query_list and inp_dir
    should be specified, since the input folder contains a set of a specific type of OpenAlex entities
    (e.g. Works, Sources, etc.). E.g. if query_list contains W-OAIDs, then inp_dir should be the path to the Works
    folder of the OA dump.
        :param query_list: a list of strings of the form 'W\d+' or 'S\d+' (e.g. 'W12345678' or 'S12345678').
        :param inp_dir: either the path to the Works folder or the Sources folder of the OA dump.
        :param out_dir: the path to the output folder, where the results will be stored in a jsonl file named according to the ent_type.
        :param ent_type: either 'works' or 'sources', used only for naming the output file.
        :return:
    """
    warnings.warn('This function is deprecated. Use get_full_metadata_for_oaids instead: it is faster, since it parallelizes the process using Dask.', DeprecationWarning)
    query_set = set(query_list)
    process_start_time = time.time()
    inp_subdirs = [name for name in listdir(inp_dir) if isdir(join(inp_dir, name))]
    out_filename = f'queried_{ent_type}_out.json'
    makedirs(out_dir, exist_ok=True)
    out_filepath = join(out_dir, out_filename)
    with jsonlines.open(out_filepath, 'a') as writer:  # see if you need encoding='utf-8'
        # out_rows = []
        for snapshot_folder_name in inp_subdirs:
            snapshot_folder_path = join((inp_dir), snapshot_folder_name)
            for compressed_jsonl_name in tqdm(listdir(snapshot_folder_path)):
                inp_path = join(snapshot_folder_path, compressed_jsonl_name)
                out_rows = []
                with gzip.open((inp_path), 'r') as inp_jsonl:
                    print(f'Processing {inp_path}')
                    for line in inp_jsonl:
                        line: dict = json.loads(line)
                        oaid_iri_prefix = 'https://openalex.org/'
                        if line['id'].removeprefix(oaid_iri_prefix) in query_set:
                            out_rows.append(line)
                for l in out_rows:
                    writer.write(l)
                print(f'Finished processing {inp_path}.')

    print(f'Process took {(time.time() - process_start_time) / 3600} hours.')


def get_full_metadata_for_oaids(input_filepath, output_filepath, query_list):
    """
    Takes a list of OAIDs and extracts the full metadata for each of them from the OpenAlex dump, parallelizing the task
    using Dask. A reciprocally compatible query_list and input_filepath (globstring) should be specified, since the input folder
    contains a set of a specific type of OpenAlex entities (e.g. Works, Sources, etc.). E.g. if query_list contains
    W-OAIDs, then input_filepath should be the (globstring) path to the Works folder of the OA dump.
        :param input_filepath: globstring path to the files to retrieve the metadata from (e.g. 'data/works/**/*.gz')
        :param output_filepath: the path to the folder where to store the output, i.e. where the .part files will be created.
        :param query_list: list of OAIDs to extract (either of Works or Sources)
        :return: None
    """

    query_set = set(query_list)
    lines = db.read_text(input_filepath, encoding='utf-8', compression='gzip')
    records = lines.map(json.loads)
    records_to_keep = records.filter(lambda line: line['id'].removeprefix('https://openalex.org/') in query_set)
    with ProgressBar():
        records_to_keep.map(json.dumps).to_textfiles(output_filepath)  # writes the output to multiple .part files


def unify_part_files(inp_path, out_path):
    """
    Takes the .part files created by get_full_metadata_for_oaids and merges them into a single JSON-L file.
    :param inp_path: the globstring path to the .part files
    :param out_path: the path to the single output file
    :return:
    """
    bag = db.read_text(inp_path).map(json.loads)  # Read JSON files and parse each line
    with ProgressBar():
        combined_data = bag.compute()  # Trigger the computation and combine all the data (takes a while)

    with open(out_path, 'w') as f:
        for line in combined_data:
            json.dump(line, f)
            f.write('\n')
    print(f"JSON-L full data for multi-mapped IDs saved to {out_path}")


def reduce_merged_ids_table(input_filepath, output_filepath):
    """
    Writes a single CSV file with the merged IDs in the OpenAlex dump, by reading them compressed files in parallel
    using Dask, and merging the output .part files together.

        :param input_filepath: the globstring of the filepaths storing the merged IDs tables.
        :param output_filepath: the single CSV file output path
        :return: None
    """

    # Read the compressed CSV files into a Dask DataFrame
    df = dd.read_csv(input_filepath, compression='gzip', encoding='utf-8')

    # only take the 'id' and the 'merged_into_id' field of each row
    df = df[['id', 'merge_into_id']]
    # todo: add Progressbar here?
    df.to_csv(output_filepath, index=False, single_file=True)


def create_query_lists_oaid(multi_mapped_df: pd.DataFrame) -> tuple:
    """
    Create a list of OAIDs for each type of resource. The OAIDs are extracted from the 'openalex_id' column of the input DataFrame.
        :param multi_mapped_df: a DataFrame where the 'openalex_id' column contains a list of OAIDs for each row.
        :return: a tuple of two lists, the first containing the OAIDs of the Works and the second containing the OAIDs of the Sources.
    """

    works_list = list({s for l in multi_mapped_df.openalex_id for s in l.split() if s.startswith('W')})
    sources_list = list({s for l in multi_mapped_df.openalex_id for s in l.split() if s.startswith('S')})
    return works_list, sources_list

# if __name__ == '__main__':
#     multi_mapped_omids_path = 'multi_mapped_omids.csv'
#     df = pd.read_csv(multi_mapped_omids_path, sep=',', header=None, names=['omid', 'openalex_id', 'type'])
#     works_list, sources_list = create_query_lists_oaid(df)
#     out_dir = 'D:/multi_mapped_analysis_out'
#     query_oa_dump(works_list, 'D:/openalex_dump/data/works', out_dir, 'works')
#     query_oa_dump(sources_list, 'D:/openalex_dump/data/sources', out_dir, 'sources')