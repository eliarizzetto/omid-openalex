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
import csv
from typing import Union
from collections import Counter, defaultdict

def analyse_multi_mapped_omids(file_path:str, res_type='journal article'):
    """
    Analyse the multi-mapped OMIDs and returns a tuple of three lists of dictionaries, where each list contains rows where either only OpenAlex Works ID, or only OpenAlex Sources ID, or both Works and Sources ID are present. The lists are filtered by the type of bibliographic resource (e.g. journal article, book, etc.). The function also prints basic statistics about the filtered data.
    :param file_path: the .csv file storing the table rows containing multi-mapped OMIDs
    :param res_type: the type of bibliographic resource to filter the data by
    :return: tupleof thre lists by composition of OAIDs (works, sources, or both)
    """
    df = pd.read_csv(file_path, sep=',', header=None, names=['omid', 'openalex_id', 'type'])


    filter_type_df = df[df['type'] == res_type]
    filter_type_df['openalex_id'] = filter_type_df['openalex_id'].str.split(' ')

    all_works = []
    all_sources = []
    works_and_sources = []

    for index, row in filter_type_df.iterrows():
        starts_with_w = all(item.startswith('W') for item in row['openalex_id'])
        starts_with_s = all(item.startswith('S') for item in row['openalex_id'])

        if starts_with_w and not starts_with_s:
            all_works.append(row.tolist())
        elif starts_with_s and not starts_with_w:
            all_sources.append(row.tolist())
        else:
            works_and_sources.append(row.tolist())

    count_works = dict(Counter(len(item[1]) for item in all_works))
    count_sources = dict(Counter(len(item[1]) for item in all_sources))
    count_works_and_sources = dict(Counter(len(item[1]) for item in works_and_sources))

    print(f'Total number of OMIDs of {res_type} multi-mapped: {len(all_works) + len(all_sources) + len(works_and_sources)}', end='\n\n')

    print(f'Number of OMIDs of {res_type} multi-mapped to Work IDs only: {len(all_works)}. The following illustrates how these are distributed over the number of OAIDs each OMID is mapped to: {dict(sorted(count_works.items()))}', end='\n\n')
    print(f'Number of OMIDs of {res_type} multi-mapped to Source IDs only: {len(all_sources)}. The following illustrates how these are distributed over the number of OAIDs each OMID is mapped to: {dict(sorted(count_sources.items()))}', end='\n\n')
    print(f'Number of OMIDs of {res_type} multi-mapped to both Work and Source IDs: {len(works_and_sources)}. The following illustrates how these are distributed over the number of OAIDs each OMID is mapped to: {dict(sorted(count_works_and_sources.items()))}', end='\n\n')

    return all_works, all_sources, works_and_sources

def add_columns_to_df(df):
    """
    Add to the dataframe the columns for the number of OAIDs for each OMID ('oaid_count' column) and the composition of the OAIDs (works, sources, or both) ('composition' column).
    :param df: the input dataframe, as it is read from the CSV file
    :return: a dataframe with the two additional columns
    """
    # add a column to the dataframe with the number of OAIDs for each OMID
    df['oaid_count'] = df['openalex_id'].apply(lambda x: len(x.split(' ') if len(x.split(' ')) > 1 else ''))

    # add a column to the dataframe with the composition of the OAIDs (works, sources, or both)
    df['composition'] = df['openalex_id'].apply(lambda x: 'works' if all(item.startswith('W') for item in x.split(' ')) else 'sources' if all(item.startswith('S') for item in x.split(' ')) else 'both' if any(item.startswith('W') or item.startswith('S') for item in x.split(' ')) else '')

    # remove extra header row
    df = df[df['composition'] != ''].reset_index(drop=True)
    return df

def prepare_data_for_filtering(filepath:str)->pd.DataFrame:
    """
    Reads the csv file at the specified path and returns a DataFrame with the columns 'omid', 'openalex_id', 'type' and
    'composition'. NaN values on the 'type' field are replaced with 'Unspecified'.
    :param filepath: the path to the csv file to be read (which must be of the form: omid, openalex_id, type)
    :return: a DataFrame with the columns 'omid', 'openalex_id', 'type' and 'composition'
    """
    data = pd.read_csv(filepath, sep=',', header=None, names=['omid', 'openalex_id', 'type'])
    # replace the NaN values in the 'type' column of the primary entities df with 'Unspecified'
    data['type'].fillna('Unspecified', inplace=True)
    # add the 'composition' column to the DataFrame
    data = add_columns_to_df(data)
    return data
def filter_multi_mapped_df(df, res_type: Union[str, None], composition: Union[Literal['works', 'sources', 'both'], None], oaid_count: Union[int, None]=2):
    """
    Filter the dataframe by the type of resource, the composition of the OAIDs, and the number of OAIDs for a single OMID.
    :param df: a DF to which columns 'oaid_count' and 'composition' have been added
    :param res_type:
    :param composition: only one at a time: 'works', 'sources', or 'both'; set at None if you want to get all the three
    :param oaid_count: the exact number of OAIDs for a single OMID
    :return:
    """
    if composition not in ['works', 'sources', 'both', None]:
        raise ValueError('The composition parameter must be one of the following: "works", "sources", "both", or None.')
    if res_type:
        if not oaid_count and not composition:
            filtered_df = df[(df['type'] == res_type)]
        elif not oaid_count and composition:
            filtered_df = df[(df['type'] == res_type) &
                             (df['composition'] == composition)]
        elif oaid_count and not composition:
            filtered_df = df[(df['type'] == res_type) &
                             (df['oaid_count'] == oaid_count)]
        else:
            filtered_df = df[(df['type'] == res_type) &
                             (df['composition'] == composition) &
                             (df['oaid_count'] == oaid_count)]
    else:
        if not oaid_count and not composition:
            warnings.warn('You need to specify at least one of the two parameters: composition or oaid_count. Otherwise, the whole dataframe is returned.', UserWarning)
            filtered_df = df
        elif not oaid_count and composition:
            filtered_df = df[(df['composition'] == composition)]
        elif oaid_count and not composition:
            filtered_df = df[(df['oaid_count'] == oaid_count)]
        else:
            filtered_df = df[(df['composition'] == composition) &
                             (df['oaid_count'] == oaid_count)]

    return filtered_df

def get_ids_uris(df, verbose=True):
    """
    Transform the dataframe into a list of dicts with OMIDs and OAIDs written as (clickable) URLs.
    :param df: any dataframe with columns 'omid' and 'openalex_id'
    :param verbose: if True, return the list of whole rows; if False, return only the 'omid' and 'openalex_id' columns
    :return:
    """
    result = []
    oa_url = 'https://api.openalex.org/'
    oc_url = 'https://opencitations.net/meta/api/v1/metadata/'


    for row in df.to_dict(orient='records'):
        res_row = row
        res_row['omid'] = oc_url + row['omid'].replace('meta:', 'omid:')
        res_row['openalex_id'] = list((map(lambda x: oa_url + x, row['openalex_id'].split())))

        if verbose:
            result.append(res_row)
        else:
            result.append({'omid':res_row['omid'], 'openalex_id':res_row['openalex_id']})
    return result


def get_multi_mapped_omids(inp_dir):

    start_time = time.time()
    res = pd.DataFrame()
    # Iterate over each CSV file
    for filename in tqdm(os.listdir(inp_dir)):
        if filename.endswith('.csv'):
            filepath = os.path.join(inp_dir, filename)
            with open(filepath, 'r') as file:
                reader = csv.DictReader(file, delimiter=',', dialect='unix')

                for row in reader:
                    oaids = set(row['openalex_id'].split())
                    if len(oaids) > 1:
                        row_df = pd.DataFrame([row])
                        res = pd.concat([res, row_df], ignore_index=True)

    print(f"Getting the multi-mapped OMIDs took {(time.time()-start_time)/60} minutes")
    return res


def get_stats(inp_dir:str):
    single_mapped_by_type = {}
    multi_mapped_by_type = {}

    # Iterate over each CSV file
    for filename in tqdm(os.listdir(inp_dir)):
        if filename.endswith('.csv'):
            filepath = os.path.join(inp_dir, filename)
            with open(filepath, 'r') as file:
                reader = csv.DictReader(file, delimiter=',', dialect='unix')
                for row in reader:
                    oaid_count = len(row['openalex_id'].split())
                    type = row['type']

                    if oaid_count == 1 and type:
                        single_mapped_by_type[type] = single_mapped_by_type.get(type, 0) + 1
                    elif oaid_count == 1 and not type:
                        single_mapped_by_type['Unspecified'] = single_mapped_by_type.get('Unspecified', 0) + 1
                    elif oaid_count > 1 and type:
                        multi_mapped_by_type[type] = multi_mapped_by_type.get(type, 0) + 1
                    elif oaid_count > 1 and not type:
                        multi_mapped_by_type['Unspecified'] = multi_mapped_by_type.get('Unspecified', 0) + 1
    # sort the dicts by decreasing value
    single_mapped_by_type = dict(sorted(single_mapped_by_type.items(), key=lambda item: item[1], reverse=True))
    multi_mapped_by_type = dict(sorted(multi_mapped_by_type.items(), key=lambda item: item[1], reverse=True))


    print(f"Total number of single-mapped OMIDs: {sum(single_mapped_by_type.values())}. The distribution by type is: {single_mapped_by_type}.\n"
          f"Total number of multi-mapped OMIDs: {sum(multi_mapped_by_type.values())}. The distribution by type is: {multi_mapped_by_type}.")
    return single_mapped_by_type, multi_mapped_by_type




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


def find_inverted_multi_mapped(inp_dir, out_dir, chunk_size=10000):
    """
    Find inverted multi-mapped entities, i.e. entities for which multiple OMIDs are mapped to the same OpenAlex ID.
    :param inp_dir:
    :param out_dir:
    :param chunk_size: number of rows to read at a time (for memory efficiency)
    :return: None
    """
    openalex_id_dict = {}
    csv_files = [os.path.join(inp_dir, filename) for filename in os.listdir(inp_dir) if filename.endswith('.csv')]
    for file in tqdm(csv_files):
        chunks = pd.read_csv(file, chunksize=chunk_size)
        for chunk in chunks:
            for index, row in chunk.iterrows():
                openalex_ids = row['openalex_id'].split()
                omid = row['omid']
                for openalex_id in openalex_ids:
                    if openalex_id in openalex_id_dict:
                        openalex_id_dict[openalex_id].append(omid)
                    else:
                        openalex_id_dict[openalex_id] = [omid]
    print("Finding entities for which multiple OMIDs are mapped to the same OpenAlex ID...")
    duplicates = {k: v for k, v in openalex_id_dict.items() if len(v) > 1}
    result_rows = []
    for openalex_id, omid_list in duplicates.items():
        for omid in omid_list:
            result_rows.append({"omid": omid, "openalex_id": openalex_id})
    result_df = pd.DataFrame(result_rows)
    print("Number of inverted multi-mapped entities: ", len(result_df))
    result_df.to_csv(os.path.join(out_dir, "inverted_multi_mapped.csv"), index=False)

# if __name__ == '__main__':
#     multi_mapped_omids_path = 'multi_mapped_omids.csv'
#     df = pd.read_csv(multi_mapped_omids_path, sep=',', header=None, names=['omid', 'openalex_id', 'type'])
#     works_list, sources_list = create_query_lists_oaid(df)
#     out_dir = 'D:/multi_mapped_analysis_out'
#     query_oa_dump(works_list, 'D:/openalex_dump/data/works', out_dir, 'works')
#     query_oa_dump(sources_list, 'D:/openalex_dump/data/sources', out_dir, 'sources')