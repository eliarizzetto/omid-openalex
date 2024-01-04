import pandas as pd
import os
from os import makedirs
from typing import Literal, List
import warnings
from typing import Union
from collections import Counter
from omid_openalex.utils import MultiFileWriter, read_csv_tables
from omid_openalex.mapping import OpenAlexProcessor


def analyse_multi_mapped_omids(file_path:str, res_type='journal article'):
    """
    Counts the number of multi-mapped OMIDs of a specific type of bibliographic resource (e.g. journal article, book,
    etc.), groups them by the type(s) of OpenAlex entity they are mapped to (e.g. Work, Source, or both),
    and prints the frequency distribution of the OMIDs over the number of corresponding OpenAlex IDs mapping to a single OMID.
    :param file_path: the .csv file storing the table rows containing multi-mapped OMIDs
    :param res_type: the type of bibliographic resource to consider (default: 'journal article').
    :return: tuple of three lists by composition of OAIDs (works, sources, or both)
    """
    df = pd.read_csv(file_path, sep=',', names=['omid', 'openalex_id', 'type'])

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

    print(f'Total number of multi-mapped OMIDs of BRs of type {res_type}: {len(all_works) + len(all_sources) + len(works_and_sources)}', end='\n\n')

    print(f'Number of OMIDs of {res_type} multi-mapped to Work IDs only: {len(all_works)}. The following illustrates how these are distributed over the number of OAIDs each OMID is mapped to: {dict(sorted(count_works.items()))}', end='\n\n')
    print(f'Number of OMIDs of {res_type} multi-mapped to Source IDs only: {len(all_sources)}. The following illustrates how these are distributed over the number of OAIDs each OMID is mapped to: {dict(sorted(count_sources.items()))}', end='\n\n')
    print(f'Number of OMIDs of {res_type} multi-mapped to both Work and Source IDs: {len(works_and_sources)}. The following illustrates how these are distributed over the number of OAIDs each OMID is mapped to: {dict(sorted(count_works_and_sources.items()))}', end='\n\n')

    return all_works, all_sources, works_and_sources


def add_columns_to_df(df):
    """
    Add to the multi-mapped dataframe the columns for the number of OAIDs for each OMID ('oaid_count' column) and the composition of
     the OAIDs (works, sources, or both) ('composition' column).
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
    :param res_type: the type of bibliographic resource
    :param composition: only one at a time: 'works', 'sources', or 'both'; set at None if you want to get all the three
    :param oaid_count: the exact number of OAIDs for a single OMID
    :return: the filtered dataframe
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


def get_api_url(df, verbose=True):
    """
    Transform the multi-mapped dataframe into a list of dicts, where values correspond to the URLs for retrieving the
    metadata of the resources in OC Meta and OpenAlex via API calls.
    :param df: any dataframe with columns 'omid' and 'openalex_id'
    :param verbose: if True, the whole rows are included in the output lists (e.g. also the 'type' field); if False, return only the 'omid' and 'openalex_id' columns
    :return: list of dictionaries where OMIDs and OpenAlex IDs are replaced by the URLs that retrieve them via API.
    """
    result = []
    oa_url = 'https://api.openalex.org/'
    oc_url = 'https://opencitations.net/meta/api/v1/metadata/'

    for row in df.to_dict(orient='records'):
        res_row = row
        res_row['omid'] = oc_url + row['omid']
        res_row['openalex_id'] = list((map(lambda x: oa_url + x, row['openalex_id'].split())))

        if verbose:
            result.append(res_row)
        else:
            result.append({'omid':res_row['omid'], 'openalex_id':res_row['openalex_id']})
    return result


def get_openalex_full_metadata(query_list: List[str], inp_dir: str, out_dir: str) -> None:
    """
    Retrieves from the OpenAlex dump the full metadata about the bibliographic resources identified by the OpenAlex IDs
    in the input query_list; stores the output to a CSV file. A reciprocally compatible query_list and inp_dir
    should be specified, since the input folder contains a set of a specific type of OpenAlex entities
    (e.g. Works, Sources). E.g. if query_list contains W-OAIDs, then inp_dir should be the path to the Works
    folder of the OA dump.
        :param query_list: a list of strings of the form 'W\d+' or 'S\d+' (e.g. 'W12345678' or 'S12345678').
        :param inp_dir: either the path to the Works folder or the Sources folder of the OA dump.
        :param out_dir: the path to the output folder, where the results will be stored in JSON-L files.
        :return:
    """
    query_set = set(query_list)
    makedirs(out_dir, exist_ok=True)
    oaid_iri_prefix = 'https://openalex.org/'

    with MultiFileWriter(out_dir, file_extension='json') as writer:
        for record in OpenAlexProcessor.read_compressed_openalex_dump(inp_dir):
            if record['id'].removeprefix(oaid_iri_prefix) in query_set:
                writer.write_row(record)


def create_query_lists_oaid(multi_mapped_df: pd.DataFrame) -> tuple:
    """
    Create two lists of OAIDs, one for Works, the other for Sources. The OAIDs are extracted from the 'openalex_id' column of the input DataFrame.
        :param multi_mapped_df: a DataFrame where the 'openalex_id' column contains a string-encoded list of un-prefixed OAIDs (separator: space).
        :return: a tuple of two lists, the first containing the OAIDs of the Works and the second containing the OAIDs of the Sources.
    """

    works_list = list({s for l in multi_mapped_df.openalex_id for s in l.split() if s.startswith('W')})
    sources_list = list({s for l in multi_mapped_df.openalex_id for s in l.split() if s.startswith('S')})
    return works_list, sources_list


def find_inverted_multi_mapped(inp_dir, out_dir):
    """
    Find inverted multi-mapped entities, i.e. entities for which multiple OMIDs are mapped to the same OpenAlex ID.
    Output the results to a CSV file.
    :param inp_dir: path to the directory containing the CSV files storing 1:1 mappings between OMIDs and OpenAlex IDs
    :param out_dir: path to the directory where the output CSV file will be stored
    :return: None
    """
    makedirs(out_dir, exist_ok=True)
    openalex_id_dict = {}

    for row in read_csv_tables(inp_dir):
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
