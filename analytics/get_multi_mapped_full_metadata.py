import pandas as pd
from analytics.helper import create_query_lists_oaid, write_multi_mapped_full_metadata


config = {
    'mm_csv_path': '../openalex_process/mapping_output/multi_mapped/multi_mapped_omids.csv',
    'oa_dump_works': '/vltd/data/openalex/dump/data/works',
    'oa_dump_sources': '/vltd/data/openalex/dump/data/sources',
    'works_full_metadata_out_dir': '../openalex_analytics/multi_mapped_full_metadata/works',
    'sources_full_metadata_out_dir': '../openalex_analytics/multi_mapped_full_metadata/sources',
}


if __name__ == '__main__':

    print('Reading multi-mapped OMIDs CSV file and creating lists of OpenAlex IDs to query for full metadata')
    mm_csv_path = config['mm_csv_path']
    mm_df = pd.read_csv(mm_csv_path, encoding='utf-8')
    mm_works_list, mm_sources_list = create_query_lists_oaid(mm_df)



    print('Writing full metadata JSON-L files fro multi-mapped works')
    write_multi_mapped_full_metadata(query_list=mm_works_list, inp_dir=config['oa_dump_works'], out_dir=config['works_full_metadata_out_dir'])
    print('Writing full metadata JSON-L files for multi-mapped sources')
    write_multi_mapped_full_metadata(query_list=mm_sources_list, inp_dir=config['oa_dump_sources'], out_dir=config['sources_full_metadata_out_dir'])


