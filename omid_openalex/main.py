from mapping import *
import yaml
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process and map OMID to OpenAlex IDs.')
    parser.add_argument('--config', '-c', dest='config', type=str, default='config.yaml',
                        help='Path to the YAML configuration file.')

    # ## Add command line arguments for map_omid_openalex_ids
    # parser.add_argument('-i', '--inp_dir', dest='inp_dir', type=str, help='Input directory for map_omid_openalex_ids')
    # parser.add_argument('-db', '--db_path', dest='db_path', type=str, help='Database path for map_omid_openalex_ids')
    # parser.add_argument('-o', '--out_dir', dest='out_dir', type=str, help='Output directory for map_omid_openalex_ids')
    # parser.add_argument('-t', '--res_type_field', dest='res_type_field', type=bool, default=True, help='Whether to include the resource type field in the mapping tables')

    args = parser.parse_args()

    # Load configuration from the specified YAML file
    with open(args.config, 'r', encoding='utf-8') as config_file:
        settings = yaml.full_load(config_file)

    # Create instances of classes with configuration
    meta_processor = MetaProcessor()
    openalex_processor = OpenAlexProcessor()
    mapping = Mapping()

    # Extract OMIDs, PIDs and types from meta tables ad make new tables
    meta_processor.preprocess_meta_tables(**settings['meta_tables'])

    # Create CSV table for OpenAlex Work IDs
    openalex_processor.create_openalex_ids_table(**settings['openalex_works'])
    # Create CSV table for OpenAlex Source IDs
    openalex_processor.create_openalex_ids_table(**settings['openalex_sources'])

    # Create database tables for PIDs in OpenAlex
    openalex_processor.create_id_db_table(**settings['db_works_doi'])
    openalex_processor.create_id_db_table(**settings['db_works_pmid'])
    openalex_processor.create_id_db_table(**settings['db_works_pmcid'])
    openalex_processor.create_id_db_table(**settings['db_sources_issn'])
    openalex_processor.create_id_db_table(**settings['db_sources_wikidata'])

    # Map OMID to OpenAlex IDs
    mapping.map_omid_openalex_ids(**settings['mapping'])