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

from omid_openalex.mapping import *
import yaml
import argparse
import logging

if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s %(message)s', filename='mapping.log', filemode='w')
    parser = argparse.ArgumentParser(description='Process and map OMID to OpenAlex IDs.')
    parser.add_argument('--config', '-c', dest='config', type=str, default='config.yaml',
                        help='Path to the YAML configuration file.')


    args = parser.parse_args()

    # Load configuration from the specified YAML file
    with open(args.config, 'r', encoding='utf-8') as config_file:
        settings = yaml.full_load(config_file)

    # Create instances of classes with configuration
    meta_processor = MetaProcessor()
    openalex_processor = OpenAlexProcessor()
    mapping = Mapping()

    # Extract OMIDs, PIDs and types from meta tables ad make new tables
    # meta_processor.preprocess_meta_tables(**settings['meta_tables'])
    #
    # # Create CSV table for OpenAlex Work IDs
    # openalex_processor.create_openalex_ids_table(**settings['openalex_works'])
    # # Create CSV table for OpenAlex Source IDs
    # openalex_processor.create_openalex_ids_table(**settings['openalex_sources'])
    #
    # # Create database tables for PIDs in OpenAlex
    # openalex_processor.create_id_db_table(**settings['db_works_doi'])
    # openalex_processor.create_id_db_table(**settings['db_works_pmid'])
    # openalex_processor.create_id_db_table(**settings['db_works_pmcid'])
    # openalex_processor.create_id_db_table(**settings['db_sources_issn'])
    # openalex_processor.create_id_db_table(**settings['db_sources_wikidata'])

    # Map OMID to OpenAlex IDs
    mapping.map_omid_openalex_ids(**settings['mapping'])
