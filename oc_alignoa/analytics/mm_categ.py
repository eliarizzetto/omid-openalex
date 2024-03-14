from collections import defaultdict
from oc_alignoa.utils import read_csv_tables, MultiFileWriter
from helper import create_mm_oaids_lists, get_openalex_full_metadata
from oc_alignoa.mapping import OpenAlexProcessor
from pprint import pprint
import re
import sqlite3 as sql
import csv
import glob
import json
import os
from tqdm import tqdm
import logging
from datetime import datetime
import argparse
import yaml


class OpenAlexFlattener:
    """
    Flattens OpenAlex JSON-L files into CSV files for relational database insertion. Created from the original script
    by the OpenAlex team accessible at
    https://github.com/ourresearch/openalex-documentation-scripts/blob/495ba3a626086d4dca4aa73f0fbb1c1cf69b0100/flatten-openalex-jsonl.py.
    """
    def __init__(self, csv_dir):

        self.CSV_DIR = csv_dir  # replace with the directory where you want to store the flattened files
        os.makedirs(self.CSV_DIR, exist_ok=True)

        self.FILES_PER_ENTITY = int(os.environ.get('OPENALEX_DEMO_FILES_PER_ENTITY', '0'))

        self.csv_files = {
            'sources': {
                'sources': {
                    'name': os.path.join(self.CSV_DIR, 'sources.csv'),
                    'columns': [
                        'id', 'issn_l', 'issn', 'display_name', 'publisher', 'works_count', 'cited_by_count', 'is_oa',
                        'is_in_doaj', 'homepage_url', 'works_api_url', 'updated_date', 'type'
                    ]
                },
                'ids': {
                    'name': os.path.join(self.CSV_DIR, 'sources_ids.csv'),
                    'columns': ['source_id', 'openalex', 'issn_l', 'issn', 'mag', 'wikidata', 'fatcat']
                },
                'counts_by_year': {
                    'name': os.path.join(self.CSV_DIR, 'sources_counts_by_year.csv'),
                    'columns': ['source_id', 'year', 'works_count', 'cited_by_count', 'oa_works_count']
                },
            },
            'works': {
                'works': {
                    'name': os.path.join(self.CSV_DIR, 'works.csv'),
                    'columns': [
                        'id', 'doi', 'title', 'display_name', 'publication_year', 'publication_date', 'type',
                        'cited_by_count',
                        'is_retracted', 'is_paratext', 'cited_by_api_url', 'abstract_inverted_index', 'language'
                    ]
                },
                'primary_locations': {
                    'name': os.path.join(self.CSV_DIR, 'works_primary_locations.csv'),
                    'columns': [
                        'work_id', 'source_id', 'landing_page_url', 'pdf_url', 'is_oa', 'version', 'license'
                    ]
                },
                'locations': {
                    'name': os.path.join(self.CSV_DIR, 'works_locations.csv'),
                    'columns': [
                        'work_id', 'source_id', 'landing_page_url', 'pdf_url', 'is_oa', 'version', 'license'
                    ]
                },
                'best_oa_locations': {
                    'name': os.path.join(self.CSV_DIR, 'works_best_oa_locations.csv'),
                    'columns': [
                        'work_id', 'source_id', 'landing_page_url', 'pdf_url', 'is_oa', 'version', 'license'
                    ]
                },
                'authorships': {
                    'name': os.path.join(self.CSV_DIR, 'works_authorships.csv'),
                    'columns': [
                        'work_id', 'author_position', 'author_id', 'institution_id', 'raw_affiliation_string'
                    ]
                },
                'biblio': {
                    'name': os.path.join(self.CSV_DIR, 'works_biblio.csv'),
                    'columns': [
                        'work_id', 'volume', 'issue', 'first_page', 'last_page'
                    ]
                },
                'concepts': {
                    'name': os.path.join(self.CSV_DIR, 'works_concepts.csv'),
                    'columns': [
                        'work_id', 'concept_id', 'score'
                    ]
                },
                'ids': {
                    'name': os.path.join(self.CSV_DIR, 'works_ids.csv'),
                    'columns': [
                        'work_id', 'openalex', 'doi', 'mag', 'pmid', 'pmcid'
                    ]
                },
                'mesh': {
                    'name': os.path.join(self.CSV_DIR, 'works_mesh.csv'),
                    'columns': [
                        'work_id', 'descriptor_ui', 'descriptor_name', 'qualifier_ui', 'qualifier_name',
                        'is_major_topic'
                    ]
                },
                'open_access': {
                    'name': os.path.join(self.CSV_DIR, 'works_open_access.csv'),
                    'columns': [
                        'work_id', 'is_oa', 'oa_status', 'oa_url', 'any_repository_has_fulltext'
                    ]
                },
                'referenced_works': {
                    'name': os.path.join(self.CSV_DIR, 'works_referenced_works.csv'),
                    'columns': [
                        'work_id', 'referenced_work_id'
                    ]
                },
                'related_works': {
                    'name': os.path.join(self.CSV_DIR, 'works_related_works.csv'),
                    'columns': [
                        'work_id', 'related_work_id'
                    ]
                },
            },
        }

    @staticmethod
    def _init_dict_writer(csv_file, file_spec, **kwargs):
        writer = csv.DictWriter(
            csv_file, fieldnames=file_spec['columns'], **kwargs
        )
        writer.writeheader()
        return writer

    def flatten_sources(self, inp_dir: str):
        """
        Modified to read non-compressed (.json) files in a flat directory, instead of compressed files in a directory tree.
        :param inp_dir: the directory storing the file to be flattened (used for inserting multi-mapped OpenAlex full
        records into a relational database).
        :return:
        """
        with open(self.csv_files['sources']['sources']['name'], 'wt', encoding='utf-8', newline='') as sources_csv, \
                open(self.csv_files['sources']['ids']['name'], 'wt', encoding='utf-8', newline='') as ids_csv, \
                open(self.csv_files['sources']['counts_by_year']['name'], 'wt', encoding='utf-8',
                     newline='') as counts_by_year_csv:

            sources_writer = csv.DictWriter(
                sources_csv, fieldnames=self.csv_files['sources']['sources']['columns'], extrasaction='ignore'
            )
            sources_writer.writeheader()

            ids_writer = csv.DictWriter(ids_csv, fieldnames=self.csv_files['sources']['ids']['columns'])
            ids_writer.writeheader()

            counts_by_year_writer = csv.DictWriter(counts_by_year_csv,
                                                   fieldnames=self.csv_files['sources']['counts_by_year']['columns'])
            counts_by_year_writer.writeheader()

            seen_source_ids = set()

            files_done = 0
            for jsonl_file_name in tqdm(glob.glob(os.path.join(inp_dir, '*.json'))):
                print(jsonl_file_name)
                with open(jsonl_file_name, 'r', newline='', encoding='utf-8') as sources_jsonl:
                    for source_json in sources_jsonl:
                        if not source_json.strip():
                            continue
                        source_json = source_json.strip()

                        source = json.loads(source_json)

                        if not (source_id := source.get('id')) or source_id in seen_source_ids:
                            continue

                        seen_source_ids.add(source_id)

                        source['issn'] = json.dumps(source.get('issn'))
                        sources_writer.writerow(source)

                        if source_ids := source.get('ids'):
                            source_ids['source_id'] = source_id
                            source_ids['issn'] = json.dumps(source_ids.get('issn'))
                            ids_writer.writerow(source_ids)

                        if counts_by_year := source.get('counts_by_year'):
                            for count_by_year in counts_by_year:
                                count_by_year['source_id'] = source_id
                                counts_by_year_writer.writerow(count_by_year)

                files_done += 1
                if self.FILES_PER_ENTITY and files_done >= self.FILES_PER_ENTITY:
                    break

    def flatten_works(self, inp_dir: str):
        """
        Modified to read non-compressed (.part) files in a flat directory, instead of compressed files in a directory tree.
        :param inp_dir: the directory storing the file to be flattened (used for inserting multi-mapped OpenAlex full
        records into a relational database).
        :return:
        """
        file_spec = self.csv_files['works']

        with open(file_spec['works']['name'], 'w', encoding='utf-8', newline='') as works_csv, \
                open(file_spec['primary_locations']['name'], 'w', encoding='utf-8',
                     newline='') as primary_locations_csv, \
                open(file_spec['locations']['name'], 'w', encoding='utf-8', newline='') as locations, \
                open(file_spec['best_oa_locations']['name'], 'w', encoding='utf-8', newline='') as best_oa_locations, \
                open(file_spec['authorships']['name'], 'w', encoding='utf-8', newline='') as authorships_csv, \
                open(file_spec['biblio']['name'], 'w', encoding='utf-8', newline='') as biblio_csv, \
                open(file_spec['concepts']['name'], 'w', encoding='utf-8', newline='') as concepts_csv, \
                open(file_spec['ids']['name'], 'w', encoding='utf-8', newline='') as ids_csv, \
                open(file_spec['mesh']['name'], 'w', encoding='utf-8', newline='') as mesh_csv, \
                open(file_spec['open_access']['name'], 'w', encoding='utf-8', newline='') as open_access_csv, \
                open(file_spec['referenced_works']['name'], 'w', encoding='utf-8', newline='') as referenced_works_csv, \
                open(file_spec['related_works']['name'], 'w', encoding='utf-8', newline='') as related_works_csv:

            works_writer = self._init_dict_writer(works_csv, file_spec['works'], extrasaction='ignore')
            primary_locations_writer = self._init_dict_writer(primary_locations_csv, file_spec['primary_locations'])
            locations_writer = self._init_dict_writer(locations, file_spec['locations'])
            best_oa_locations_writer = self._init_dict_writer(best_oa_locations, file_spec['best_oa_locations'])
            authorships_writer = self._init_dict_writer(authorships_csv, file_spec['authorships'])
            biblio_writer = self._init_dict_writer(biblio_csv, file_spec['biblio'])
            concepts_writer = self._init_dict_writer(concepts_csv, file_spec['concepts'])
            ids_writer = self._init_dict_writer(ids_csv, file_spec['ids'], extrasaction='ignore')
            mesh_writer = self._init_dict_writer(mesh_csv, file_spec['mesh'])
            open_access_writer = self._init_dict_writer(open_access_csv, file_spec['open_access'])
            referenced_works_writer = self._init_dict_writer(referenced_works_csv, file_spec['referenced_works'])
            related_works_writer = self._init_dict_writer(related_works_csv, file_spec['related_works'])

            files_done = 0
            for jsonl_file_name in tqdm(glob.glob(os.path.join(inp_dir, '*.json'))):
                print(jsonl_file_name)
                with open(jsonl_file_name, 'r', newline='', encoding='utf-8') as works_jsonl:
                    for work_json in works_jsonl:
                        if not work_json.strip():
                            continue

                        work_json = work_json.strip()
                        work = json.loads(work_json)

                        if not (work_id := work.get('id')):
                            continue

                        # works
                        if (abstract := work.get('abstract_inverted_index')) is not None:
                            work['abstract_inverted_index'] = json.dumps(abstract, ensure_ascii=False)

                        works_writer.writerow(work)

                        # primary_locations
                        if primary_location := (work.get('primary_location') or {}):
                            if primary_location.get('source') and primary_location.get('source').get('id'):
                                primary_locations_writer.writerow({
                                    'work_id': work_id,
                                    'source_id': primary_location['source']['id'],
                                    'landing_page_url': primary_location.get('landing_page_url'),
                                    'pdf_url': primary_location.get('pdf_url'),
                                    'is_oa': primary_location.get('is_oa'),
                                    'version': primary_location.get('version'),
                                    'license': primary_location.get('license'),
                                })

                        # locations
                        if locations := work.get('locations'):
                            for location in locations:
                                if location.get('source') and location.get('source').get('id'):
                                    locations_writer.writerow({
                                        'work_id': work_id,
                                        'source_id': location['source']['id'],
                                        'landing_page_url': location.get('landing_page_url'),
                                        'pdf_url': location.get('pdf_url'),
                                        'is_oa': location.get('is_oa'),
                                        'version': location.get('version'),
                                        'license': location.get('license'),
                                    })

                        # best_oa_locations
                        if best_oa_location := (work.get('best_oa_location') or {}):
                            if best_oa_location.get('source') and best_oa_location.get('source').get('id'):
                                best_oa_locations_writer.writerow({
                                    'work_id': work_id,
                                    'source_id': best_oa_location['source']['id'],
                                    'landing_page_url': best_oa_location.get('landing_page_url'),
                                    'pdf_url': best_oa_location.get('pdf_url'),
                                    'is_oa': best_oa_location.get('is_oa'),
                                    'version': best_oa_location.get('version'),
                                    'license': best_oa_location.get('license'),
                                })

                        # authorships
                        if authorships := work.get('authorships'):
                            for authorship in authorships:
                                if author_id := authorship.get('author', {}).get('id'):
                                    institutions = authorship.get('institutions')
                                    institution_ids = [i.get('id') for i in institutions]
                                    institution_ids = [i for i in institution_ids if i]
                                    institution_ids = institution_ids or [None]

                                    for institution_id in institution_ids:
                                        authorships_writer.writerow({
                                            'work_id': work_id,
                                            'author_position': authorship.get('author_position'),
                                            'author_id': author_id,
                                            'institution_id': institution_id,
                                            'raw_affiliation_string': authorship.get('raw_affiliation_string'),
                                        })

                        # biblio
                        if biblio := work.get('biblio'):
                            biblio['work_id'] = work_id
                            biblio_writer.writerow(biblio)

                        # concepts
                        for concept in work.get('concepts'):
                            if concept_id := concept.get('id'):
                                concepts_writer.writerow({
                                    'work_id': work_id,
                                    'concept_id': concept_id,
                                    'score': concept.get('score'),
                                })

                        # ids
                        if ids := work.get('ids'):
                            ids['work_id'] = work_id
                            ids_writer.writerow(ids)

                        # mesh
                        for mesh in work.get('mesh'):
                            mesh['work_id'] = work_id
                            mesh_writer.writerow(mesh)

                        # open_access
                        if open_access := work.get('open_access'):
                            open_access['work_id'] = work_id
                            open_access_writer.writerow(open_access)

                        # referenced_works
                        for referenced_work in work.get('referenced_works'):
                            if referenced_work:
                                referenced_works_writer.writerow({
                                    'work_id': work_id,
                                    'referenced_work_id': referenced_work
                                })

                        # related_works
                        for related_work in work.get('related_works'):
                            if related_work:
                                related_works_writer.writerow({
                                    'work_id': work_id,
                                    'related_work_id': related_work
                                })

                files_done += 1
                if self.FILES_PER_ENTITY and files_done >= self.FILES_PER_ENTITY:
                    break


class MultiMappedClassifier:
    def __init__(self, mm_csv_dir, out_file_path, db_path):
        self.mm_csv_dir = mm_csv_dir
        self.out_file_path = out_file_path
        self.db_path = db_path

        self.possible_preprint_prefixes = {
            '10.1002': ['Open Anthropology Research Repository', 'Earth and Space Science Open Archive', 'Wiley'],
            '10.1097': ['Lippincott® Preprints', 'Ovid Technologies (Wolters Kluwer Health)'],
            '10.1099': ['Microbiology Society'],
            '10.1101': ['bioRxiv', 'medRxiv', 'Cold Spring Harbor Laboratory'],
            '10.1130': ['Geological Society of America'],
            '10.1149': ['The Electrochemical Society'],
            '10.1158': ['American Association for Cancer Research', 'American Association for Cancer Research (AACR)'],
            '10.12788': ['Frontline Medical Communications, Inc.'],
            '10.13031': ['American Society of Agricultural and Biological Engineers',
                         'American Society of Agricultural and Biological Engineers ', '(ASABE)'],
            '10.1364': ['Optica Open', 'Optica Publishing Group'],
            '10.14434': ['Indiana University', 'IUScholarWorks'],
            '10.1484': ['Brepols', 'Brepols Publishers NV'],
            '10.15329': ['Federacao Brasileira de Psicodrama'],
            '10.17077': ['University of Iowa', 'The University of Iowa'],
            '10.20378': ['Universitatsbibliothek Bamberg'],
            '10.20944': ['MDPI AG'],
            '10.21034': ['Federal Reserve Bank of Minneapolis'],
            '10.21072': ['IMBR RAS'],
            '10.21203': ['Research Square', 'Research Square Platform LLC'],
            '10.21428': ['PubPub'],
            '10.21467': ['AIJR Publisher'],
            '10.21504': ['Rhodes University'],
            '10.2196': ['JMIR Publications Inc.'],
            '10.2337': ['American Diabetes Association'],
            '10.24108': ['NPG Publishing'],
            '10.24296': ['JOMI, LLC'],
            '10.26434': ['American Chemical Society (ACS)'],
            '10.26686': ['Open Access Victoria University of Wellington | Te Herenga Waka',
                         'Open Access Te Herenga Waka-Victoria University of Wellington',
                         'Victoria University of Wellington Library'],
            '10.26761': ['International Journal of Research in Library Science'],
            '10.31124': ['Advance', 'SAGE Publications'],
            '10.31219': ['Center for Open Science'],
            '10.31220': ['CABI Publishing'],
            '10.31221': ['Center for Open Science'],
            '10.31222': ['Center for Open Science'],
            '10.31223': ['California Digital Library (CDL)'],
            '10.31224': ['Open Engineering Inc'],
            '10.31225': ['Center for Open Science'],
            '10.31226': ['Center for Open Science'],
            '10.31227': ['Center for Open Science'],
            '10.31228': ['Center for Open Science'],
            '10.31229': ['Center for Open Science'],
            '10.31230': ['Center for Open Science'],
            '10.31231': ['Center for Open Science'],
            '10.31232': ['Center for Open Science'],
            '10.31233': ['Center for Open Science'],
            '10.31234': ['Center for Open Science'],
            '10.31235': ['Center for Open Science'],
            '10.31236': ['Center for Open Science'],
            '10.31237': ['Center for Open Science'],
            '10.31730': ['Center for Open Science'],
            '10.31923': ['PoolText, Inc'],
            '10.32920': ['Toronto Metropolitan University', 'Ryerson University',
                         'Ryerson University Library and Archives'],
            '10.32942': ['California Digital Library (CDL)'],
            '10.33767': ['Center for Open Science'],
            '10.33774': ['Cambridge University Press (CUP)'],
            '10.34055': ['Center for Open Science'],
            '10.35542': ['Center for Open Science'],
            '10.35543': ['Open Access India'],
            '10.36227': ['TechRxiv', 'Institute of Electrical and Electronics Engineers (IEEE)'],
            '10.37281': ['Genesis Sustainable Future Ltd.'],
            '10.3762': ['Beilstein Institut'],
            '10.38140': ['University of the Free State'],
            '10.3897': ['ARPHA Preprints', 'Pensoft Publishers'],
            '10.46715': ['SkepticMed Publishers'],
            '10.47340': ['Millennium Journals'],
            '10.47649': ['Kh.Dosmukhamedov Atyrau University'],
            '10.48199': ['Journal of Urban Planning and Architecture'],
            '10.5117': ['Amsterdam University Press'],
            '10.51767': ['The Bhopal School of Social Sciences'],
            '10.5194': ['Copernicus GmbH'],
            '10.53731': ['Front Matter', 'Syldavia Gazette'],
            '10.5772': ['IntechOpen'],
            '10.6028': ['National Institute of Standards and Technology (NIST)'],
            '10.7554': ['eLife Sciences Publications, Ltd'],
            '10.17615': ['Carolina Digital Repository', 'University of North Carolina at Chapel Hill'],
            # added manually from Datacite
            '10.6084': ['Figshare'],  # added manually from Datacite
            '10.22541': ['Authorea'],  # added manually from Datacite
            '10.5281': ['Zenodo'],  # added manually from Datacite
            '10.17605': ['Center for Open Science', 'OSF'],  # added manually from Datacite
            '10.48550': ['arXiv']  # added manually from Datacite
        }
        self.preprint_string_clues = [
            '/agrirxiv',
            '/arphapreprints',
            '/arxiv',
            '/au.',
            '/chemrxiv',
            '/essoar',
            '/f1000research',
            '/gatesopenres',
            '/healthopenres /amrcopenres',
            '/hrbopenres',
            '/indiarxiv',
            '/jxiv',
            '/mitofit:',
            '/mniopenres',
            '/openresafrica',
            '/osf.io',
            '/peerj.preprints',
            '/preprints',
            '/rs.',
            '/scielopreprints',
            '/wellcomeopenres',
            '/zenodo',
            '/srxiv.'
        ]
        # The 'CATEGORIES' dictionary below is only directed at humans and intended to be used as a reference for the
        # categories' names, it is never used in the code!
        self.CATEGORIES = {
            'works': {
                'A': 'Multiple OpenAlex Works share the same DOI, PMID or PMCID',
                'B': 'DOI(s) for preprint/postprint/version hosted in repository',
                'C': 'Error in data source or 2 entities linked together by mistake (e.g. duplicated DOI)',
                'D': 'Version-marked DOI',
                'E': 'Preprint server DOI',
                'F': 'Multiple DOIs all from the same publisher/DOI issuer: errata, letters, editorials, other',
                'non classified': 'Non classified',
            },
            'sources': {
                'A': 'Multiple OpenAlex Sources share the same ISSN/ISSN-L. Wikidata IDs are not considered.',
                'non classified': 'Non classified',
            }
        }

    def sqlite_categorize_mm(self):
        categories_count = dict()
        categories_count['works'] = defaultdict(lambda: defaultdict(int))
        categories_count['sources'] = defaultdict(lambda: defaultdict(int))
        oa_uri_prefix = 'https://openalex.org/'

        version_pattern = re.compile(
            r'(?:[\.\/]v\d{1,2}[\./])|(?:[\.\/]v\d{1,2}$)|(?:\/\d{1,2}$)|(?:[^a-zA-Z]v\d{1,2}$)')

        with sql.connect(self.db_path) as conn:
            cur = conn.cursor()

            for row in tqdm(read_csv_tables(self.mm_csv_dir), desc='Processing multi-mapped OMIDs', unit='row'):

                prefixed_oaids = [oa_uri_prefix + i for i in row['openalex_id'].split()]
                oaids_data = defaultdict(dict)
                oc_br_type = row['type']

                visited_pids = set()
                visited_doi_prefixes = set()

                # # WORKS
                if row['openalex_id'].startswith('W'):

                    work_ids_query = "SELECT wids.openalex, wids.doi, wids.pmid, wids.pmcid FROM works_ids AS wids WHERE wids.work_id = ?;"
                    query_work_type = "SELECT w.type FROM works AS w WHERE w.id = ?;"
                    query_primloc_version = "SELECT wpl.version FROM works_primary_locations AS wpl WHERE wpl.work_id = ?;"

                    for pos, oaid in enumerate(prefixed_oaids):

                        # -- get the OpenAlex entity PIDs
                        cur.execute(work_ids_query, (oaid,))
                        work_ids_query_result = cur.fetchone()

                        work_ids = {
                            'openalex': work_ids_query_result[0],
                            'doi': work_ids_query_result[1],
                            'pmid': work_ids_query_result[2],
                            'pmcid': work_ids_query_result[3]
                        }
                        oaids_data[oaid]['ids'] = work_ids

                        doi = oaids_data[oaid]['ids']['doi']  # str or None

                        # WORKS CASE D: Multiple OpenAlex Works share the same DOI, PMID or PMCID
                        if any(v in visited_pids for v in oaids_data[oaid]['ids'].values() if v):
                            categories_count['works'][oc_br_type]['A'] += 1
                            break
                        else:
                            visited_pids.update(oaids_data[oaid]['ids'].values())

                        # with version number --> preprint
                        if doi:
                            doi = doi.lower().removeprefix('https://doi.org/')
                            doi_prefix = doi.split('/')[0]
                            if re.findall(version_pattern, doi):
                                categories_count['works'][oc_br_type]['D'] += 1
                                break

                            # WORKS CASE A, B, and C: preprints, postprints, and publisher versions hosted on platforms other than the publisher's
                            if doi_prefix in self.possible_preprint_prefixes.keys():

                                # # -- get the OpenAlex entity's primary location version

                                cur.execute(query_primloc_version, (oaid,))
                                primlocversion_result = cur.fetchone()
                                oaids_data[oaid]['primloc_version'] = primlocversion_result[
                                    0] if primlocversion_result else None
                                if oaids_data[oaid]['primloc_version'] in ['submittedVersion', 'acceptedVersion']:
                                    categories_count['works'][oc_br_type]['B'] += 1
                                    break
                            if any(doi.removeprefix(doi_prefix).startswith(s) for s in self.preprint_string_clues):
                                categories_count['works'][oc_br_type]['E'] += 1
                                break

                            visited_doi_prefixes.add(doi_prefix)

                            # -- get the OpenAlex entity's type
                            cur.execute(query_work_type, (oaid,))
                            worktype_result = cur.fetchone()
                            oaids_data[oaid]['type'] = worktype_result[0] if worktype_result else None

                            if pos == (len(prefixed_oaids) - 1):
                                if doi_prefix in visited_doi_prefixes and len(
                                        visited_doi_prefixes) == 1:  # it means that all the DOIs share the same prefix, therefore have the same publisher
                                    if any(d.get('type') in ['other', 'peer-review', 'editorial', 'erratum', 'letter']
                                           for d
                                           in oaids_data.values()):
                                        categories_count['works'][oc_br_type]['F'] += 1
                                        break
                                    elif len(prefixed_oaids) == 2:
                                        categories_count['works'][oc_br_type]['C'] += 1
                                        break
                                    else:
                                        categories_count['works'][oc_br_type]['non classified'] += 1
                                        break

                                else:
                                    categories_count['works'][oc_br_type]['non classified'] += 1
                                    break
                        if pos == (len(prefixed_oaids) - 1):
                            categories_count['works'][oc_br_type]['non classified'] += 1
                            break

                # # SOURCES
                elif row['openalex_id'].startswith('S'):

                    source_ids_query = """
                        SELECT sids.openalex, sids.issn, sids.wikidata
                        FROM sources_ids sids
                        WHERE sids.source_id = ?;
                    """
                    for pos, oaid in enumerate(prefixed_oaids):
                        cur.execute(source_ids_query, (oaid,))
                        source_ids_query_results = cur.fetchone()
                        source_ids = {
                            'openalex': source_ids_query_results[0],
                            'issn': set(json.loads(source_ids_query_results[1])),
                            # transform json-encoded list into set
                            'wikidata': source_ids_query_results[2]
                        }

                        oaids_data[oaid]['ids'] = source_ids

                        # WORKS CASE A: Multiple OpenAlex Sources share the same ISSN/ISSN-L (do not consider Wikidata IDs)
                        if any(i in visited_pids for i in oaids_data[oaid]['ids']['issn'] if i):
                            categories_count['sources'][oc_br_type]['A'] += 1
                            break
                        else:
                            visited_pids.update(oaids_data[oaid]['ids']['issn'])
                            if pos == (len(prefixed_oaids) - 1):
                                categories_count['sources'][oc_br_type]['non classified'] += 1
                                break

        # transform nested defaultdicts into regular dicts
        categories_count['works'] = {k: dict(v) for k, v in categories_count['works'].items()}

        categories_count['sources'] = {k: dict(v) for k, v in categories_count['sources'].items()}

        logging.info(f'Categories count: {categories_count}')
        pprint(categories_count)

        with open(self.out_file_path, 'w', encoding='utf-8') as out_file:
            json.dump(categories_count, out_file, indent=4)

        return categories_count


def execute_sql_script(db_path, script_path):
    """
    Execute a SQL script on a database.
    """
    conn = sql.connect(db_path)
    cursor = conn.cursor()

    with open(script_path, 'r') as script_file:
        script = script_file.read()
        cursor.executescript(script)

    conn.commit()
    conn.close()


def copy_csv_files_to_db(db_path, csv_data_dir):
    """
    Copy all CSV files storing full records of Sources and Works to a sqlite database.
    :param db_path:
    :param csv_data_dir: directory storing flattened CSV files of Sources and Works.
    """
    with sql.connect(db_path) as conn:
        cur = conn.cursor()
        for file in os.listdir(csv_data_dir):
            print(file)
            if file.endswith(".csv"):
                table_name = file.split(".")[0]
                print(table_name)

                with open(os.path.join(csv_data_dir, file), 'r', encoding='utf-8') as csv_file:
                    reader = csv.DictReader(csv_file)
                    for row in reader:
                        columns:str = ', '.join(row.keys())
                        placeholders:str = ', '.join('?' * len(row))
                        sql_insert = f'INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})'
                        try:
                            cur.execute(sql_insert, list(row.values()))
                        except sql.IntegrityError as e:
                            print(e)
                            print(sql_insert)
                            continue
                    conn.commit()


if __name__ == '__main__':
    log_file = f'mm_categorisation_{datetime.now().strftime("%Y-%m-%d")}.log'
    logging.basicConfig(filename=log_file, level=logging.INFO)

    parser = argparse.ArgumentParser(description='Tool for categorising instances of multi-mapped bibliographic resources.')
    parser.add_argument('-c', '--config', default='mm_categ_config.yaml', help='Path to the YAML configuration file')
    args = parser.parse_args()

    with open(args.config, 'r') as file:
        config = yaml.safe_load(file)

    # >> (1) Read all OpenAlex compressed JSON-L files of Works and Sources and extract the records to be inserted in the
    # database: for Sources, consider all the records and simply decompress the files as they come; for Works, consider
    # only multi-mapped resources and extract the records with the specific function.
    mm_works_list, mm_sources_list = create_mm_oaids_lists(config['mm_csv_dir'])

    print(f'Unzipping OpenAlex compressed JSON-L files of all Sources to {config["sources_full_metadata_dir"]}.')
    with MultiFileWriter(config['sources_full_metadata_dir'], file_extension='json') as writer:
        for row in OpenAlexProcessor.read_compressed_openalex_dump(config['oa_dump_sources']):
            writer.write(row)

    print(f'Writing full metadata JSON-L files for multi-mapped Works at {config["works_full_metadata_dir"]}.')
    get_openalex_full_metadata(query_list=mm_works_list, inp_dir=config['oa_dump_works'],
                               out_dir=config['works_full_metadata_dir'])

    # >> (2) Flatten into CSV files the JSON-L files containing the records selected in the previous step.
    print(f'Flattening full metadata JSON-L files for multi-mapped Works at {config["works_full_metadata_dir"]}.')
    flattener = OpenAlexFlattener(config['flat_csv_dir'])
    flattener.flatten_sources(config['sources_full_metadata_dir'])
    flattener.flatten_works(config['works_full_metadata_dir'])

    # >> (3) Create the SQLite database and its schema, then copy the CSV files into it.
    execute_sql_script(config['db_path'], config['sql_schema_path'])
    print(f'Copying CSV files from {config["flat_csv_dir"]} to the SQLite database at {config["db_path"]}.')
    copy_csv_files_to_db(config['db_path'], config['flat_csv_dir'])

    # >> (4) Categorize the multi-mapped OpenAlex records.
    classifier = MultiMappedClassifier(config['flat_csv_dir'], config['out_file_path'], config['db_path'])
    print(f'Categorizing multi-mapped OpenAlex records and writing the results to {config["out_file_path"]}.')
    classifier.sqlite_categorize_mm()

