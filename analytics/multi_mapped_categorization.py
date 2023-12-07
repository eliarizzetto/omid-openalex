from collections import defaultdict
import json
import yaml
from omid_openalex.utils import read_csv_tables
from pprint import pprint
import re
from tqdm import tqdm
import psycopg2 as pg
import logging
import tracemalloc
import sqlite3 as sql

preprint_string_clues = [
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

possible_preprint_prefixes = {
    '10.1002': ['Open Anthropology Research Repository', 'Earth and Space Science Open Archive', 'Wiley'],
    '10.1097': ['Lippincott® Preprints', 'Ovid Technologies (Wolters Kluwer Health)'],
    '10.1099': ['Microbiology Society'],
    '10.1101': ['bioRxiv', 'medRxiv', 'Cold Spring Harbor Laboratory'],
    '10.1130': ['Geological Society of America'],
    '10.1149': ['The Electrochemical Society'],
    '10.1158': ['American Association for Cancer Research', 'American Association for Cancer Research (AACR)'],
    '10.12788': ['Frontline Medical Communications, Inc.'],
    '10.13031': ['American Society of Agricultural and Biological Engineers', 'American Society of Agricultural and Biological Engineers ', '(ASABE)'],
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
    '10.26686': ['Open Access Victoria University of Wellington | Te Herenga Waka', 'Open Access Te Herenga Waka-Victoria University of Wellington', 'Victoria University of Wellington Library'],
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
    '10.32920': ['Toronto Metropolitan University', 'Ryerson University', 'Ryerson University Library and Archives'],
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
    '10.17615': ['Carolina Digital Repository', 'University of North Carolina at Chapel Hill'], # added manually
    '10.6084': ['Figshare'], # added manually
    '10.22541': ['Authorea'], # added manually
    '10.5281': ['Zenodo'], # added manually
    '10.17605': ['Center for Open Science', 'OSF'], # added manually
    '10.48550': ['arXiv'] # added manually
}


def sqlite_categorize_mm(mm_csv_dir_path, out_file_path, db_path):
    categories_count = dict()
    categories_count['works'] = defaultdict(lambda: defaultdict(int))
    categories_count['sources'] = defaultdict(lambda: defaultdict(int))
    oa_uri_prefix = 'https://openalex.org/'

    version_pattern = re.compile(r'(?:[\.\/]v\d{1,2}[\./])|(?:[\.\/]v\d{1,2}$)|(?:\/\d{1,2}$)|(?:[^a-zA-Z]v\d{1,2}$)')

    with sql.connect(db_path) as conn:
        cur = conn.cursor()

        for row in tqdm(read_csv_tables(mm_csv_dir_path), desc='Processing multi-mapped OMIDs', unit='row'):

            prefixed_oaids = [oa_uri_prefix + i for i in row['openalex_id'].split()]
            oaids_data = defaultdict(dict)
            oc_br_type = row['type']

            visited_pids = set()
            visited_doi_prefixes = set()

            # # WORKS
            if row['openalex_id'].startswith('W'):

                work_ids_query = "SELECT wids.openalex, wids.doi, wids.pmid, wids.pmcid FROM works_ids AS wids WHERE wids.work_id = ?;"
                query_work_type = "SELECT w.type FROM works AS w WHERE w.id = ?;"
                # TODO: questa query non funziona, perché mancano i dati delle Sources che ospitano i Works multi-mappati ma non sono a loro volta multi-mappate. usa la seconda, anche se è solo per la versione e non per il type della source
                # query_primloc_type = "SELECT s.type, wpl.version FROM sources AS s JOIN works_primary_locations AS wpl ON s.id = wpl.source_id WHERE wpl.work_id = ?;"
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
                        categories_count['works'][oc_br_type]['D'] += 1
                        break
                    else:
                        visited_pids.update(oaids_data[oaid]['ids'].values())

                    # WORKS CASE A: DOI(s)
                    # with version number --> preprint
                    if doi:
                        doi = doi.lower().removeprefix('https://doi.org/')
                        doi_prefix = doi.split('/')[0]
                        if re.findall(version_pattern, doi):
                            categories_count['works'][oc_br_type]['A'] += 1
                            break

                        # WORKS CASE A, B, and C: preprints, postprints, and publisher versions hosted on platforms other than the publisher's
                        if doi_prefix in possible_preprint_prefixes.keys():

                            # # -- get the OpenAlex entity's primary location type and version

                            # TODO: usa la parte commentata solo se sei riuscito a sistemare il DATABASE (mancano Sources che ospitano Works multi-mappati ma non sono a loro volta multi-mappate). Se fai cosi, togli la parte relativa a query_primloc_version
                            # cur.execute(query_primloc_type, (oaid,))
                            # primloctype_result = cur.fetchone()

                            # oaids_data[oaid]['primloc_type'] = primloctype_result[0] if primloctype_result else None
                            # oaids_data[oaid]['primloc_version'] = primloctype_result[1] if primloctype_result else None
                            # if oaids_data[oaid]['primloc_type'] in ['ebook platform', 'repository', 'other'] or \
                            #         oaids_data[oaid]['primloc_version'] in ['submittedVersion', 'acceptedVersion']:
                            #     categories_count['works'][oc_br_type]['ABC'] += 1
                            #     break

                            cur.execute(query_primloc_version, (oaid,))
                            primlocversion_result = cur.fetchone()
                            oaids_data[oaid]['primloc_version'] = primlocversion_result[0] if primlocversion_result else None
                            if oaids_data[oaid]['primloc_version'] in ['submittedVersion', 'acceptedVersion']:
                                categories_count['works'][oc_br_type]['ABC'] += 1
                                break
                        if any(doi.removeprefix(doi_prefix).startswith(s) for s in preprint_string_clues):
                            categories_count['works'][oc_br_type]['possible_preprints'] += 1
                            break

                        visited_doi_prefixes.add(doi_prefix)
                        # -- get the OpenAlex entity's type
                        cur.execute(query_work_type, (oaid,))
                        worktype_result = cur.fetchone()
                        oaids_data[oaid]['type'] = worktype_result[0] if worktype_result else None

                        if pos == (len(prefixed_oaids) - 1):
                            if doi_prefix in visited_doi_prefixes and len(
                                    visited_doi_prefixes) == 1:  # it means that all the DOIs share the same prefix, therefore have the same publisher
                                if any(d.get('type') in ['other', 'peer-review', 'editorial', 'erratum', 'letter'] for d
                                       in oaids_data.values()):
                                    categories_count['works'][oc_br_type]['EFG'] += 1
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
                        'issn': set(json.loads(source_ids_query_results[1])),  # transform json-encoded list into set
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
    categories_count['works'] = {k: dict(v) for k,v in categories_count['works'].items()} # dict(categories_count['works'])

    categories_count['sources'] = {k: dict(v) for k,v in categories_count['sources'].items()}  #  dict(categories_count['sources'])

    logging.info(f'Categories count: {categories_count}')
    pprint(categories_count)

    with open(out_file_path, 'w', encoding='utf-8') as out_file:
        json.dump(categories_count, out_file, indent=4)


    return categories_count

if __name__ == '__main__':

    logging.basicConfig(filename='sqlite_categorize_mm.log', level=logging.INFO)

    sqlite_categorize_mm('mm_latest/', out_file_path='tmp_mm_categories.json', db_path='E:/sqlite_mm_openalex.db')