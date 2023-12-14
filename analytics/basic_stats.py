from omid_openalex.mapping import OpenAlexProcessor


op = OpenAlexProcessor()

works_dir = '/vltd/data/openalex/dump/data/works'
sources_dir = '/vltd/data/openalex/dump/data/sources'

w_count = 0
w_oaid_only_count = 0
w_other_pids_count= 0
for row in op.read_compressed_openalex_dump(works_dir):
    w_count += 1
    if row['id'].get('doi') or row['id'].get('pmid') or row['id'].get('pmcid'):
        w_other_pids_count += 1
    else:
        w_oaid_only_count += 1

print('WORKS')
print('Number of rows in zipped Meta CSV dump: ', w_count)
print('Number of rows with OAID only: ', w_oaid_only_count)
print('Number of rows with PIDs supported by OC Meta: ', w_other_pids_count)

s_count = 0
s_oaid_only_count = 0
s_other_pids_count= 0
for row in op.read_compressed_openalex_dump(sources_dir):
    s_count += 1
    if row['id'].get('issn') or row['id'].get('wikidata'):
        s_other_pids_count += 1
    else:
        s_oaid_only_count += 1

print('SOURCES')
print('Number of rows in zipped Meta CSV dump: ', s_count)
print('Number of rows with OAID only: ', s_oaid_only_count)
print('Number of rows with PIDs supported by OC Meta: ', s_other_pids_count)
