from omid_openalex.mapping import MetaProcessor

mp = MetaProcessor()

count = 0
omid_only_count = 0
other_pids_count= 0
for row in mp.read_compressed_meta_dump('/vltd/data/meta/dump/meta-csv-dump-2023-10/csv_output_current.zip'):
    count += 1
    if ' ' in row['id']:
        other_pids_count += 1
    else:
        omid_only_count += 1

print('Number of rows in zipped Meta CSV dump: ', count)
print('Number of rows with OMIDs only: ', omid_only_count)
print('Number of rows with other PIDs: ', other_pids_count)
