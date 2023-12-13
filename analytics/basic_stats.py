from omid_openalex.mapping import MetaProcessor

mp = MetaProcessor()

count = 0
for row in mp.read_compressed_meta_dump('/vltd/data/meta/dump/meta-csv-dump-2023-10/csv_output_current.zip'):
    count += 1

print('Number of rows in zipped Meta CSV dump: ', count)
