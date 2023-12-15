# from analytics.helper import find_inverted_multi_mapped
#
#
# inp_dir = '../openalex_process/mapping_output/mapped'
# out_dir = '../openalex_analytics'
#
# print(find_inverted_multi_mapped(inp_dir, out_dir))
# print('Inverted multi-mapped file written to: ', out_dir)


from omid_openalex.utils import read_csv_tables


# count the number of BRs that have at least one ID supported by OpenAlex (count BRs with only omid + isbn)

non_mappable_count = 0
for row in read_csv_tables('../openalex_process/meta_ids/primary_ents'):
    supported = False
    for indx, id in enumerate(row['id'].split()):
        pref = id.split(':')[0]
        if pref in {'doi', 'pmid', 'pmcid', 'issn', 'wikidata'}:
            supported = True
            break
    if not supported:
        non_mappable_count += 1

print('Number of BRs with no supported IDs in OpenAlex (non mappable): ', non_mappable_count)
