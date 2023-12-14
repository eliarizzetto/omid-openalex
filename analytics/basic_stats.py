from analytics.helper import find_inverted_multi_mapped


inp_dir = '../openalex_process/mapping_output/mapped'
out_dir = '../openalex_analytics'

print(find_inverted_multi_mapped(inp_dir, out_dir))
print('Inverted multi-mapped file written to: ', out_dir)
