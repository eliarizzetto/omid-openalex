# OMID to OpenAlex ID mapping

This repository contains the code to generate the mapping between OMIDs and OpenAlex IDs. The mapping is based on the public persistent identifiers that are associated with a given entity in both the data collections: e.g., if a journal article's DOI is registered in both OpenAlex and OpenCitations Meta, then the corresponding OpenAlex ID and OMID get aligned.
This alignment permits to add the corresponding OpenAlex ID for every resource already registered in OpenCitations Meta.


Diario: https://sleepy-geese-5d2.notion.site/Index-6544baacb4e44b14ac4562926070ae6f?pvs=4

The tasks performed by this software are:
1. to create a mapping between the bibliographic resources (BRs) already existing in OpenCitations Meta (OC Meta) and their 
equivalents in OpenAlex
2. to analyse the output of the mapping process with *ad hoc* processes and functions, with a focus on the provenance 
of OC Meta BRs that are not mapped to any BR in OpenAlex (unmapped BRs) and on OC Meta BRs that are mapped to more than 
one BR in OpenAlex (multi-mapped BRs).

The primary goal of the mapping is the addition of OpenAlex IDs to the metadata of the BRs in OC Meta; analysing the 
mapping's output, moreover, provides the chance to detect potential inconsistencies in the input collections (i.e. OC 
Meta and OpenAlex) and better understand the causes of these issues.

The present guide concerns the mapping process. For the mapping results analysis see the [README.md documentation file](./omid_openalex/analytics/README.md) inside omid_openalex/analytics.

### Launch the mapping process
The process can be launched from CLI with the following command, executed from inside the `omid-openalex` directory:
```
python -m omid_openalex.main -c <PATH>
```
Where:
* `-c` `--config`: path to the configuration file.

A guide on how to write the configuration file is provided [here](#configuration).

## The mapping process
The mapping between the BRs in the two collections is performed by linking them according to the presence, in both 
collections, of a common external persistent identifier (PID). For example, if a journal article in OC Meta has, in its 
metadata the same DOI as a BR in OpenAlex, these entities get aligned ([Fig 1](#fig1)).

<figure>
  <img src="./imgs/mapping.png?raw=true" alt="my alt text"/>
    <figcaption style="font-style: italic; font-size: 85%">
    <i><a id="fig1"><b>Figure 1.</b></a> Visual representation of the mapping between a BR in OC Meta and the corresponding BR in OpenAlex.</i>
    </figcaption>
</figure>

\
The output of this mapping process is divided into three separate groups of BRs:
1. BRs mapped in a 1:1 ratio, i.e. OC Meta BRs that have been mapped to exactly one BR in OpenAlex each and 
their corresponding BRs in OpenAlex.
2. Multi-mapped BRs, i.e. OC Meta BRs that have been mapped to more than one BR in OpenAlex each and their corresponding
BRs in OpenAlex.
3. Unmapped BRs, i.e. OC Meta BRs that have not been mapped to any BR in OpenAlex.

### Input 
The mapping process takes as input the zipped CSV files of an OC Meta dump and the compressed JSON-Lines files of a 
snapshot of the OpenAlex catalog. The OC Meta CSV dump can be downloaded from [Figshare](https://figshare.com/articles/dataset/OpenCitations_Meta_CSV_dataset_of_all_bibliographic_metadata/21747461/6).
The OpenAlex snapshot can be downloaded from Amazon S3 following the dedicated [guide](https://docs.openalex.org/download-all-data/download-to-your-machine) by the OpenAlex team.

Each row of the OC Meta CSV tables store basic metadata about a BR: for the purpose of the mapping and the following 
analysis, we are interested in the `id` field (storing the BR's OMID and, if present, external PIDs, such as DOIs or ISSNs)
and the `type` field, specifying the BR type (e.g. journal article, book, journal, dataset, etc.).

``` csv
# a sample CSV row of OC Meta dump
"id","title","author","issue","volume","venue","page","pub_date","type","publisher","editor"
"omid:br/060924 doi:10.4230/lipics.fun.2021.11","Efficient Algorithms For Battleship","Crombez, Loïc [omid:ra/062503694315 orcid:0000-0002-9542-5276]; Fonseca, Guilherme D. Da [omid:ra/0612046462 orcid:0000-0002-9807-028X]; Gerard, Yan [omid:ra/0612046189 orcid:0000-0002-2664-0650]","","","LIPIcs : Leibniz International Proceedings In Informatics [omid:br/060182 issn:1868-8969]","","2020","report","Schloss Dagstuhl - Leibniz-Zentrum Für Informatik [omid:ra/0607497]","Farach, Martin [omid:ra/0614045842 orcid:0000-0003-3616-7788]; Prencipe, Giuseppe [omid:ra/0625025102 orcid:0000-0001-5646-7388]; Uehara, Ryuhei [omid:ra/0617014675 orcid:0000-0003-0895-3765]"
```

In the snapshot of the OpenAlex collection, BRs and their metadata are distinguished into two entity types: Works (e.g. 
journal articles, books, reports) and Sources (i.e. where Works are contained, e.g. journals, conferences, repositories).
Each Work or Source is represented inside the JSON-Lines files as a JSON object, of which, mapping-wise, we need to 
consider only the `ids` field; this is itself a JSON object where keys are PID schemes (e.g. `doi`, `issn`) and values 
are the values for those schemes.

``` json
# a sample from the JSON object representing a Source entity in the OpenAlex snaphot
{
   "id":"https://openalex.org/S168707975",
   "issn_l":"1941-7012",
   "issn":[
      "1941-7012"
   ],
   "display_name":"Journal of Renewable and Sustainable Energy",
    ...
   "ids":{
      "openalex":"https://openalex.org/S168707975",
      "issn_l":"1941-7012",
      "issn":[
         "1941-7012"
      ],
      "mag":"168707975",
      "wikidata":"https://www.wikidata.org/entity/Q6295857",
      "fatcat":"https://fatcat.wiki/container/xgsd2zrqkzhgpmbd3ofxlhuxci"
   },
    ...
   "type":"journal",
    ...
}
```
### Process
First, the input data (CSV files of the OC Meta dump and JSON-Lines files of the OpenAlex snapshot) is pre-processed to 
create new tables:
- a table storing the OMID and the external PIDs of each BR in the OC Meta CSV dump, and optionally 
its type (`omid`, `ids` and `type` fields, respectively). In this step, OC Meta BRs without external PIDs are discarded, 
meaning that they will not be included in the produced table and therefore will not be considered for the mapping step.
- two tables, one for OpenAlex Works and the other for OpenAlex Sources, storing the OpenAlex IDs and the external PIDs 
of each Work/Source in the OpenAlex snapshot, among the ID schemes that are also supported by OC Meta. In both tables, 
each row stores an association between a single, mutually-supported external PID (`supported_id` field) and the OpenAlex ID of the 
resource it is attributed to (`openalex_id` field). In this step, OpenAlex BRs without external PIDs
that are also supported by OC Meta are discarded, meaning that they will not be included in the produced tables and therefore
will not be considered for the mapping step.

Then, the two tables storing the IDs of the BRs in OpenAlex are converted into an SQLite database
to enable faster queries on this data. Each external PID is a primary key, to which a single OpenAlex ID is linked ([Fig 2](#fig2)).

<figure>
  <img src="./imgs/oa_ids_tables_db.png?raw=true" alt="Entity relationship diagram for the database storing OpenAlex IDs and the external PIDs associated to them"/>
    <figcaption style="font-style: italic; font-size: 85%">
    <i><a id="fig2"><b>Figure 2.</b> </a>Diagram representing the SQLite database storing OpenAlex IDs and the external PIDs associated to them.</i>
    </figcaption>
</figure>

\
Finally, the actual mapping step takes place: for each row in the table storing OC Meta IDs (i.e. for each OC Meta
BR with external PIDs), each external PID is looked for in the database: if it is present, the OMID 
linked to that PID from the CSV table and the OpenAlex ID linked to it in the database are aligned. For every OC Meta BR 
_**all**_ external PIDs are looked for in the database: if all of them point to the same OpenAlex ID, 
then the resource in OC Meta is mapped to a single resource in OpenAlex; if they point to different OpenAlex IDs, this results 
in a multi-mapped case; if none of the external PIDs is found in the database, no mapping is found for that BR.

### Output

The three groups of BRs that make up the output of the mapping process are stored in three separate CSV tables. In the tables for 
BRs mapped in a 1:1 ratio and tables for multi-mapped BRs each row constitutes an alignment between the BR in OC Meta
(represented by its OMID, in the `omid` field) and the BR(s) in OpenAlex (represented by one or multiple OpenAlex IDs, 
in the `openalex_id` field); the optional `type` field stores the type of BRs as registered in OC Meta.
In the tables storing unmapped BRs each rows contain only the OMID and, 
optionally, the BR type, in the `omid` and `type` fields respectively.

## Software structure
The `utils` module contains the `read_csv_tables` function and the `MultiFileWriter` class, which are used across the 
whole software to read and write tables.
The source code for the mapping process can be found inside the `mapping` module. There are three classes ([Fig 3](#fig3)):
1. The `MetaProcessor` class deals with creating the tables storing OC Meta BRs that have external IDs (`process_meta_tables` method).
2. The `OpenAlexProcessor` class deals with creating the tables storing OpenAlex BRs that have external IDs supported also by OC Meta (`create_openalex_ids_tables` method)
and with converting these tables into SQLite database tables (`create_id_db_table` method).
3. The `Mapping` class has only one method, `map_omid_openalex_ids`, which is the implementation of the mapping step.

As of now, some software features concern the pre-processing of responsible agents (authors, publishers, editors) and the separate pre-processing of venues (container publications such as journals, conferences, etc.). For example:
* the `MetaProcessor.process_meta_tables` function saves separate tables for all bibliographic resources (in a directory named `primary_ents`), for venues only (in a directory named `venues`) and for responsible agents (in a directory named `resp_ags`)
* the `OpenAlexProcessor` class includes methods for extracting PIDs of other OpenAlex entity types besides Works and Sources (namely Institutions, Publishers, Funders  and Authors)
These features are to be considered experimental and are not used for the mapping process.

The `main` module is the executable to run the whole process (except the result analysis process, which is run separately). The following section illustrates how to write the configuration file storing the arguments passed to the functions called inside `main.py`.

<figure>
  <img src="./imgs/mapping_class_diagram.png?raw=true" alt="UML Class diagram"/>
    <figcaption style="font-style: italic; font-size: 85%">
    <i><a id="fig3"><b>Figure 3.</b> </a>UML Class diagram of the software for the mapping process.</i>
    </figcaption>
</figure>

## Configuration
Function calls in the `main` module (which stores the code to execute the mapping process) take their parameters from a YAML configuration file, 
whose path is specified in the `--config` argument of the launching command.
The YAML file is read as a nested dictionary, and arguments in it are grouped under different keys, according to the purpose they serve. An example of the configuration file can be found in the [config.yaml](config.yaml) file. The 
following illustrates how to compile the configuration file.

#### `meta_tables`
Groups the parameters to pass to `MetaProcessor.process_meta_tables()` for creating CSV tables of OC Meta BRs with external PIDs.
- `meta_dump_zip` (str): path to the ZIP file of the OC Meta dump
- `meta_ids_out` (str): path to the directory where to save the CSV tables. **Here, the tables will be stored in a subdirectory named `primary_ents`**
- `all_rows` (bool): if True, processes all the BRs in the OC Meta CSV dump, regardless of whether a BR already has an OpenAlex ID. If False, only BRs for which the OpenAlex ID is missing are processed.

#### `openalex_works`
Groups the parameters to pass to `OpenAlexProcessor.create_openalex_ids_tables()` for creating CSV tables of OpenAlex Works with external PIDs supported also in OC Meta.
- `inp_dir` (str): the path to the directory storing data of OpenAlex Works
- `out_dir` (str): the path to the directory where to store the CSV tables with OpenAlex Works
- `entity_type` (str): the OpenAlex entity type. Since the directory to process contains Works, it must be set to "work".

#### `openalex_sources`
Groups the parameters to pass to `OpenAlexProcessor.create_openalex_ids_tables()` for creating CSV tables of OpenAlex Works with external PIDs supported also in OC Meta.
The parameters are named the same as `openalex_works`, as they are used by the same function, but they must be set to deal with OpenAlex Sources
instead of Works. The `entity_type` parameter must be set to `source`.

#### `db_works_doi`
Groups the parameters to pass to `OpenAlexProcessor.create_id_db_table()` for creating a database table storing the DOIs of Work entities in OpenAlex.
- `inp_dir` (str): path to the directory containing CSV tables storing OpenAlex Works with external PIDs (i.e. the same as `openalex_works.out_dir`)
- `db_path` (str): the path to the SQLite database where to store the data
- `id_type` (str): the ID scheme of the IDs to store in the table. Since we want to store Works' _DOIs_, it must be set to "doi".
- `entity_type` (str): the OpenAlex entity type for the database table to produce. Since we want to store DOIs for _Works_, it must be set to "work".

#### `db_works_pmid`, `db_works_pmcid`, `db_sources_issn`, `db_sources_wikidata`
These group the parameters to pass to `OpenAlexProcessor.create_id_db_table()` for creating database tables for PMIDs and PMCIDs of Works, and
for ISSNs and Wikidata IDs of Sources. This follows the same logic as the parameters in `db_works_doi`, but the argument values must be adapted (except `db_path`).
When processing Works, `entity_type` must be set to "work and `id_type` must be set to "pmid" and "pmcid"; when processing Sources, `entity_type` must be set to "source" and `id_type` must be set to "issn" and "wikidata".

#### `mapping`
Groups the parameters to pass to `Mapping.map_omid_openalex_ids()` for creating the mapping.
- `inp_dir` (str): the directory where the table storing OC Meta BRs with external PIDs are saved, i.e. the directory "primary_ents" inside `meta_tables.meta_ids_out`
- `db_path` (str): The path to the database storing OpenAlex BRs
- `out_dir` (str): The directory where to save the tables storing BRs mapped in a 1:1 ratio
- `multi_mapped_dir` (str): The directory where to save the table storing multi-mapped BRs
- `non_mapped_dir` (str): The directory where to save the table storing unmapped BRs
- `type_field` (bool): If True, always write the `type` field in the tables.
- `all_rows` (bool): If True, processes all the BRs in the input table, regardless of whether a BR already has an OpenAlex ID. If False, only BRs for which the OpenAlex ID is missing are processed.
