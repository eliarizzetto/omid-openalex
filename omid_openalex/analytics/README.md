* provenance analysis (come viene applicato ai dati, cioè per le risorse unmapped; che tipo di dati coinvolge; i moduli)
  * provenance analysis process launch
* multi-mapped analysis process launch
  * multi-mapped analysis process launch

# Mapping results analysis
The `omid_openalex.analytics` subpackage includes the Python code to analyse the output of the mapping process with special regard to [multi-mapped bibliographic resources and unmapped bibliographic resources](../../README.md#the-mapping-process). Unmapped resources are analysed with respect to their provenance, in order to understand to what extent each of the sources used by OC Meta (e.g. Crossref, Datacite, Zenodo) contributed to provide data that is not currently mapped to OpenAlex.
Multi-mapped bibliographic resources are analysed with respect to their cause and features: the cases where a single OMID has been found to align to multiple OpenAlex IDs are categorised via specifically designed heuristics that aim at catching the most likely cause for such multi-mapping.

## Analysis of unmapped resources' provenance
The process to analyse the provenance of unmapped resources can be launched from CLI with the following command, executed from inside the `omid-openalex` directory:
```
python -m omid_openalex.analytics.prov_analysis -c <PATH>
```
Where:
* `-c` `--config`: path to the YAML configuration file.

An example of the configuration file for the process analysing the provenance of unmapped BRs can be found [here](../../prov_analysis_config.yaml).

The process consists of the following steps:
1. Parse the RDF provenance data of all the bibliographic resources (BRs) in OC Meta (JSON-LD serialisation) and create a SQLite database storing the primary source(s) (e.g. Crossref; Crossref and Datacite) of each resource.
2. Parse the tables storing OC Meta BRs that have external IDs (the ones produced for the mapping process) and store their OMIDs in a flat-file SQLite database (just a single table with only one column).
3. Write a CSV table for OC Meta BRs that have not been processed by the mapping tool, either because there are no external IDs associated with them, or because they were not included in the CSV dump of OC Meta.
4. For each BR that has not been mapped (both processed BR for which no alignment was found and BRs that have not been processed for the mapping, i.e. the BRs in the table created in step 3), retrieve the primary source from the database created in step 1. Count the data sources found for each BR type (e.g. book, journal article, etc.), distinguishing BRs with external IDs from BRs with OMID only.

The parameters specified in the configuration file are the following:
* `br_rdf_path`: the path to the ZIP archive storing the provenance information RDF data of OC Meta BRs
* `prov_db_path`: the path to the database file where to store provenance information (see step 1)
* `meta_tables_csv`: the path to the directory containing the CSV files storing OC Meta BRs that have external IDs (the ones produced for the mapping process)
* `omid_db_path`: the path to the database file where to store the OMIDs of OC Meta BRs that have external IDs (see step 2)
* `extra_br_out_dir`: the path to the directory where to store the CSV tables of BRs that have not been processed for the mapping
* `non_mapped_dir`: the path to the directory storing the CSV tables of unmapped BRs (BRs that have been processed for the mapping but do not align to OpenAlex)
* `results_out_path`: the path to the JSON file where to store the results of the analysis


## Multi mapped BRs categorisation
The process to categorise multi-mapped BRs can be launched from CLI with the following command, executed from inside the `omid-openalex` directory:
```
python -m omid_openalex.analytics.mm_categ -c <PATH>
```
Where:
* `-c` `--config`: path to the YAML configuration file.

An example of the configuration file for the multi-mapped categorisation process can be found [here](../../mm_categ_config.yaml).

The heuristics on which the categorisation is based include data that is available in the CSV tables processed for the mapping as well as additional data that is only stored in the OpenAlex dump. To retrieve and use the latter, a new SQLite database is created, storing the full records of OpenAlex Sources (all of them) and Works (only those that are involved in the multi-mapping).
The process comprises the following steps:
1. Read all OpenAlex compressed JSON-L files of Works and Sources and extract the records to be inserted in the database: for Sources, consider all the records; for Works, consider only multi-mapped resources. The output of this step is two directories, one for Works and the other for Source, containing JSON-L files.
2. Flatten into CSV files the JSON-L files containing the records selected in the previous step.
3. Create the SQLite database and its schema, then copy the CSV files into it.
4. Categorize the multi-mapped OpenAlex records using the heuristics implemented in the `sqlite_categorize_mm()` method of the `mm_categ.MultiMappedClassifier` class. The output of this step is a JSON file providing the count of instances for each category, OpenAlex entity type, and resource type.

OC Meta BRs that are multi-mapped to OpenAlex Sources fit only into one category, "A", which
groups cases where two or more multi-mapped OpenAlex Sources share at least one ISSN. Categories for OpenAlex Works are explained as follows:
 * Category A includes cases where two or more Works among the ones that are multi-mapped to a single OC Meta BR share at least one external PID. Given that external PIDs, such as DOIs, should be uniquely assigned to a BR, having more than one entity with the same external PID in the OpenAlex dataset means that there are either duplicate entities or errors in the metadata. 
 * Category B includes cases where the same entity in OC Meta is mapped to different versions of the same publication, each represented by a Work entity in OpenAlex – e.g. in the case of having a version of record and one or more preprint and/or postprint versions. Preprints and postprints are hosted in a preprint server or a digital repository. DOIs of preprints or postprints are determined by considering the DOI prefix and looking it up on a list of DOI prefixes reserved for institutions that manage preprint servers or digital repositories for non-peer-reviewed publications.
 * Category C includes cases where the same entity in OC Meta is mapped to exactly 2 different Works in OpenAlex, and neither is a preprint or postprint version. The most likely causes for this scenario are errors in the data source used by OC Meta, bugs in OC Meta software, or different DOIs intentionally linked to the same OC Meta entity.
 * Category D includes cases where the same entity in OC Meta is mapped to multiple preprint versions of the same publication, each represented by a Work entity in OpenAlex. This typology is similar to category B, but it only includes preprint versions and detects them by checking for version number (e.g. “/v1”) in the DOI value
 * Category E includes cases where the same entity in OC Meta is mapped to multiple preprint versions of the same publication, each represented by a Work entity in OpenAlex. This typology is similar to categories B and D, but detects preprint versions by analysing the DOI value and checking if it contains semantic indicators that associate the DOI with a preprint server (e.g. “/arxiv” or “/zenodo”).
 * Category F includes cases where the multi-mapped OpenAlex Works include a version of record, together with one or more Works of type “peer-review”, “letter”, “editorial”, “erratum”, or “other”. For example, the DOI for an erratum notice and a DOI for the journal article that is being corrected may be wrongly assigned the same OMID in OC Meta, due to errors in the data source.

## General analysis of the mapping output
For a more general analysis of the mapping output, for the use of tools intended to aid a manual observation of this data and its visualisation, see the suggestions provided in [this Jupyter notebook](../../analysis_guide.ipynb).