-- Schema creation

-- Table: authors
CREATE TABLE IF NOT EXISTS authors (
    id TEXT PRIMARY KEY,
    orcid TEXT,
    display_name TEXT,
    display_name_alternatives JSON,
    works_count INTEGER,
    cited_by_count INTEGER,
    last_known_institution TEXT,
    works_api_url TEXT,
    updated_date TIMESTAMP
);

-- Table: authors_counts_by_year
CREATE TABLE IF NOT EXISTS authors_counts_by_year (
    author_id TEXT,
    year INTEGER,
    works_count INTEGER,
    cited_by_count INTEGER,
    oa_works_count INTEGER,
    PRIMARY KEY (author_id, year)
);

-- Table: authors_ids
CREATE TABLE IF NOT EXISTS authors_ids (
    author_id TEXT PRIMARY KEY,
    openalex TEXT,
    orcid TEXT,
    scopus TEXT,
    twitter TEXT,
    wikipedia TEXT,
    mag BIGINT
);

-- Table: concepts
CREATE TABLE IF NOT EXISTS concepts (
    id TEXT PRIMARY KEY,
    wikidata TEXT,
    display_name TEXT,
    level INTEGER,
    description TEXT,
    works_count INTEGER,
    cited_by_count INTEGER,
    image_url TEXT,
    image_thumbnail_url TEXT,
    works_api_url TEXT,
    updated_date TIMESTAMP
);

-- Table: concepts_ancestors
CREATE TABLE IF NOT EXISTS concepts_ancestors (
    concept_id TEXT,
    ancestor_id TEXT,
    PRIMARY KEY (concept_id, ancestor_id)
);

-- Table: concepts_counts_by_year
CREATE TABLE IF NOT EXISTS concepts_counts_by_year (
    concept_id TEXT,
    year INTEGER,
    works_count INTEGER,
    cited_by_count INTEGER,
    oa_works_count INTEGER,
    PRIMARY KEY (concept_id, year)
);

-- Table: concepts_ids
CREATE TABLE IF NOT EXISTS concepts_ids (
    concept_id TEXT PRIMARY KEY,
    openalex TEXT,
    wikidata TEXT,
    wikipedia TEXT,
    umls_aui JSON,
    umls_cui JSON,
    mag BIGINT
);

-- Table: concepts_related_concepts
CREATE TABLE IF NOT EXISTS concepts_related_concepts (
    concept_id TEXT,
    related_concept_id TEXT,
    score REAL,
    PRIMARY KEY (concept_id, related_concept_id)
);

-- Table: institutions
CREATE TABLE IF NOT EXISTS institutions (
    id TEXT PRIMARY KEY,
    ror TEXT,
    display_name TEXT,
    country_code TEXT,
    type TEXT,
    homepage_url TEXT,
    image_url TEXT,
    image_thumbnail_url TEXT,
    display_name_acronyms JSON,
    display_name_alternatives JSON,
    works_count INTEGER,
    cited_by_count INTEGER,
    works_api_url TEXT,
    updated_date TIMESTAMP
);

-- Table: institutions_associated_institutions
CREATE TABLE IF NOT EXISTS institutions_associated_institutions (
    institution_id TEXT,
    associated_institution_id TEXT,
    relationship TEXT,
    PRIMARY KEY (institution_id, associated_institution_id)
);

-- Table: institutions_counts_by_year
CREATE TABLE IF NOT EXISTS institutions_counts_by_year (
    institution_id TEXT,
    year INTEGER,
    works_count INTEGER,
    cited_by_count INTEGER,
    oa_works_count INTEGER,
    PRIMARY KEY (institution_id, year)
);

-- Table: institutions_geo
CREATE TABLE IF NOT EXISTS institutions_geo (
    institution_id TEXT PRIMARY KEY,
    city TEXT,
    geonames_city_id TEXT,
    region TEXT,
    country_code TEXT,
    country TEXT,
    latitude REAL,
    longitude REAL
);

-- Table: institutions_ids
CREATE TABLE IF NOT EXISTS institutions_ids (
    institution_id TEXT PRIMARY KEY,
    openalex TEXT,
    ror TEXT,
    grid TEXT,
    wikipedia TEXT,
    wikidata TEXT,
    mag BIGINT
);

-- Table: publishers
CREATE TABLE IF NOT EXISTS publishers (
    id TEXT PRIMARY KEY,
    display_name TEXT,
    alternate_titles JSON,
    country_codes JSON,
    hierarchy_level INTEGER,
    parent_publisher TEXT,
    works_count INTEGER,
    cited_by_count INTEGER,
    sources_api_url TEXT,
    updated_date TIMESTAMP
);

-- Table: publishers_counts_by_year
CREATE TABLE IF NOT EXISTS publishers_counts_by_year (
    publisher_id TEXT,
    year INTEGER,
    works_count INTEGER,
    cited_by_count INTEGER,
    oa_works_count INTEGER,
    PRIMARY KEY (publisher_id, year)
);

-- Table: publishers_ids
CREATE TABLE IF NOT EXISTS publishers_ids (
    publisher_id TEXT,
    openalex TEXT,
    ror TEXT,
    wikidata TEXT,
    PRIMARY KEY (publisher_id)
);

-- Table: sources
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    issn_l TEXT,
    issn JSON,
    display_name TEXT,
    publisher TEXT,
    works_count INTEGER,
    cited_by_count INTEGER,
    is_oa BOOLEAN,
    is_in_doaj BOOLEAN,
    homepage_url TEXT,
    works_api_url TEXT,
    updated_date TIMESTAMP,
    type TEXT
);

-- Table: sources_counts_by_year
CREATE TABLE IF NOT EXISTS sources_counts_by_year (
    source_id TEXT,
    year INTEGER,
    works_count INTEGER,
    cited_by_count INTEGER,
    oa_works_count INTEGER,
    PRIMARY KEY (source_id, year)
);

-- Table: sources_ids
CREATE TABLE IF NOT EXISTS sources_ids (
    source_id TEXT,
    openalex TEXT,
    issn_l TEXT,
    issn JSON,
    mag BIGINT,
    wikidata TEXT,
    fatcat TEXT,
    PRIMARY KEY (source_id)
);

-- Table: works
CREATE TABLE IF NOT EXISTS works (
    id TEXT PRIMARY KEY,
    doi TEXT,
    title TEXT,
    display_name TEXT,
    publication_year INTEGER,
    publication_date TEXT,
    type TEXT,
    cited_by_count INTEGER,
    is_retracted BOOLEAN,
    is_paratext BOOLEAN,
    cited_by_api_url TEXT,
    abstract_inverted_index JSON,
    language TEXT
);

-- Table: works_primary_locations
CREATE TABLE IF NOT EXISTS works_primary_locations (
    work_id TEXT,
    source_id TEXT,
    landing_page_url TEXT,
    pdf_url TEXT,
    is_oa BOOLEAN,
    version TEXT,
    license TEXT,
    PRIMARY KEY (work_id, source_id)
);

-- Table: works_locations
CREATE TABLE IF NOT EXISTS works_locations (
    work_id TEXT,
    source_id TEXT,
    landing_page_url TEXT,
    pdf_url TEXT,
    is_oa BOOLEAN,
    version TEXT,
    license TEXT,
    PRIMARY KEY (work_id, source_id)
);

-- Table: works_best_oa_locations
CREATE TABLE IF NOT EXISTS works_best_oa_locations (
    work_id TEXT,
    source_id TEXT,
    landing_page_url TEXT,
    pdf_url TEXT,
    is_oa BOOLEAN,
    version TEXT,
    license TEXT,
    PRIMARY KEY (work_id, source_id)
);

-- Table: works_authorships
CREATE TABLE IF NOT EXISTS works_authorships (
    work_id TEXT,
    author_position TEXT,
    author_id TEXT,
    institution_id TEXT,
    raw_affiliation_string TEXT,
    PRIMARY KEY (work_id, author_position)
);

-- Table: works_biblio
CREATE TABLE IF NOT EXISTS works_biblio (
    work_id TEXT PRIMARY KEY,
    volume TEXT,
    issue TEXT,
    first_page TEXT,
    last_page TEXT
);

-- Table: works_concepts
CREATE TABLE IF NOT EXISTS works_concepts (
    work_id TEXT,
    concept_id TEXT,
    score REAL,
    PRIMARY KEY (work_id, concept_id)
);

-- Table: works_ids
CREATE TABLE IF NOT EXISTS works_ids (
    work_id TEXT PRIMARY KEY,
    openalex TEXT,
    doi TEXT,
    mag BIGINT,
    pmid TEXT,
    pmcid TEXT
);

-- Table: works_mesh
CREATE TABLE IF NOT EXISTS works_mesh (
    work_id TEXT,
    descriptor_ui TEXT,
    descriptor_name TEXT,
    qualifier_ui TEXT,
    qualifier_name TEXT,
    is_major_topic BOOLEAN,
    PRIMARY KEY (work_id, descriptor_ui)
);

-- Table: works_open_access
CREATE TABLE IF NOT EXISTS works_open_access (
    work_id TEXT PRIMARY KEY,
    is_oa BOOLEAN,
    oa_status TEXT,
    oa_url TEXT,
    any_repository_has_fulltext BOOLEAN
);

-- Table: works_referenced_works
CREATE TABLE IF NOT EXISTS works_referenced_works (
    work_id TEXT,
    referenced_work_id TEXT,
    PRIMARY KEY (work_id, referenced_work_id)
);

-- Table: works_related_works
CREATE TABLE IF NOT EXISTS works_related_works (
    work_id TEXT,
    related_work_id TEXT,
    PRIMARY KEY (work_id, related_work_id)
);
