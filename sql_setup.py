# SQL Setup
## Takes a SQLite connection and constructs tables, potentially replacing old ones

import sys
import sqlalchemy
from prefect import flow


@flow
def sql_setup(filepath, replace):
    engine = sqlalchemy.create_engine('sqlite:///' + filepath)

    # Drop old tables if replace is set to y, otherwise keep any that already exist
    if replace == 'y':
        engine.execute("""DROP TABLE IF EXISTS tbl_pubmed_article;""")
        engine.execute("""DROP TABLE IF EXISTS tbl_author;""")
        engine.execute("""DROP TABLE IF EXISTS tbl_author_list;""")
        engine.execute("""DROP TABLE IF EXISTS tbl_author_affiliation;""")
    elif replace != 'n':
        print('Replace must be y/n, proceeding without replacement')
    # Create tables
    engine.execute(
        """CREATE TABLE IF NOT EXISTS tbl_pubmed_article (
	medline_article_id TEXT PRIMARY KEY,
	medline_citation_status TEXT,
	medline_citation_owner TEXT,
	pmid INTEGER,
	pmid_version INTEGER,
    completed_year INTEGER,
    completed_month INTEGER,
    completed_day INTEGER,
    revised_year INTEGER,
    revised_month INTEGER,
    revised_day INTEGER,
	pub_model TEXT,
	issn TEXT,
	issn_type TEXT,
	cited_medium TEXT,
	volume INTEGER,
	issue INTEGER,
	pub_date_month TEXT,
	pub_date_year INTEGER,
	journal_title TEXT,
	iso_abbreviation TEXT,
	article_title TEXT,
	medline_pgn TEXT,
	elocation_id TEXT,
	eid_type TEXT,
	elocation_valid_yn TEXT,
	abstract_text TEXT,
	authorlist_complete TEXT,
	article_language TEXT,
	vernacular_title TEXT,
	country TEXT,
	medline_ta TEXT,
	nlm_unique_id INTEGER,
	citation_subset TEXT,
	publication_status TEXT
);""")
    engine.execute(
        """CREATE TABLE IF NOT EXISTS tbl_author (
	author_valid_yn TEXT,
	lastname TEXT,
	forename TEXT,
	initials TEXT,
	identifier TEXT,
	identifier_source TEXT,
	PRIMARY KEY (lastname, forename, initials)
);""")
    engine.execute(
"""CREATE TABLE IF NOT EXISTS tbl_author_list (
	medline_article_id INTEGER,
	lastname TEXT,
	forename TEXT,
	initials TEXT,
	PRIMARY KEY (medline_article_id, lastname, forename, initials),
	FOREIGN KEY (medline_article_id)
       REFERENCES tbl_pubmed_article (medline_article_id) 
);""")
    engine.execute(
"""CREATE TABLE IF NOT EXISTS tbl_author_affiliation (
	lastname TEXT,
	forename TEXT,
	initials TEXT,
	affiliation TEXT,
	PRIMARY KEY (lastname, forename, initials, affiliation),
	FOREIGN KEY (lastname, forename, initials)
       REFERENCES tbl_author (lastname, forename, initials)
);"""
    )


if __name__ == '__main__':
    # Either run this from terminal; or if debugging add parameters to the run configuration
    try:
        sqlite_filepath = sys.argv[1]
        replace_tables = sys.argv[2]
    except:
        sqlite_filepath = input('SQLite filepath: ')
        replace_tables = input('Replace existing tables? y/n: ')

    sql_setup(sqlite_filepath, replace_tables)
