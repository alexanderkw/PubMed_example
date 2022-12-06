# XML Reader
# Loads XMLs in /Users/xml-input and processes them into a csv table style

import pathlib
import sqlalchemy
import xml.etree.ElementTree as et
import pandas
from prefect import flow
import sys


# Assemble leaf locations for each column's values per article
leaf_addresses = {
    "medline_citation_status": "MedlineCitation",
    "medline_citation_owner": "MedlineCitation",
    "pmid": "MedlineCitation/PMID",
    "pmid_version": "MedlineCitation/PMID",
    "completed_year": "MedlineCitation/DateCompleted/Year",
    "completed_month": "MedlineCitation/DateCompleted/Month",
    "completed_day": "MedlineCitation/DateCompleted/Day",
    "revised_year": "MedlineCitation/DateRevised/Year",
    "revised_month": "MedlineCitation/DateRevised/Month",
    "revised_day": "MedlineCitation/DateRevised/Day",
    "pub_model": "MedlineCitation/Article",
    "issn": "MedlineCitation/Article/Journal/ISSN",
    "issn_type": "MedlineCitation/Article/Journal/ISSN",
    "cited_medium": "MedlineCitation/Article/Journal/JournalIssue",
    "volume": "MedlineCitation/Article/Journal/JournalIssue/Volume",
    "issue": "MedlineCitation/Article/Journal/JournalIssue/Issue",
    "pub_date_month": "MedlineCitation/Article/Journal/JournalIssue/PubDate/Month",
    "pub_date_year": "MedlineCitation/Article/Journal/JournalIssue/PubDate/Year",
    "journal_title": "MedlineCitation/Article/Journal/Title",
    "iso_abbreviation": "MedlineCitation/Article/Journal/ISOAbbreviation",
    "article_title": "MedlineCitation/Article/ArticleTitle",
    "medline_pgn": "MedlineCitation/Article/Pagination/MedlinePgn",
    "elocation_id": "MedlineCitation/Article/ELocationID",
    "eid_type": "MedlineCitation/Article/ELocationID",
    "elocation_valid_yn": "MedlineCitation/Article/ELocationID",
    "abstract_text": "MedlineCitation/Article/Abstract/AbstractText",
    "authorlist_complete": "MedlineCitation/Article/AuthorList",
    "article_language": "MedlineCitation/Article/Language",
    "vernacular_title": "MedlineCitation/Article/VernacularTitle",
    "country": "MedlineCitation/MedlineJournalInfo/Country",
    "medline_ta": "MedlineCitation/MedlineJournalInfo/MedlineTA",
    "nlm_unique_id": "MedlineCitation/MedlineJournalInfo/NlmUniqueID",
    "citation_subset": "MedlineCitation/CitationSubset",
    "publication_status": "PubmedData/PublicationStatus"
}
# Author leaf locations
author_leaf_addresses = {
    'author_valid_yn': '',
    'lastname': 'LastName',
    'forename': 'ForeName',
    'initials': 'Initials',
    'identifier': 'Identifier',
    'identifier_source': 'Identifier'
}
# Author list locations
author_list_leaf_addresses = {
    "medline_citation_status": "MedlineCitation",
    "medline_citation_owner": "MedlineCitation",
    "pmid": "MedlineCitation/PMID",
    'lastname': 'LastName',
    'forename': 'ForeName',
    'initials': 'Initials'
}
# Author affiliation locations
author_affiliation_leaf_addresses = {
    'lastname': 'LastName',
    'forename': 'ForeName',
    'initials': 'Initials',
    'affiliation': 'Affiliation'
}
# Special case values that come from XML variables instead of contents
# Key is database name and value is XML variable name
xml_variable_locations = {
    "medline_citation_status": "Status",
    "medline_citation_owner": "Owner",
    "pmid_version": "Version",
    'pub_model': 'PubModel',
    'issn_type': 'IssnType',
    'cited_medium': 'CitedMedium',
    'eid_type': 'EIdType',
    'elocation_valid_yn': 'ValidYN',
    'author_valid_yn': 'ValidYN',
    'identifier_source': 'Source'
}
# Initialise a data frame for each desired table
df_pubmed_article = pandas.DataFrame(columns=list(leaf_addresses.keys()))
df_author = pandas.DataFrame(columns=list(author_leaf_addresses.keys()))
df_author_list = pandas.DataFrame(columns=list(author_list_leaf_addresses.keys()))
df_author_affiliation = pandas.DataFrame(columns=list(author_affiliation_leaf_addresses.keys()))


# Function for grabbing the XML element value
def get_leaf_value(address_dict: dict, column_identifier: str, xml_branch):
    if address_dict[column_identifier] != '':
        leaf = xml_branch.find(address_dict[column_identifier])
    else:
        leaf = xml_branch
    if leaf is not None:
        # Check if the value is stored in content or as a variable
        if column_identifier in xml_variable_locations:
            # Get the value from the path to the XML variable
            return leaf.attrib.get(xml_variable_locations[column_identifier])
        else:
            # Get the value from the content
            return leaf.text
    else:
        return None


@flow(name="XML parse")
def load_xml_from_directory(directory):
    # Get list of XML files in xml-input
    try:
        xml_files = list(pathlib.Path(directory).glob('*.xml'))
    except:
        print('No XML files found in directory')

    # Iterators to keep track of our row numbers
    i = 0
    i_author = 0
    i_affiliation = 0

    # Load and parse the files
    for file in xml_files:
        # Parse XML as dictionary
        with open(file, 'rb') as xmlfile:
            xtree = et.parse(xmlfile)
            xroot = xtree.getroot()

            for article in xroot:
                # Check for validity - e.g. a human readable title
                # If one isn't found we skip the article entry
                if article.find(leaf_addresses["article_title"]) is None:
                    continue

                # Get start values for dimension tables
                author_start_index = i_author

                # Add to the pubmed article dataframe
                for xml_element in leaf_addresses.keys():
                    df_pubmed_article.at[i, xml_element] = get_leaf_value(leaf_addresses, xml_element, article)

                # Find the authors and check if they exist
                authors = article.find(leaf_addresses["authorlist_complete"])
                if authors is not None:
                    # Add to the author dataframe
                    for author in authors:
                        for xml_element in author_leaf_addresses.keys():
                            df_author.at[i_author, xml_element] = get_leaf_value(author_leaf_addresses, xml_element, author)

                            # Add author foreign key to the the author list dataframe
                            if xml_element in author_list_leaf_addresses:
                                df_author_list.at[i_author, xml_element] = get_leaf_value(author_list_leaf_addresses, xml_element, author)

                            ## Add author foreign key to the the author affiliation dataframe
                            #if xml_element in author_affiliation_leaf_addresses:
                            #    df_author_list.at[i_author, xml_element] = get_leaf_value(author_list_leaf_addresses, xml_element, author)

                        # Get the author affiliation if it exists
                        affiliation_info = article.findall('AffiliationInfo')
                        if affiliation_info is not None:
                            for affiliation in affiliation_info:
                                # Get the affiliation from its branches and the author info from the higher level
                                for xml_element in author_affiliation_leaf_addresses.keys():
                                    if xml_element == 'affiliation':
                                        df_author_affiliation.at[i_affiliation, xml_element] = get_leaf_value(author_affiliation_leaf_addresses, xml_element, affiliation)
                                    else:
                                        df_author_affiliation.at[i_affiliation, xml_element] = get_leaf_value(author_affiliation_leaf_addresses, xml_element, author)

                                # Increment affiliation
                                i_affiliation += 1


                        # Increment author
                        i_author += 1

                ### Finish dimension tables with central table values
                # Author list
                for xml_element in author_list_leaf_addresses.keys():
                    if xml_element in list(df_pubmed_article.columns.values):
                        df_author_list[xml_element].iloc[author_start_index:i_author] = df_pubmed_article[xml_element][i]

                # Increment the article iterator
                i += 1

    # After running all files do final transformation
    # Create medline_article_id based on medline_citation_status, medline_citation_owner and pmid
    df_pubmed_article.insert(
        loc=0,
        column='medline_article_id',
        value= df_pubmed_article[
            [
                'medline_citation_status',
                'medline_citation_owner',
                'pmid'
            ]
        ].agg('-'.join, axis=1)
    )

    df_author_list.insert(
        loc=0,
        column='medline_article_id',
        value= df_author_list[
            [
                'medline_citation_status',
                'medline_citation_owner',
                'pmid'
            ]
        ].agg('-'.join, axis=1)
    )

    # As a dimension table, drop the constituents
    df_author_list.drop(labels=['medline_citation_status', 'medline_citation_owner', 'pmid'], axis=1, inplace=True)


@flow(name="SQL load")
def load_to_sql(sql_target):
    ### Begin SQL load component
    try:
        engine = sqlalchemy.create_engine(sql_target)
    except:
        print('SQLite target not found')
    # Send dataframes to target database as newly created staging tables
    df_pubmed_article.to_sql('temp_pubmed_article', engine, if_exists='replace', index=False)
    df_author.to_sql('temp_author', engine, if_exists='replace', index=False)
    df_author_list.to_sql('temp_author_list', engine, if_exists='replace', index=False)
    df_author_affiliation.to_sql('temp_author_affiliation', engine, if_exists='replace', index=False)

    # From the staging tables insert any entries that are missing
    # Note: this does NOT currently update existing entries
    engine.execute(
        """INSERT INTO tbl_pubmed_article
    SELECT * FROM temp_pubmed_article tmp
    WHERE tmp.medline_article_id
    NOT IN (
        SELECT medline_article_id
        FROM tbl_pubmed_article
    )
    GROUP BY tmp.medline_article_id
    ;"""
    )

    engine.execute(
        """INSERT INTO tbl_author
    SELECT * FROM temp_author tmp
    WHERE IFNULL(tmp.lastname, '') || IFNULL(tmp.forename, '') || IFNULL(tmp.initials, '')
    NOT IN (
        SELECT IFNULL(lastname, '') || IFNULL(forename, '') || IFNULL(initials, '')
        FROM tbl_author
    )
    GROUP BY tmp.lastname, tmp.forename, tmp.initials
    ;"""
    )

    engine.execute(
        """INSERT INTO tbl_author_list
    SELECT * FROM temp_author_list tmp
    WHERE IFNULL(tmp.medline_article_id, '') || IFNULL(tmp.lastname, '') || IFNULL(tmp.forename, '') || IFNULL(tmp.initials, '')
    NOT IN (
        SELECT IFNULL(medline_article_id, '') || IFNULL(lastname, '') || IFNULL(forename, '') || IFNULL(initials, '')
        FROM tbl_author_list
    )
    GROUP BY tmp.medline_article_id, tmp.lastname, tmp.forename, tmp.initials
    ;"""
    )

    engine.execute(
        """INSERT INTO tbl_author_affiliation
    SELECT * FROM temp_author_affiliation tmp
    WHERE IFNULL(tmp.lastname, '') || IFNULL(tmp.forename, '') || IFNULL(tmp.initials, '') || IFNULL(tmp.affiliation, '')
    NOT IN (
        SELECT IFNULL(lastname, '') || IFNULL(forename, '') || IFNULL(initials, '') || IFNULL(tmp.affiliation, '')
        FROM tbl_author_affiliation
    )
    GROUP BY tmp.medline_article_id, tmp.lastname, tmp.forename, tmp.initials
    ;"""
    )

    # Drop the temp tables afterwards
    engine.execute(
        """DROP TABLE IF EXISTS temp_pubmed_article;"""
    )
    engine.execute(
        """DROP TABLE IF EXISTS temp_author;"""
    )
    engine.execute(
        """DROP TABLE IF EXISTS temp_author_list;"""
    )
    engine.execute(
        """DROP TABLE IF EXISTS temp_author_affiliation;"""
    )


if __name__ == '__main__':
    # Either run this from terminal; or if debugging add parameters to the run configuration
    try:
        xml_in_filepath = sys.argv[1]
        sqlite_filepath = sys.argv[2]
    except:
        xml_in_filepath = input('XML directory filepath: ')
        sqlite_filepath = input('SQLite filepath: ')

    load_xml_from_directory(xml_in_filepath)
    load_to_sql('sqlite:///' + sqlite_filepath)