# XML Reader
# Loads XMLs in /Users/xml-input and processes them into a csv table style

# Static
XML_IN_FILEPATH = 'C:/Users/GreenMachine/IdeaProjects/North-West-Health-Pipeline/xml-in'
# NOTE: please set sqlite_filepath to the file location of your local pubmed.db, otherwise it will be created but
#       no tables will be present
SQLITE_FILEPATH = 'sqlite:///D:/Program Files/sqlite/pubmed.db'

import os
import re
import pathlib
import xmltodict
import sqlalchemy
import xml.etree.ElementTree as et
import pandas

# Get list of XML files in xml-input
xml_files = list(pathlib.Path(XML_IN_FILEPATH).glob('*.xml'))

# Load and parse the files
for file in xml_files:
    # Parse XML as dictionary
    with open(file, 'rb') as xmlfile:
        xtree = et.parse(xmlfile)
        xroot = xtree.getroot()

        # Init the dicts to hold rownum and value for each column
        medline_citation_status = {}
        medline_citation_owner = {}
        pmid = {}
        pmid_version = {}
        date_completed = {}
        date_revised = {}
        pub_model = {}
        issn = {}
        issn_type = {}
        cited_medium = {}
        volume = {}
        issue = {}
        pub_date_month = {}
        pub_date_year = {}
        journal_title = {}
        iso_abbreviation = {}
        article_title = {}
        medline_pgn = {}
        elocation_id = {}
        eid_type = {}
        elocation_valid_yn = {}
        abstract_text = {}
        article_language = {}
        vernacular_title = {}
        country = {}
        medline_ta = {}
        nlm_unique_id = {}
        citation_subset = {}
        publication_status = {}

        # Also assemble leaf locations for each column's values
        leaf_addresses = {
            "medline_citation_status": "MedlineCitation",
            "medline_citation_owner": "MedlineCitation",
            "pmid": "MedlineCitation/PMID",
            "pmid_version": "MedlineCitation/PMID",
            "date_completed": "MedlineCitation/DateCompleted/Day",
            "date_revised": "MedlineCitation/DateRevised/Day",
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
            "article_language": "MedlineCitation/Article/Language",
            "vernacular_title": "MedlineCitation/Article/VernacularTitle",
            "country": "MedlineCitation/MedlineJournalInfo/Country",
            "medline_ta": "MedlineCitation/MedlineJournalInfo/MedlineTA",
            "nlm_unique_id": "MedlineCitation/MedlineJournalInfo/NlmUniqueID",
            "citation_subset": "MedlineCitation/CitationSubset",
            "publication_status": "PubmedData/PublicationStatus"
        }

        # Iterator to keep track of our row number
        i = 0

        for article in xroot:
            # Check if the path exists, if it does get the appropriate value otherwise get None
            leaf = article.find(leaf_addresses['medline_citation_status'])
            if leaf is not None:
                medline_citation_status[i] = leaf.attrib.get("Status")
            else:
                medline_citation_status[i] = None

            leaf = article.find(leaf_addresses['medline_citation_owner'])
            if leaf is not None:
                medline_citation_owner[i] = leaf.attrib.get("Owner")
            else:
                medline_citation_owner[i] = None

            leaf = article.find(leaf_addresses['pmid'])
            if leaf is not None:
                pmid[i] = leaf.text
            else:
                pmid[i] = None

            leaf = article.find(leaf_addresses['pmid_version'])
            if leaf is not None:
                pmid_version[i] = leaf.attrib.get("Version")
            else:
                pmid_version[i] = None

            leaf = article.find(leaf_addresses['date_completed'])
            if leaf is not None:
                date_completed[i] = article.find("MedlineCitation/DateCompleted/Year").text + "-" + article.find("MedlineCitation/DateCompleted/Month").text + "-" + article.find("MedlineCitation/DateCompleted/Day").text
            else:
                date_completed[i] = None

            leaf = article.find(leaf_addresses['date_revised'])
            if leaf is not None:
                date_revised[i] = article.find("MedlineCitation/DateRevised/Year").text + "-" + article.find("MedlineCitation/DateRevised/Month").text + "-" + article.find("MedlineCitation/DateRevised/Day").text
            else:
                date_revised[i] = None

            leaf = article.find(leaf_addresses['pub_model'])
            if leaf is not None:
                pub_model[i] = leaf.attrib.get("PubModel")
            else:
                pub_model[i] = None

            leaf = article.find(leaf_addresses['issn'])
            if leaf is not None:
                issn[i] = leaf.text
            else:
                issn[i] = None

            leaf = article.find(leaf_addresses['issn_type'])
            if leaf is not None:
                issn_type[i] = leaf.attrib.get("IssnType")
            else:
                issn_type[i] = None

            leaf = article.find(leaf_addresses['cited_medium'])
            if leaf is not None:
                cited_medium[i] = leaf.attrib.get("CitedMedium")
            else:
                cited_medium[i] = None

            leaf = article.find(leaf_addresses['volume'])
            if leaf is not None:
                volume[i] = leaf.text
            else:
                volume[i] = None

            leaf = article.find(leaf_addresses['issue'])
            if leaf is not None:
                issue[i] = leaf.text
            else:
                issue[i] = None

            leaf = article.find(leaf_addresses['pub_date_month'])
            if leaf is not None:
                pub_date_month[i] = leaf.text
            else:
                pub_date_month[i] = None

            leaf = article.find(leaf_addresses['pub_date_year'])
            if leaf is not None:
                pub_date_year[i] = leaf.text
            else:
                pub_date_year[i] = None

            leaf = article.find(leaf_addresses['journal_title'])
            if leaf is not None:
                journal_title[i] = leaf.text
            else:
                journal_title[i] = None

            leaf = article.find(leaf_addresses['iso_abbreviation'])
            if leaf is not None:
                iso_abbreviation[i] = leaf.text
            else:
                iso_abbreviation[i] = None

            leaf = article.find(leaf_addresses['article_title'])
            if leaf is not None:
                article_title[i] = leaf.text
            else:
                article_title[i] = None

            leaf = article.find(leaf_addresses['medline_pgn'])
            if leaf is not None:
                medline_pgn[i] = leaf.text
            else:
                medline_pgn[i] = None

            leaf = article.find(leaf_addresses['elocation_id'])
            if leaf is not None:
                elocation_id[i] = leaf.text
            else:
                elocation_id[i] = None

            leaf = article.find(leaf_addresses['eid_type'])
            if leaf is not None:
                eid_type[i] = leaf.attrib.get("EIdType")
            else:
                eid_type[i] = None

            leaf = article.find(leaf_addresses['elocation_valid_yn'])
            if leaf is not None:
                elocation_valid_yn[i] = leaf.attrib.get("ValidYN")
            else:
                elocation_valid_yn[i] = None

            leaf = article.find(leaf_addresses['abstract_text'])
            if leaf is not None:
                abstract_text[i] = leaf.text
            else:
                abstract_text[i] = None

            leaf = article.find(leaf_addresses['article_language'])
            if leaf is not None:
                article_language[i] = leaf.text
            else:
                article_language[i] = None

            leaf = article.find(leaf_addresses['vernacular_title'])
            if leaf is not None:
                vernacular_title[i] = leaf.text
            else:
                vernacular_title[i] = None

            leaf = article.find(leaf_addresses['country'])
            if leaf is not None:
                country[i] = leaf.text
            else:
                country[i] = None

            leaf = article.find(leaf_addresses['medline_ta'])
            if leaf is not None:
                medline_ta[i] = leaf.text
            else:
                medline_ta[i] = None

            leaf = article.find(leaf_addresses['nlm_unique_id'])
            if leaf is not None:
                nlm_unique_id[i] = leaf.text
            else:
                nlm_unique_id[i] = None

            leaf = article.find(leaf_addresses['citation_subset'])
            if leaf is not None:
                citation_subset[i] = leaf.text
            else:
                citation_subset[i] = None

            leaf = article.find(leaf_addresses['publication_status'])
            if leaf is not None:
                publication_status[i] = leaf.text
            else:
                publication_status[i] = None
            # Increment the iterator
            i += 1

        # Create the central data frame using these values
        # First list the dictionaries
        dict_list = [
            medline_citation_status,
            medline_citation_owner,
            pmid,
            pmid_version,
            date_completed,
            date_revised,
            pub_model,
            issn,
            issn_type,
            cited_medium,
            volume,
            issue,
            pub_date_month,
            pub_date_year,
            journal_title,
            iso_abbreviation,
            article_title,
            medline_pgn,
            elocation_id,
            eid_type,
            elocation_valid_yn,
            abstract_text,
            article_language,
            vernacular_title,
            country,
            medline_ta,
            nlm_unique_id,
            citation_subset,
            publication_status
        ]
        # Then coerce into a data frame
        #print(zip(medline_citation_status, medline_citation_owner))
        # Initialise empty data frame
        df_pubmed_article = pandas.DataFrame()
        # get an appropriate column name and dataframe for each
        i = 0
        for dict in dict_list:
            column = pandas.Series(dict, dtype=str)
            df_pubmed_article[list(leaf_addresses.keys())[i]] = column
            i += 1

        # Remove missing and overly malformed entries
        print(len(df_pubmed_article))
        df_pubmed_article = df_pubmed_article[df_pubmed_article.article_title.notnull()]
        print(len(df_pubmed_article))

        #print(df_pubmed_article.elocation_id)

# Begin load component
engine = sqlalchemy.create_engine(SQLITE_FILEPATH, echo=True)

# If present push pubmed_article to temp table
#df_pubmed_article.to_sql('#temp_pubmed_article', engine, if_exists='replace', index=False)

# Detect duplicates
#rs = con.execute(
#    'UPDATE tbl_pubmed_article SET arrests.Value = temp.Value'
#    'FROM tbl_pubmed_article LEFT JOIN #temp_pubmed_article temp'
#    'ON arrests.PK = temp.PK'
#    'WHERE arrests.Value != temp.value'
#)

df_pubmed_article.to_sql('tbl_pubmed_article', engine, if_exists='append', index=False)

        #print(journal_title)
        #xml = xmltodict.parse(xmlfile.read())['PubmedArticleSet']
        #xml = xmltodict.parse(xmlfile.read())
        #xml = flatdict.FlatterDict(
        #    xml,
        #    delimiter='.'
        #)
        #xml.pop('DeleteCitation', None)


