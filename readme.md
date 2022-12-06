# North West Health Pipeline

A pipeline that takes pubmed article xmls from a directory and loads them into a local database for querying etc.

## Description

Uses prefect workflows to create a Python pipeline from a specified local directory into a specified local SQLite database (to set up a SQLite database see Installing). The scripts handle data in the form of XML pubmed files from https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/.

## Notes

* Prefect task running at present is effectively consecutive largely for ease of implementation; parallel xml file parsing would enormously speed up implementation in a cloud setting.
* The script at present gets overall values for a PubmedArticle element and many-to-one values for associated Author and Affiliation elements. Other elements are ignored principally due to time considerations for the purposes of this demonstration.
* A number of assumptions have been made about the nature of the incoming data:
    * The data of interest are the PubmedArticle elements, and not other elements such as those recording deletion. This is reasoned on the basis that other elements are mostly either obviously broken or nearly completely empty; e.g. DeleteCitation consists only of a numeric id.
    * PubmedArticles must contain an ArticleTitle element to be valid, that is, they must have at a title of some kind that is at least theoretically human readable. Articles without titles were viewed as either broken or empty of crucial human readable information.
    * The medline_citation_status, medline_citation_owner, and pmid are asserted to function as a primary key for PubmedArticle elements, on the basis that an article as identified by the pmid can exist in the database in different stages of submission (captured by medline_citation_status and potentially medline_citation_owner) but almost never exists in the database multiple times in the same state. At that point it is viewed as a duplicate.
    * An author’s first and last names and initials are considered to function as a primary key, on the basis that these the total commonly present identifiers used to describe them.

## Getting Started

### Dependencies

* Python 3.10 or higher
* Prefect2 python library and dependencies
* Pandas python library
* SQLite 3

### Installing

* Create a SQLite database using ```sqlite3 your_name_here.db```
* Extract North-West-Health-Pipeline to a suitable location.

### Executing program

* Create the SQLite database tables by running sql_setup.py. This takes arguments for the path to the SQLite database and a y/n character for replacing existing tables or not. Initial set up can be done with
```
python sql_setup.py C:/path/to/database.db y
```
* Process into SQL table format and load with xml-reader.py. This takes arguments for the path to the directory containing the xml files to be loaded and the path to the SQLite database. All .xml files in the directory will be loaded.
```
python xml-reader.py 'C:/path/to/xml/folder C:/path/to/database.db
```

## Author

Alex K-W