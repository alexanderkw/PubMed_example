# North West Health Pipeline

A pipeline that takes pubmed article xmls from a directory and loads them into a local database for querying etc.

## Description

Uses prefect workflows to create a Python pipeline from a specified local directory into a specified local SQLite database (to set up a SQLite database see Installing). The scripts handle data in the form of XML pubmed files from https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/.

## Notes

* The set of tasks is intended to be run using a Prefect workflow; see Executing Program for details. Information on linking to Prefect Cloud or Orion is not provided here, although the code does support it.
* Prefect task running at present is effectively consecutive largely for ease of implementation; parallel xml file parsing would enormously speed up implementation in a cloud setting.
* The script at present gets overall values for a PubmedArticle element and many-to-one values for associated Author and Affiliation elements. Other elements are ignored principally due to time considerations for the purposes of this demonstration.
* The script at present ignores existing entries instead of updating them. Updating on duplication could easily be implemented by slightly altering the SQL code.
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
* Download the .xml.gz files desired from https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/ . These will need to be extracted in order to be read by the script, so extract any you wish to load into a suitable directory.

### Executing program

The program can be run via Prefect or run as each of the three major files individually if their respective \_main\_ elements are uncommented.

#### Prefect Workflow

* A local Prefect workflow can be run from the file work_flow_example.py. This will set up tables in a provided SQLite database, replacing any previously present if 'y' is given, and load data into them from .xml files in a given directory. Note that doing so could be very time consuming if using a full xml download set.
```
python work_flow_example.py 'C:/path/to/database.db' 'n' 'C:/path/to/xml/folder'
```

#### Individual File Execution

* Create the SQLite database tables by running sql_setup.py. This takes arguments for the path to the SQLite database and a y/n character for replacing existing tables or not. Initial set up can be done with
```
python sql_setup.py 'C:/path/to/database.db' 'y'
```
* Process into SQL table format and load with xml_reader.py. This takes arguments for the path to the directory containing the xml files to be loaded and the path to the SQLite database. All .xml files in the directory will be loaded.
```
python xml_reader.py 'C:/path/to/xml/folder' 'C:/path/to/database.db'
```
* The example_queries.py file queries the resulting database and demonstrates connections between the major tables. It takes the path to the SQLite database as an argument.
```
python example_queries.py C:/path/to/database.db
```

## Author

Alex K-W