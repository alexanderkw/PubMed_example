# XML Reader
# Loads XMLs in /Users/xml-input and processes them into a csv table style

import sys
import sqlalchemy

def query_test(sql_target):
    ### Begin SQL load component
    engine = sqlalchemy.create_engine(sql_target, echo=True)
    try:
        engine = sqlalchemy.create_engine(sql_target)
    except:
        print('SQLite target not found')

    test_list = [
        ('Get the largest pmid present in tbl_pubmed_article:',
         """SELECT MAX(pmid)
FROM tbl_pubmed_article;"""),
        ('Get the 5 authors with the alphabetically last surnames:',
         """SELECT forename, lastname, initials
FROM tbl_author
ORDER BY lastname DESC
LIMIT 5;"""),
        ('Look at 5 affiliations:',
         """SELECT * FROM tbl_author_affiliation LIMIT 5;"""),
        ('Return five journals with published authors whose last names start with Q:',
         """SELECT DISTINCT tbl_pubmed_article.journal_title
FROM tbl_pubmed_article
NATURAL JOIN tbl_author_list
NATURAL JOIN tbl_author
WHERE tbl_author.lastname LIKE 'Q%'
LIMIT 5;"""),
        ('Get 8 affiliations of authors with a lastname ending in C:',
         """SELECT DISTINCT tbl_author_affiliation.affiliation
FROM tbl_author_affiliation
NATURAL JOIN tbl_author
WHERE tbl_author.lastname LIKE 'Q%'
LIMIT 8;""")
    ]

    for test in test_list:
        print(test[0])
        query_result = engine.execute(test[1])
        for row in query_result:
            print(row)


if __name__ == '__main__':
    # Either run this from terminal; or if debugging add parameters to the run configuration
    try:
        sqlite_filepath = sys.argv[1]
    except:
        sqlite_filepath = input('SQLite filepath: ')

    query_test('sqlite:///' + sqlite_filepath)