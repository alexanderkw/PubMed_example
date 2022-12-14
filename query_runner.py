# SQL Query Runner
# Uses SQLAlchemy to run arbitrary queries based on SQL input

from prefect import flow
import sqlalchemy
import sys

@flow(name='Run Query')
def run_query(sqlite_filepath, sql_query):
    engine = sqlalchemy.create_engine('sqlite:///' + sqlite_filepath)

    print("Running query parameter")
    print(sql_query)
    print("____________________")

    query_result = engine.execute(sql_query)
    if sql_query.startswith("SELECT"):
        for row in query_result:
            print(row)
    else:
        print("Execution complete")

    print("____________________")

if __name__ == '__main__':
    # Either run this from terminal; or if debugging add parameters to the run configuration
    try:
        sqlite_filepath = sys.argv[1]
        sql_query = sys.argv[2]
    except:
        sqlite_filepath = input('SQLite filepath: ')
        sql_query = input('SQL query to run: ')

    run_query(sqlite_filepath, sql_query)