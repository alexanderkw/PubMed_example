import sys
from prefect import flow, task
from sql_setup import sql_setup
import xml_reader


@flow(name='SQL Setup')
def sql_setup_flow(sql_path, replace_yn):
    sql_setup(sql_path, replace_yn)

@flow(name='Parse XML Documents')
def parse_xml_flow(xml_path):
    xml_reader.load_xml_from_directory(xml_path)

@flow(name='Send To SQL')
def sql_load_flow(sql_path, replace_yn):
    sql_setup(sql_path, replace_yn)

if __name__ == "__main__":
    sqlite_filepath = sys.argv[1]
    replace_tables = sys.argv[2]
    xml_in_filepath = sys.argv[3]

    sql_setup_flow(sqlite_filepath, replace_tables)
    parse_xml_flow(xml_in_filepath)
    sql_load_flow(sqlite_filepath)