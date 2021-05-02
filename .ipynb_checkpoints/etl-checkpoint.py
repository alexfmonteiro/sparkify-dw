import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Iterates through all the copy commands and executes them
    """
    for query in copy_table_queries:
        print(f'Running copy SQL:\n {query}')
        cur.execute(query)
        conn.commit()
        print('Done.\n\n')


def insert_tables(cur, conn):
    """
    Iterates through all the insert commands and executes them
    """
    for query in insert_table_queries:
        print(f'Running insert SQL:\n {query}')
        cur.execute(query)
        conn.commit()
        print('Done.\n\n')


def main():
    """
    Gets the configuration properties from the dwh.cfg file
    Connects to the database
    Calls the function to load the staging tables passing the database cursor and the database connection
    Calls the function to load the star schema tables passing the database cursor and the database connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()