import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Iterates through all the drop table commands and executes them
    """
    for query in drop_table_queries:
        print(f'Running DROP TABLE SQL:\n {query}')
        cur.execute(query)
        conn.commit()
        print('Done.\n\n')


def create_tables(cur, conn):
    """
    Iterates through all the create table commands and executes them
    """
    for query in create_table_queries:
        print(f'Running CREATE TABLE SQL:\n {query}')
        cur.execute(query)
        conn.commit()
        print('Done.\n\n')


def main():
    """
    Gets the configuration properties from the dwh.cfg file
    Connects to the database
    Calls the functions to drop and create all the tables passing the database cursor and the database connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()