import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Drops all the table using queries in sql_queries
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Creates tables using queries in sql_queries
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():

    # Read credentials from config file 
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Establish Connection
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Drop tables 
    drop_tables(cur, conn)
    # Create tables
    create_tables(cur, conn)

    conn.close()
    print('Tables created')

if __name__ == "__main__":
    main()