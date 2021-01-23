import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Copies the data from the log files to the staging tables 
    '''
    for query in copy_table_queries:
        print('Executing {}...'.format(query))
        cur.execute(query)
        print("Done")
        conn.commit()


def insert_tables(cur, conn):
    '''
    Inserts the data from the staging tables to the final tables
    '''
    for query in insert_table_queries:
        print('Executing {}...'.format(query))
        cur.execute(query)
        print("Done")
        conn.commit()


def main():
    
    # Read credentials from cinfig file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Establish connection
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Load data from S3 to the staging tables 
    print('Loading S3 data into the staging...')
    load_staging_tables(cur, conn)
    print('Loading complete')

    # Insert data from staging tables to the final tables
    print('Inserting data into the tables...')
    insert_tables(cur, conn)
    print('Inserting complete')
    
    conn.close()


if __name__ == "__main__":
    main()