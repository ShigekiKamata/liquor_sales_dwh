import configparser
import psycopg2
from sql_queries import sample_rows_queries, count_rows_queries

def check_tables(cur, conn):
    '''
    Check if the tables is properly created
    '''
    for query1, query2 in zip(sample_rows_queries, count_rows_queries):
        print('Executing {}...'.format(query1))
        print("------------------------------------------------------")
        cur.execute(query1)
        rows = cur.fetchall()
        for row in rows:
            print(row)
        conn.commit()
        print("------------------------------------------------------")

        print('Executing {}...'.format(query2))
        print("------------------------------------------------------")
        cur.execute(query2)
        row = cur.fetchone()
        print("{} row(s) found".format(row[0]))
        conn.commit()
        print("======================================================")


def main():
    
    # Read config data
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Establish connection
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Check tables
    print('Checking the data...')
    check_tables(cur, conn)
    print('Checking complete')
    
    conn.close()


if __name__ == "__main__":
    main()