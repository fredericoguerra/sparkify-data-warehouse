import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Loads data into events and songs staging table from log and song json files stored in S3."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Copy data from the staging tables and distribute into each table on the Redshift cluster."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    """Open connection with the cluster using the credentials and paths inserted on dwh.cfg"""
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connected to the cluster sucssesfuly!")
    
    load_staging_tables(cur, conn)
    print("Staging Tables loaded!")
    insert_tables(cur, conn)
    print("Tables inserted!")

    conn.close()


if __name__ == "__main__":
    main()