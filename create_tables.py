import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """This query asserts that no previous tables are created and drops them whether there are anyone."""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates all the tables on dist schemma, including staging events and songs tables. All column types are predefined in this step as well as the relations between those tables."""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    """Open connection with the cluster using the credentials and paths inserted on dwh.cfg"""
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connected to the cluster sucssesfuly!")

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()