import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads S3 data into Staging_Events table and Staging_Songs table

    Arg:
        cur: executes copy queries; staging_events_copy and staging_stongs_copy 
        conn: commits the executed query
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Takes staging tables data and inserts into the following tables: songplay, users, artists, songs, time.

    Arg:
        cur: executes insert queries; songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert 
        conn: commits the executed queries
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Connects to Redshift Cluster and Database, then executes the load_staging_tables and insert_tables functions
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