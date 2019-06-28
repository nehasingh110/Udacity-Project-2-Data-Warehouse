import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    The Function executes the copy commands to dump the data from
    files residing on S3 to Redshift staging tables.

    Args:
        conn: Database connection.
        cur: Database cursor.

    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    The Function executes the insert queries to populate the
    fact and dimension tables from the staging tables after
    making the appropriate transformations.

    Args:
        conn: Database connection.
        cur: Database cursor.

    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    #Reading configuration file to get the database connection details
    config.read('dwh.cfg')

    #Creating connection to the sparkifydb database and a cursor to it
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config.get('CLUSTER','HOST'),\
                            config.get('CLUSTER','DB_NAME'),config.get('CLUSTER','DB_USER'),\
                            config.get('CLUSTER','DB_PASSWORD'), config.get('CLUSTER','DB_PORT')))
    cur = conn.cursor()
    
    #calling methods
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    #closing the database connection to redshift
    conn.close()


if __name__ == "__main__":
    main()