import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    The Function executes drop table queries by running a for loop 
    on drop_table_queries list which further contains variables that 
    have been assigned the drop table queries in sql_queries.py. 
    If the tables already exist they will get dropped after calling 
    this function.

    Args:
        conn: Database connection.
        cur: Database cursor.

    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    The Function executes create table queries by running a for loop 
    on create_table_queries list. The tables get created after calling
    this function.

    Args:
        conn: Database connection.
        cur: Database cursor.

    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    #reading config file to get the database configuration details
    config.read('dwh.cfg')

    #Creating connection to the sparkifydb database and a cursor to it
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config.get('CLUSTER','HOST'),\
                            config.get('CLUSTER','DB_NAME'),config.get('CLUSTER','DB_USER'),\
                            config.get('CLUSTER','DB_PASSWORD'), config.get('CLUSTER','DB_PORT')))
    cur = conn.cursor()  
    
    #calling drop_tables method
    drop_tables(cur, conn)
    #calling create_tables method
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()