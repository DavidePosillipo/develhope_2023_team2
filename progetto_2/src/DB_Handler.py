import psycopg2
import pandas as pd

class DB_Handler():
    def __init__(self, database, user, password, host, database_name):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.database_name = database_name
        try:
            # Connect to the PostgreSQL server
            conn = psycopg2.connect(database=database, user=user, password=password, host=host)
            # Open a cursor to perform database operations
            cur = conn.cursor()
            # Rollback any open transaction
            cur.execute("ROLLBACK")
            # Drop the database if it already exists
            cur.execute(f"DROP DATABASE IF EXISTS {database_name};")
            # Execute a CREATE DATABASE command
            cur.execute(f"CREATE DATABASE {database_name};")
            # Commit the transaction
            conn.commit()
            print("Database created successfully")
        except psycopg2.Error as e:
            print("Error creating database:", e)
        finally:
            if conn is not None:
                cur.close()
                conn.close()

    def create_table_and_import_data(self, table_name, columns, primary_key, csv_path):
        try:
            # Connect to the PostgreSQL server
            conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            # Open a cursor to perform database operations
            cur = conn.cursor()
            # Drop the table if it already exists
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")
            # Create the table
            cur.execute(f"CREATE TABLE {table_name} ({', '.join(columns)}, PRIMARY KEY ({primary_key}));")
            # Import data from CSV to table
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                next(f)
                cur.copy_from(f, table_name, sep=',')
            # Commit the transaction
            conn.commit()
            print("Table created and data imported successfully")
        except psycopg2.Error as e:
            print("Error creating table or importing data:", e)
        finally:
            if conn is not None:
                cur.close()
                conn.close()

    def test_query(self, table_name):
        try:
            # Connect to the PostgreSQL server
            conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            # Open a cursor to perform database operations
            cur = conn.cursor()
            # Execute a SELECT query on the table
            cur.execute(f"SELECT * FROM {table_name} LIMIT 10;")
            rows = cur.fetchall()
            # Print the rows returned by the query
            for row in rows:
                print(row)
        except psycopg2.Error as e:
            print("Error executing SELECT query:", e)
        finally:
            if conn is not None:
                cur.close()
                conn.close()

    def import_table(self, table_name):
        try:
            conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM {table_name} LIMIT 10;")
            data = cur.fetchall()
        except psycopg2.Error as e:
            print("Error importing table:", e)

        cols = []
        for elt in cur.description:
            cols.append(elt[0])

        return pd.DataFrame(data=data, columns=cols)
    
    def save_on_cloud(self):
        local_conn = psycopg2.connect(database = self.database, user = self.user, password = self.password)
        cloud_conn = psycopg2.connect(host='rogue.db.elephantsql.com', database = 'nmpmetdq', user = 'nmpmetdq', password = 'y1CeiiPavj4mxXMlrrt8mH_A0kb0tMeV')

        local_cur = local_conn.cursor()
        cloud_cur = cloud_conn.cursor()

        local_cur.close()
        cloud_conn.close()

        cloud_cur.close()
        local_conn.close()

'''
db = db_handler(database = 'postgres', user = 'postgres', password='postgres', host='localhost', database_name = 'googleplaystore')

table_name = 'googleplaystore_processed'
columns = ['Index INT', 
    'App VARCHAR(256)',
    'Category VARCHAR(256)',
    'Rating FLOAT(50)',
    'Reviews INT',
    'Size INT',
    'Installs INT',
    'Type VARCHAR(15)',
    'Price FLOAT',
    'Content_Rating VARCHAR(30)',
    'Genres VARCHAR(50)',
    'Last_Updated DATE',
    'Age_Restriction INT']
primary_key = 'Index'
csv_path = './database/output/processed_googleplaystore.csv'
db.create_table_and_import_data(table_name, columns, primary_key, csv_path)
df = db.read_table(table_name)
df.head()
'''