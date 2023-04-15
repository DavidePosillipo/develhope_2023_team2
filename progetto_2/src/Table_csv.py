import psycopg2
import csv

class db_handler():
    def __init__(self, database, user, password, host, database_name):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.database_name = database_name
        try:
            # Connect to the PostgreSQL server
            self.conn = psycopg2.connect(database=database, user=user, password=password, host=host)
            # Open a cursor to perform database operations
            self.cur = self.conn.cursor()
            # Rollback any open transaction
            self.cur.execute("ROLLBACK")
            # Drop the database if it already exists
            self.cur.execute(f"DROP DATABASE IF EXISTS {database_name};")
            # Execute a CREATE DATABASE command
            self.cur.execute(f"CREATE DATABASE {database_name};")
            # Commit the transaction
            self.conn.commit()
            print("Database created successfully")
        except psycopg2.Error as e:
            print("Error creating database:", e)
        finally:
            if self.conn is not None:
                self.cur.close()
                self.conn.close()

    def create_table(self, query):
        try:
            # Connect to the PostgreSQL server and create a new database
            self.conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            # Open a cursor to perform database operations
            self.cur = self.conn.cursor()
            # Execute the query to create the table
            self.cur.execute(query)
            # Commit the transaction
            self.conn.commit()
            print("Table created successfully")
        except psycopg2.Error as e:
            print("Error creating table:", e)
        finally:
            if self.conn is not None:
                self.cur.close()
                self.conn.close()

    def insert_data_from_csv(self, path, query):
        try:
            # Connect to the PostgreSQL server
            self.conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            # Open a cursor to perform database operations
            self.cur = self.conn.cursor()
            # Read in the CSV file and insert data into the categories table
            with open(path, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                next(reader)  # skip the header row
                for row in reader:
                    category = row[2]
                    print(f"Row: {row}")
                    print(f"Category: {category}")
                    self.cur.execute(query, (category, category))
            # Commit the transaction
            self.conn.commit()
            print("Data inserted successfully")
        except psycopg2.Error as e:
            print("Error inserting data:", e)
        finally:
            if self.conn is not None:
                self.cur.close()
                self.conn.close()

db = db_handler('postgres', 'postgres', 'c', 'localhost', 'prova_db')
table_query = """
    CREATE TABLE categories (
        CategoryID SERIAL PRIMARY KEY,
        Name VARCHAR(256) NOT NULL
    )
"""
db.create_table(table_query)

insert_query = """
    INSERT INTO categories (Name)
    SELECT %s
    WHERE NOT EXISTS (
        SELECT 1 FROM categories WHERE Name = %s
    )
"""
db.insert_data_from_csv('./database/output/processed_googleplaystore.csv', insert_query)
