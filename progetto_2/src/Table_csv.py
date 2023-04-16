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
    
    
    def insert_values_main(self, table_name, csv_path):
        try:
            # Connect to the PostgreSQL server
            self.conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            # Open a cursor to perform database operations
            self.cur = self.conn.cursor()
            # Import data from CSV to table
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                next(f)
                self.cur.copy_from(f, table_name, sep=',')
            # Commit the transaction
            self.conn.commit()
            print("Table created and data imported successfully")
        except psycopg2.Error as e:
            print("Error creating table or importing data:", e)
        finally:
            if self.conn is not None:
                self.cur.close()
                self.conn.close()

    def insert_values_categories(self, path, query):
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

    def insert_values_apps(self, path, query):
        try:
            # Connect to the PostgreSQL server
            self.conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            # Open a cursor to perform database operations
            self.cur = self.conn.cursor()
            # Read in the CSV file and insert data into the apps table
            with open(path, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                next(reader)  # skip the header row
                for row in reader:
                    App_ID, app_name, category, rating, reviews, size, installs, app_type, price, content_rating, genres, last_updated, age_restriction = row
                    app_id_query = """SELECT "App ID" FROM processed_googleplaystore WHERE "App Name" = %s"""
                    category_id_query = """SELECT "Category ID" FROM categories WHERE Name = %s"""
                    self.cur.execute(category_id_query, (category,))
                    category_id = self.cur.fetchone()[0]
                    app_values = (App_ID, app_name, category_id, rating, reviews, size, installs, app_type, price, content_rating, genres, last_updated, age_restriction)
                    self.cur.execute(query, app_values)
            # Commit the transaction
            self.conn.commit()
            print("Data inserted successfully")
        except psycopg2.Error as e:
            print("Error inserting data:", e)
        finally:
            if self.conn is not None:
                self.cur.close()
                self.conn.close()

    
    def test_query(self, table_name, limit=False):
        try:
            # Connect to the PostgreSQL server
            conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            # Open a cursor to perform database operations
            cur = conn.cursor()
            # Execute a SELECT query on the table
            if limit==True:
                cur.execute(f"SELECT * FROM {table_name} LIMIT 10;")
            else:
                cur.execute(f"SELECT * FROM {table_name};")
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

db = db_handler('postgres', 'postgres', 'c', 'localhost', 'prova_db')

table_query = """
    CREATE TABLE processed_googleplaystore (
    "App ID" INT PRIMARY KEY,
    "App Name" VARCHAR(256),
    Category VARCHAR(50),
    Rating VARCHAR(10),
    Reviews VARCHAR(50),
    Size VARCHAR(50),
    Installs VARCHAR(50),
    Type VARCHAR(10),
    Price VARCHAR(50),
    "Content Rating" VARCHAR(50),
    Genres VARCHAR(50),
    "Last Updated" VARCHAR(50),
    "Age Restriction" VARCHAR(50)
    )
"""
db.create_table(table_query)
db.insert_values_main('processed_googleplaystore', './database/output/processed_googleplaystore.csv')
db.test_query('processed_googleplaystore', True)

table_query = """
    CREATE TABLE categories (
        "Category ID" SERIAL PRIMARY KEY,
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
db.insert_values_categories('./database/output/processed_googleplaystore.csv', insert_query)
db.test_query('categories')

table_query = """
    CREATE TABLE Apps (
        "App ID" INT REFERENCES processed_googleplaystore("App ID"),
        "App Name" VARCHAR(256),
        "Category ID" INT REFERENCES categories("Category ID"),
        Rating FLOAT(50),
        Reviews INT,
        Size INT,
        Installs INT,
        Type VARCHAR(15),
        Price FLOAT,
        "Content Rating" VARCHAR(30),
        Genres VARCHAR(50),
        "Last Updated" DATE,
        "Age Restriction" INT
    )
"""
db.create_table(table_query)

query = """INSERT INTO Apps (
                "App ID", "App Name", "Category ID", Rating, Reviews, Size, Installs, Type, Price, 
                "Content Rating", Genres, "Last Updated", "Age Restriction") 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
path = './database/output/processed_googleplaystore.csv'
db.insert_values_apps(path, query)
db.test_query('Apps', True)