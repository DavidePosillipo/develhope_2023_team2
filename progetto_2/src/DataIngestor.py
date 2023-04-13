import pandas as pd
from PIL import Image
import os
from typing import Literal
import psycopg2
import csv

class DataIngestor:

    def __init__(self):
        pass


    def load_file(self, path):                                              #     Def Load_File:
         
        format = path.rsplit(".")[1]
                                                                                    # - Reads a file from the specified path
        if format == 'pkl':                                                      # - Supports loading files in 'pickle', 'csv', and 'xlsx' formats
            return pd.read_pickle(path)                                             # - Returns a DataFrame or an error message for unsupported formats
        elif format == 'csv':                     
            return pd.read_csv(path)
        elif format == 'xlsx':
            return pd.read_excel(path)
        else:
            return 'Apoligies, but this format has not been implemented yet.'
        
        
        
    def save_file(self, df, path):                                          # -     Def Save_File:
         
        format = path.rsplit(".")[1] 
                                                                                    # - Saves a DataFrame to the specified path 
        if format == 'pkl':                                                      # - Supports saving files in 'pickle', 'csv', and 'xlsx' formats
            return df.to_pickle(path)                                               # - Returns an error message for unsupported formats
        elif format == 'csv':                     
            return df.to_csv(path)
        elif format == 'xlsx':
            return df.to_excel(path)
        else:
            return 'Apoligies, but this format has not been implemented yet.'
        
        
        
    def load_to_list(self, path, col):                                      #       Def Load_to_List:
        
        format = path.rsplit(".")[1] 
                                                                                    # - Reads a file from the specified path ('pickle', 'csv', and 'xlsx' formats)
        if format == 'pkl':                                                      # - Extracts a specified column from the DataFrame
            df = pd.read_pickle(path)                                               # - Returns the column values as a list
            return df.iloc[:, col].tolist()                                        # - Returns an error message for unsupported formats
        elif format == 'csv':                                                       
            df = pd.read_csv(path)               
            return df.iloc[:, col].tolist()     
        elif format == 'xlsx':
            df = pd.read_excel(path).reset_index(drop= True)
            return df.iloc[:, col].tolist() 
        else:
            return 'Apoligies, but this format has not been implemented yet.'
        
        
        
    def load_image(self, format, library: Literal["seaborn", "matplotlib"]):        #       Def Load_Image
                                                                                    # - Loads and displays images from a specified directory
        if format == 'png':                                                         # - Filters images (png format) based on the specified library
            directory = './database/output/graphs'                                  # - Returns an error message for unsupported formats
            for filename in os.listdir(directory):                              
                if library == 'seaborn' and 'sns' not in filename:
                    continue
                if library == 'matplotlib' and 'mat' not in filename:
                    continue
            
                filepath = os.path.join(directory, filename)
                img = Image.open(filepath)
                img.show()
        else:
            return 'Apologies, but this format has not been implemented yet.'

    def create_db(self, db, user, password, host, path, table, columns):
        # Open the input and output CSV files
        with open(path, 'r') as in_file, open('output.csv', 'w', newline='') as out_file:
            # Create a CSV reader and writer
            reader = csv.reader(in_file)
            writer = csv.writer(out_file)

            # Read the header row
            header_row = next(reader)

            # Add the Index column to the header row
            header_row.insert(0, 'Index')

            # Write the updated header row to the output file
            writer.writerow(header_row)

            # Write the remaining rows to the output file
            index = 1
            for row in reader:
                # Add the Index value to the beginning of each row
                row.insert(0, index)
                index += 1

                # Write the updated row to the output file
                writer.writerow(row)
        # Connect to the database
        conn = psycopg2.connect(
            host=host,
            database=db,
            user=user,
            password=password
        )

        # Open a cursor to perform database operations
        cur = conn.cursor()

        # Check if the table already exists and drop it if it does
        cur.execute(f"DROP TABLE IF EXISTS {table}")

        # Use the CREATE TABLE command to create the table
        create_table_query = f"""CREATE TABLE {table} (
                                    Index INT,
                                    App VARCHAR(256),
                                    Category VARCHAR(256),
                                    Rating FLOAT(50),
                                    Reviews INT,
                                    Size INT,
                                    Installs INT,
                                    Type VARCHAR(15),
                                    Price FLOAT,
                                    Content_Rating VARCHAR(30),
                                    Genres VARCHAR(50),
                                    Last_Updated TIMESTAMP,
                                    Age_Restriction INT,
                                    PRIMARY KEY (Index)
                                );"""
        cur.execute(create_table_query)

         # Use the COPY command to insert the data from the CSV file into the table
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row
            for row in reader:
                cur.execute(f"INSERT INTO {table} ({', '.join(columns[1:])}) VALUES ({', '.join(['%s'] * len(columns[1:]) )})", row[1:])
        # Try except e printare la riga che non riesce a decodificare
        #Prova con la libreria Pandas a leggere il file

        # Commit the changes to the database
        conn.commit()

        # Close the cursor and the database connection
        cur.close()
        conn.close()


    def create_categories_table(self, db, user, password, host):
        # Connect to the database
        conn = psycopg2.connect(
            host=host,
            database=db,
            user=user,
            password=password
        )

        # Open a cursor to perform database operations
        cur = conn.cursor()

        # Use the CREATE TABLE command to create the categories table
        create_categories_table_query = "CREATE TABLE categories (" \
                                        "Category VARCHAR(256) PRIMARY KEY);"
        cur.execute(create_categories_table_query)

        # Commit the changes to the database
        conn.commit()

        # Close the cursor and the database connection
        cur.close()
        conn.close()

