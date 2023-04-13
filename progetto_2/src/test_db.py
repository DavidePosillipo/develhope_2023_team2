import pandas as pd
from sqlalchemy import create_engine

class DatabaseCreator:
    def __init__(self, database_type, username, password, hostname_or_ip, port, database_name):
        db_url = f'{database_type}://{username}:{password}@{hostname_or_ip}:{port}/{database_name}'
        self.engine = create_engine(db_url)

    def create_table(self, table_name, columns, primary_key=None):
        with self.engine.connect() as conn:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join(columns)})")
            # Add primary key constraint
            if primary_key:
                conn.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key})")

    def copy_csv_to_table(self, csv_path, table_name):
        try:
            df = pd.read_csv(csv_path)
            with self.engine.connect() as conn:
                df.to_sql(table_name, con=conn, if_exists='append', index=False)
        except Exception as e:
            print(f"Error copying CSV to table: {e}")
    
    def add_foreign_key(self, table_name, column_name, foreign_table_name, foreign_column_name):
        with self.engine.connect() as conn:
            conn.execute(f"ALTER TABLE {table_name} ADD FOREIGN KEY ({column_name}) REFERENCES {foreign_table_name}({foreign_column_name})")

# Example usage
db_url = f'postgresql://username:password@localhost:5432/mydatabase'
csv_path = './database/output/processed_googleplaystore.csv'
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
    'Last_Updated TIMESTAMP',
    'Age_Restriction INT']
primary_key = 'App'

db_creator = DatabaseCreator('postgresql', 'postgres', 'c', 'localhost', '5432', 'progetto_team2')
db_creator.create_table(table_name, columns)
db_creator.copy_csv_to_table(csv_path, table_name)
#db_creator.add_foreign_key(table_name, 'Category', 'foreign_table', 'id')
