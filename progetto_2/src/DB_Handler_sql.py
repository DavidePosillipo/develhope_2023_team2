from sqlalchemy import create_engine

class DB_Handler:
    def __init__(self, database_url):
        self.database_url = database_url
        self.engine = create_engine(self.database_url)
    
    def create_database(self, database_name):
        # Create a connection to the default database (e.g., "postgres" for PostgreSQL)
        default_engine = create_engine(self.database_url)
        default_connection = default_engine.connect()
        
        # Create the new database
        default_connection.execute(f"CREATE DATABASE {database_name}")
        
        # Close the connection to the default database
        default_connection.close()
        
        # Update the database URL to include the new database name
        self.database_url = self.database_url.rsplit('/', 1)[0] + '/' + database_name
        
        # Create a new engine using the updated database URL
        self.engine = create_engine(self.database_url)
        
        print(f"Database '{database_name}' created.")

# Instantiate the DB_Handler class with the database URL
db_handler = DB_Handler('postgresql://postgres:c@localhost:5432/postgres')

# Create a new database named 'mydatabase'
db_handler.create_database('SQL_alchemy_db_prova')
