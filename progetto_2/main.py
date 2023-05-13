from src.DataIngestor import DataIngestor
from src.DataPreprocessor import DataPreprocessor
from src.DataVisualizer import DataVisualizer
from src.DataAnalyzer import DataAnalyzer
from src.DataIngestor import DataIngestor
from src.DB_Handler import DB_Handler


di = DataIngestor()
dp = DataPreprocessor()
dv = DataVisualizer(library="seaborn", style='darkgrid', show=False, save=True) 
da = DataAnalyzer()  # Any list of words formatted in one column
db = DB_Handler(database = 'postgres', user = 'postgres', password='postgres', host='localhost', database_name = 'googleplaystore')

# Uploads csv file containing data about Google Play Store apps
df = di.load_file('database/raw/googleplaystore.csv')

# Applies a data cleaning pipeline to the dataframe (DataPreprocessor)
df = dp.pipeline(df) 

# Saves the processed dataframe in a pickle file
#di.save_file(df, 'database/output/processed_googleplaystore.csv')

# Loads the csv file containing app user reviews
#df = di.load_file('database/output/processed_googleplaystore.csv')

# CATEGORY TABLE
table_query = """
    CREATE TABLE categories (
        "Category ID" SERIAL PRIMARY KEY,
        Name VARCHAR(256) NOT NULL
    )
"""
db.create_table(table_query, 'categories')
insert_query = """
    INSERT INTO categories (Name)
    SELECT %s
    WHERE NOT EXISTS (
        SELECT 1 FROM categories WHERE Name = %s
    )
"""
db.insert_values_categories('./database/output/processed_googleplaystore.csv', insert_query)
# APP TABLE
# Define the table creation query
table_query = """
    CREATE TABLE apps (
        "App ID" SERIAL PRIMARY KEY,
        name VARCHAR(256) NOT NULL
    )
"""
# Create the table
db.create_table(table_query, 'apps')
# Define the query for inserting data into the table
insert_query = """
    INSERT INTO apps (name)
    SELECT %s
    WHERE NOT EXISTS (
        SELECT 1 FROM apps WHERE name = %s
    )
"""
# Insert data from CSV file into the table
db.insert_values_apps('./database/output/processed_googleplaystore.csv', insert_query)

# APPS TABLE
table_query = """
    CREATE TABLE Main (
    "Index" INT,
    "App ID" INT REFERENCES apps("App ID"),
    "Category ID" INT REFERENCES categories("Category ID"),
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
db.create_table(table_query, 'Main')
query = """INSERT INTO Main (
                "Index", "App ID", "Category ID", Rating, Reviews, Size, Installs, Type, Price, 
                "Content Rating", Genres, "Last Updated", "Age Restriction") 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
path = './database/output/processed_googleplaystore.csv'
db.insert_values_main(path, query)

#!!! PAOLO !!!
df = db.read_table('Main') 
df.head(10)
# Loads the created file
df_reviews = di.load_file('database/raw/googleplaystore_user_reviews.csv')

# Applies a data cleaning pipeline for reviews (DataPreprocessor)
df_reviews = dp.pipeline_reviews(df_reviews) 

# Saves the processed review dataframe to a pickle file
#di.save_file(df_reviews, 'database/output/processed_reviews.pkl')

# Loads the created file
#df_reviews = di.load_file('database/output/processed_reviews.pkl')

# Loads excel files containing negative and positive word lists
negative_words = di.load_to_list('database/raw/n.xlsx', col=0)
positive_words = di.load_to_list('database/raw/p.xlsx', col=0)

# Applies a pipeline for sentiment analysis (DataIngestor)
df_reviews, df_sentiment, df_all = da.pipeline(df, df_reviews, n_words= negative_words, p_words= positive_words)

# Saves the processed sentiment dataframe in a pickle file
di.save_file(df_all, 'database/output/googleplaystore_sentiment.pkl')

# Applies the data visualization pipeline (DataVisualizer)
df_all = di.load_file('database/output/googleplaystore_sentiment.pkl')
dv.pipeline(df, df_all)

# Loads PNG graphs based on library
#di.load_image('png', library='seaborn')
