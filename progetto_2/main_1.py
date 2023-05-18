from src.DataIngestor import DataIngestor
from src.DataPreprocessor import DataPreprocessor
from src.DataVisualizer import DataVisualizer
from src.DataAnalyzer import DataAnalyzer
from src.DataIngestor import DataIngestor
from src.DB_Handler import DB_Handler
from src.db_handler_sql_pd import DB_Handler_cloud


di = DataIngestor()
dp = DataPreprocessor()
dv_seaborn = DataVisualizer(library="seaborn", style='darkgrid', show=False, save=True) 
dv_matplotlib = DataVisualizer(library='matplotlib', style='darkgrid', show=False, save=True)
da = DataAnalyzer()  # Any list of words formatted in one column
db = DB_Handler(database = 'postgres', user = 'postgres', password='c', host='localhost', database_name = 'googleplaystore')

database = 'yhpzbiwk'
user = 'yhpzbiwk'
password = 'gNxA8nZrA_vFYCAQ143gVn-HRg6XTF1-'
host = 'snuffleupagus.db.elephantsql.com'
port = 5432

database_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
file_path = "./"

db_cloud = DB_Handler_cloud(database_url, file_path)


# Uploads csv file containing data about Google Play Store apps
df = di.load_file('database/raw/googleplaystore.csv')

# Applies a data cleaning pipeline to the dataframe (DataPreprocessor)
df = dp.pipeline(df) 

# Saves the processed dataframe in a pickle file
di.save_file(df, 'database/output/processed_googleplaystore.csv')

# Loads the csv file containing app user reviews
df = di.load_file('database/output/processed_googleplaystore.csv')

# Loads the created file
df_reviews = di.load_file('database/raw/googleplaystore_user_reviews.csv')

# Applies a data cleaning pipeline for reviews (DataPreprocessor)
df_reviews = dp.pipeline_reviews(df_reviews) 

# Saves the processed review dataframe to a pickle file
di.save_file(df_reviews, 'database/output/processed_reviews.pkl')

# Loads the created file
df_reviews = di.load_file('database/output/processed_reviews.pkl')

# Loads excel files containing negative and positive word lists
negative_words = di.load_to_list('database/raw/n.xlsx', col=0)
positive_words = di.load_to_list('database/raw/p.xlsx', col=0)

# Applies a pipeline for sentiment analysis (DataIngestor)
df_reviews, df_sentiment, df_all = da.pipeline(df, df_reviews, n_words= negative_words, p_words= positive_words)

# Saves the processed sentiment dataframe in a pickle file
di.save_file(df_all, 'database/output/googleplaystore_sentiment.pkl')

# Applies the data visualization pipeline (DataVisualizer)
df_all = di.load_file('database/output/googleplaystore_sentiment.pkl')
#dv_seaborn.pipeline(df, df_all)
#dv_matplotlib.pipeline(df, df_all)
# Loads PNG graphs based on library
#di.load_image('png', library='seaborn')

'''db.run_data_pipeline()

df = db.read_table('Main') 
df_categories = db.read_table('categories')
df_apps = db.read_table('apps')
print(df.head(3), df_categories.head(3), df_apps.head(3))'''

database = 'yhpzbiwk'
user = 'yhpzbiwk'
password = 'gNxA8nZrA_vFYCAQ143gVn-HRg6XTF1-'
host = 'snuffleupagus.db.elephantsql.com'
port = 5432

database_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
file_path = "./"

db_cloud = DB_Handler_cloud(database_url, file_path)
# Run db_cloud pipeline
db_cloud.run_data_pipeline()