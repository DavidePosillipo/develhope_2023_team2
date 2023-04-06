from src.DataIngestor import DataIngestor
from src.DataPreprocessor import DataPreprocessor
from src.DataVisualizer import DataVisualizer
from src.DataAnalyzer import DataAnalyzer

di = DataIngestor()
dp = DataPreprocessor()
dv = DataVisualizer("seaborn") # Compatibile con matplotlib e seaborn
da = DataAnalyzer() # Any list of words formatted in one column

# Carica il file csv contenente informazioni sulle app di Google Play Store
df = di.load_file('database/raw/googleplaystore.csv', 'csv')

#Applica una pipeline di pulizia dei dati al dataframe
df = dp.pipeline(df) 

# Salva il dataframe elaborato in un file pickle
di.save_file(df, 'database/output/processed_googleplaystore.pkl', 'pickle')

# Carica il file csv contenente le recensioni degli utenti delle app
df= di.load_file('database/output/processed_googleplaystore.csv', 'csv')

# Carica il file creato
df_reviews = di.load_file('database/raw/googleplaystore_user_reviews.csv', 'csv')

# Applica una pipeline di pulizia dei dati per le recensioni
df_reviews = dp.pipeline_reviews(df_reviews) 

# Salva il dataframe elaborato delle recensioni in un file pickle
di.save_file(df_reviews, 'database/output/processed_reviews.pkl', 'pickle')

# Carica il file creato
df_reviews = di.load_file('database/output/processed_reviews.pkl', 'pickle')

# # Carica i file excel contenenti le liste di parole negative e positive
negative_words = di.load_to_list('database/raw/n.xlsx', col=0, format='xlsx')
positive_words = di.load_to_list('database/raw/p.xlsx', col=0, format='xlsx')

df_reviews, df_sentiment, df_all = da.pipeline(df, df_reviews, n=negative_words, p=positive_words)

di.save_file(df_all, 'database/output/googleplaystore_sentiment.pkl', 'pickle')
dv.pipeline(df, df_all)