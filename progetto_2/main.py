import pandas as pd
import numpy as np

from src.DataIngestor import DataIngestor
from src.DataPreprocessor import DataPreprocessor
from src.DataVisualizer import DataVisualizer
from src.DataAnalyzer import DataAnalyzer

di = DataIngestor()
dp = DataPreprocessor()
dv = DataVisualizer("seaborn")
da = DataAnalyzer()

df = di.load_file('database/raw/googleplaystore.csv', 'csv')
df = dp.pipeline(df) # Data cleaning App file
di.save_file(df, 'database/output/processed_googleplaystore.pkl', 'pickle')
df= di.load_file('database/output/processed_googleplaystore.csv', 'csv')
#dv.pipeline(df)
df = di.load_file('database/raw/googleplaystore_user_reviews.csv', 'csv')
df = dp.pipeline_reviews(df) #data cleaning reviews file
di.save_file(df, 'database/output/processed_reviews.pkl', 'pickle')
df = di.load_file('database/output/processed_reviews.pkl', 'pickle')
negative_words = di.load_to_list('database/raw/n.xlsx', col=0, format='xlsx')
positive_words = di.load_to_list('database/raw/p.xlsx', col=0, format='xlsx')
#print(negative_words, positive_words, df)
df_n = da.sentiment_score(df)
di.save_file(df_n, 'database/output/processed_reviews.pkl', 'pickle')
df = di.load_file('database/output/processed_reviews.pkl', 'pickle')
df_sentiment_score = da.pipeline(df)
di.save_file(df_sentiment_score, 'database/output/sentiment_score.pkl', 'pickle')
df_googleplaystore = di.load_file('database/output/processed_googleplaystore.pkl', 'pickle')
df_sentiment_score = di.load_file('database/output/sentiment_score.pkl', 'pickle')
df = pd.merge(df_googleplaystore, df_sentiment_score, on='App')
di.save_file(df, 'database/output/merged_dataframe_apps_sentimentscore.pkl', 'pickle')
df = di.load_file('database/output/merged_dataframe_apps_sentimentscore.pkl', 'pickle')
print(df)