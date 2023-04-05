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
#print(negative_words)
#print(negative_words, positive_words, df)
df_reviews, df_sentiment = da.pipeline(df, n=negative_words, p=positive_words)
print(df_reviews, df_sentiment)