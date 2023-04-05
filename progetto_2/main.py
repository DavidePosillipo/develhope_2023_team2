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
df_reviews = di.load_file('database/raw/googleplaystore_user_reviews.csv', 'csv')
df_reviews = dp.pipeline_reviews(df_reviews) #data cleaning reviews file
di.save_file(df_reviews, 'database/output/processed_reviews.pkl', 'pickle')
df_reviews = di.load_file('database/output/processed_reviews.pkl', 'pickle')
negative_words = di.load_to_list('database/raw/n.xlsx', col=0, format='xlsx')
positive_words = di.load_to_list('database/raw/p.xlsx', col=0, format='xlsx')

df_reviews, df_sentiment, df_all = da.pipeline(df, df_reviews, n=negative_words, p=positive_words)

di.save_file(df_all, 'database/output/googleplaystore_sentiment.pkl', 'pickle')
dv.pipeline(df, df_all)
#di.load_image('png', library='seaborn')