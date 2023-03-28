import pandas as pd
import numpy as np

from src.DataIngestor import DataIngestor
from src.DataPreprocessor import DataPreprocessor
from src.DataVisualizer import DataVisualizer

di = DataIngestor()

df = di.load_file('googleplaystore.csv')

df.loc[df['App'] == 'Life Made WI-Fi Touchscreen Photo Frame', ['Category', 'Rating', 'Reviews', 'Size', 'Installs', 'Type', 'Price', 'Content Rating', 'Genres', 'Last Updated', 'Current Ver', 'Android Ver']] = np.NaN, 1.9, '19', '3.0M', '1,000+', 'Free', '0', 'Everyone', np.NaN, 'February 11, 2018', '1.0.19', '4.0 and up'
df.loc[df['App'] == 'Life Made WI-Fi Touchscreen Photo Frame', ['Category', 'Genres']] = 'LIFESTYLE', 'Lifestyle'

dp = DataPreprocessor()

df = dp.pipeline(df)

df= di.load_file('processed_googleplaystore.csv')

sns_vis = DataVisualizer(library="seaborn")
plt_vis = DataVisualizer(library="matplotlib")

# 1-Find if there is a correlation between the price of the apps and the category (Teen, Everyone, Mature). 
sns_vis.scatter_plot(df, 'Price', '')











#2-find the sentiment of all apps using np files (negative words and positive words) and "afinn" lib 
#link for np files https://drive.google.com/drive/folders/1824UvFm8WBcOX_iiev0kNMrDu7aUARra?usp=share_link
#ask them to search about afinn lib 
#3-for paid apps only list the top 5 highest and lowest sentiment numbers with the name of the app and the app category 
#4-what is the best category according to sentiment values