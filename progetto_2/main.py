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

"""print(df)
print(df.dtypes)
print(df.isna().sum())"""

sns_vis = DataVisualizer("seaborn")


sns_vis = DataVisualizer(library="seaborn")
"""sns_vis.barh_by_grouping(df, column="Rating", group_by="Category", agg='sum')
sns_vis.scatter_plot(df, 'Installs', 'Reviews')
sns_vis.countplot(df, var='Category', hue='Type')"""
sns_vis.grouped_rating(df, ["Category", "Type"], "Rating")                          #average Rating devided in free and paid Apps for each Category
sns_vis.grouped_rating(df, "Category", "Rating")                                    #average Rating per Category
sns_vis.popularity_score(df)                                                        #top 10 Apps by Popularity (Rating*Installs)
sns_vis.rating_counter(df, "Rating", "Category")                                    #number of Apps in each Category for each Rating range
sns_vis.rating_counter(df, "Rating", "Type")                                        #number of Apps in each Type (free, paid) for each Rating range

"""plt_vis = DataVisualizer(library="matplotlib")
plt_vis.barh_by_grouping(df, column="Rating", group_by="Category", agg='sum')
plt_vis.scatter_plot(df, 'Installs', 'Reviews')
plt_vis.countplot(df, var='Category', hue='Type')"""