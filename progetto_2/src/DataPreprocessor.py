import math
from typing import Literal
import numpy as np
import pandas as pd

class DataPreprocessor:

    def __init__(self):
        pass

    def pipeline(self, df, copy: bool = False):
        if copy:
            df = df.copy()
        df['Last Updated'] = pd.to_datetime(df['Last Updated'])
        self.drop_outdated(df)
        df['Category'] = pd.Categorical(df['Category'])
        self.to_bytes(df, 'Size')
        self.estimate_size(df)
        self.genre_cleaning(df)
        df['Genres'] = pd.Categorical(df['Genres'])
        df['Size'] = df['Size'].astype('Int32')
        self.installs_cleaning(df)
        self.price(df)
        self.rating_fillna(df)
        self.reviews_to_int(df)
        self.drop_na_values(df)
        df.drop(columns=['Current Ver', 'Android Ver'], inplace=True)

        return df

    def drop_outdated(self, df):
        # Drop outdated data by sorting by date and keeping the last entry
        df.sort_values(by='Last Updated', inplace=True)

        df.drop_duplicates(
            subset = ['App', 'Rating', 'Size', 'Installs', 'Type',
                'Price', 'Content Rating', 'Genres', 'Current Ver',
                'Android Ver'], # Ignoring 'Reviews', 'Category', and 'Last Updated'
            keep = 'last', # The last entry is also the most recent one 
            inplace = True)

    def item_to_bytes(self, item):
        # Convert the item into multiplying the value for the corresponding
        # multiplier based on the simbol. If the value in not recognised it's
        # return without alteration
        if item.isdigit():
            return int(item)
        elif item[-1] == 'k':
            return int(float(item[:-1]) * 1_024) 
        elif item[-1] == 'M':
            return int(float(item[:-1]) * 1_024 * 1_024) 
        else:
            return item

    def to_bytes(self, df, column):
        # Convert the entire column to bytes
        df[column] = df[column].apply(self.item_to_bytes)

    def estimate_size(self, df):
        # Estimate the average size of every category based on the avilable data and use it to fill the missing value.
        # Can handle np.nan and 'Varies with device'
        categories_mean_size = {}

        for category in df['Category'].unique():
            category_mean = df.loc[(df['Category'] == category) & (df['Size'] != 'Varies with device'), 'Size'].mean()
            categories_mean_size[category] = math.floor(category_mean)

        for category in df['Category'].unique():
            df.loc[(df['Category'] == category) & (df['Size'] == 'Varies with device'), 'Size'] = categories_mean_size[category]
            df.loc[df['Size'].isna(), 'Size'] = categories_mean_size[category]

    def genre_cleaning(self, df):
        # If the genre is composed by two genres, keep only the first one
        df['Genres'] = df['Genres'].str.split(';', expand=True)[0]

    def size_to_int(self, df):
        # Casting size column in integer type 
        df['Size'] = df['Size'].astype('Int32')

    def installs_cleaning(self, df):
        # Extracting numerical values from original column and then concatenating them back together leaving out
        # non numerical digits, and finally casting into integer type 
        df['Installs'] = df['Installs'].astype('str').str.extractall('(\d+)').unstack().fillna('').sum(axis=1).astype(int)
        
    def price(self, df):
        # Replacing $ sign with empty and casting values in float type
        df['Price'] = np.array([value.replace('$', '') for value in df['Price']]).astype(float)
        
    def rating_fillna(self, df):
        ## replacing nan values with mean of the column 
        mean = df['Rating'].dropna().mean()
        df['Rating'].fillna(mean, inplace=True)
        
    def reviews_to_int(self, df):
        # Casting reviews column in integer type 
        n=0
        except_ls=[]
        for i in df['Reviews']:
            try:
                int_value=int(i)
                df['Reviews'].values[n]=int_value
            except:
                except_ls.append([n,i])
            n+=1
        df['Reviews']=df['Reviews'].astype({'Reviews':'Int32'}, copy=False)
        
    def drop_na_values(self, df):
        # Dropping Nan value(s) left
        if df.isna().sum().any()>0:
            df.dropna(inplace=True)