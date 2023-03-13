import math
from typing import Literal

import pandas as pd

class DataPreprocessor:

    def __init__(self):
        pass

    def pipeline(self, df):
        df = df.copy()
        self.to_datetime(df, 'Last Updated')
        self.sort_values(df)
        self.drop_duplicates(df)
        self.to_bytes(df, 'Size')
        self.estimate_size(df)
        self.installs_cleaning(df)
        return df

    def to_datetime(self, df, column):
        df[column] = pd.to_datetime(df[column])
        
    def sort_values(self, df, by: str|list =['Last Updated']):
        df.sort_values(by=by, inplace=True)

    def drop_duplicates(self, df, keep: Literal['first', 'last', False]='last', inplace: bool=True):
        df.drop_duplicates(
            subset = ['App', 'Rating', 'Size', 'Installs', 'Type',
                'Price', 'Content Rating', 'Genres', 'Current Ver',
                'Android Ver'], # Ignoring 'Reviews', 'Category', and 'Last Updated'
            keep = keep, # The last entry is also the most recent one 
            inplace = inplace)
        df.sort_index(inplace= True, ignore_index= True)

    def item_to_bytes(self, item):
        if item.isdigit():
            return int(item)
        elif item[-1] == 'k':
            return int(float(item[:-1]) * 1_024) 
        elif item[-1] == 'M':
            return int(float(item[:-1]) * 1_024 * 1_024) 
        else:
            return item

    def to_bytes(self, df, column):
        df[column] = df[column].apply(self.item_to_bytes)

    def estimate_size(self, df):
        categories_mean_size = {}

        for category in df['Category'].unique():
            category_mean = df.loc[(df['Category'] == category) & (df['Size'] != 'Varies with device'), 'Size'].mean()
            categories_mean_size[category] = math.floor(category_mean)

        for category in df['Category'].unique():
            df.loc[(df['Category'] == category) & (df['Size'] == 'Varies with device'), 'Size'] = categories_mean_size[category]

    def installs_cleaning(self, df):
        for i in range(len(df)):
            if df.Installs[i] == "0+":
                df.Installs[i] = "1"
            if "," in df.Installs[i]:
                df.Installs[i] = "".join(list(x for x in df.Installs[i][:len(df.Installs[i]) -1] if x != ","))
            if "+" in df.Installs[i]:
                df.Installs[i] = df.Installs[i][:len(df.Installs[i]) -1]

        df.Installs = df.Installs.astype(int)

        return df
     

    def rating_fillna (self, df):
        ## replacing nan values with mean of the column 
        s=sum(df['Rating'].dropna())
        n=df['Rating'].shape[0]
        rating_mean=s/n
        print(rating_mean)
        df['Rating'].fillna(rating_mean, inplace=True)
        
    def reviews_to_int():
        n=0
        except_ls=[]
        for i in data['Reviews']:
            try:
                int_value=int(i)
                data['Reviews'].values[n]=int_value
            except:
                except_ls.append([n,i])
            n+=1
        
    