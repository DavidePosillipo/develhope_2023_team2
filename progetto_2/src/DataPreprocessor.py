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
        self.to_int32(df, 'Reviews')
        self.to_bytes(df, 'Size')
        self.estimate_size(df)

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

    def to_int32(self, df, column):
        df[column] = df[column].astype('Int32')

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
