import math
import numpy as np
import pandas as pd

class DataPreprocessor:

    def __init__(self):
        pass

    def pipeline(self, df, copy: bool = False):
        df.loc[df['App'] == 'Life Made WI-Fi Touchscreen Photo Frame', ['Category', 'Rating', 'Reviews', 'Size', 'Installs', 'Type', 'Price', 'Content Rating', 'Genres', 'Last Updated', 'Current Ver', 'Android Ver']] = np.NaN, 1.9, '19', '3.0M', '1,000+', 'Free', '0', 'Everyone', np.NaN, 'February 11, 2018', '1.0.19', '4.0 and up'
        df.loc[df['App'] == 'Life Made WI-Fi Touchscreen Photo Frame', ['Category', 'Genres']] = 'LIFESTYLE', 'Lifestyle'
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
        self.transform_age(df, 'Content Rating')
        self.rename_categories(df)
        self.comma_replacer(df, 'App')
        self.quotatione_marks_replacer(df, 'App')
        self.drop_duplicates(df, 'App')
        return df
    
    
    def pipeline_reviews(self, df):                                                                             
        df = df.dropna()                                                                                        
        df = df[['App', 'Translated_Review']]                                                                   
        return df                                                                                               
      
    def drop_outdated(self, df):                                                                               
        
        df.sort_values(by='Last Updated', inplace=True)                                                         
        df.drop_duplicates(
            subset = ['App', 'Rating', 'Size', 'Installs', 'Type',
                'Price', 'Content Rating', 'Genres', 'Current Ver',
                'Android Ver'], 
            keep = 'last', 
            inplace = True)
        
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
            df.loc[df['Size'].isna(), 'Size'] = categories_mean_size[category]

    def genre_cleaning(self, df):                                                                               
        df['Genres'] = df['Genres'].str.split(';', expand=True)[0]                                              

    def size_to_int(self, df):                                                                                  
        df['Size'] = df['Size'].astype('Int32')

    def installs_cleaning(self, df):                                                                                                        
        df['Installs'] = df['Installs'].astype('str').str.extractall('(\d+)').unstack().fillna('').sum(axis=1).astype(int)                 
        
    def price(self, df):                                                                                        
        df['Price'] = np.array([value.replace('$', '') for value in df['Price']]).astype(float)                 

    def rating_fillna(self, df):                                                                                
        mean = round(df['Rating'].dropna().mean(), 1)                                                           
        df['Rating'].fillna(mean, inplace=True)

    def reviews_to_int(self, df):                                                                               
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
        if df.isna().sum().any()>0:
            df.dropna(inplace=True)

    def transform_age(self, df, column):                                                                        
        age_map = {                                                                                             
            "Everyone" : 0,                                                                                     
            "Everyone 10+": 10,
            "Teen": 13,
            "Mature 17+": 17,
            "Adults only 18+": 18, 
            "Unrated" : 0
        }
        df['Age Restriction'] = df[column].map(age_map).fillna(0).astype(int)
        df.loc[df['Content Rating'] == 'Unrated', 'Content Rating'] = 'Everyone'
       
    def drop_unnamed(self, df):                                                                                 
        df = df.drop(columns=['Unnamed: 0'])

    def rename_categories(self, df):                                                                            
        df['Category'] = df['Category'].str.replace('_', ' ').str.capitalize()

    def comma_replacer(self, df, col):                                                                          
        df[col] = df[col].str.replace(',', '')

    def quotatione_marks_replacer(self, df, col):                                                               
        df[col] = df[col].str.replace("'", '')
        df[col] = df[col].str.replace('"', '')   

    def drop_duplicates(self, df, col):
        df = df.drop_duplicates(subset=col, keep='first')                                                    