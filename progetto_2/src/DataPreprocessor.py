import math
import numpy as np
import pandas as pd

class DataPreprocessor:

    def __init__(self):
        pass

    def pipeline(self, df, copy: bool = False):
        if copy:
            df = df.copy()
        self.format_column_names(df)
        df.loc[df['app'] == 'Life Made WI-Fi Touchscreen Photo Frame', ['category', 'rating', 'reviews', 'size', 'installs', 'type', 'price', 'content_rating', 'genres', 'last_updated', 'current_ver', 'android_ver']] = np.NaN, 1.9, '19', '3.0M', '1,000+', 'Free', '0', 'Everyone', np.NaN, 'February 11, 2018', '1.0.19', '4.0 and up'
        df.loc[df['app'] == 'Life Made WI-Fi Touchscreen Photo Frame', ['category', 'genres']] = 'LIFESTYLE', 'Lifestyle'
        df['last_updated'] = pd.to_datetime(df['last_updated'])                                                 #       Def Pipeline:
        self.drop_outdated(df)                                                                                  # - - Runs DataPreprocessor pipeline's methods
        df['category'] = pd.Categorical(df['category'])
        self.to_bytes(df, 'size')
        self.estimate_size(df)
        self.genre_cleaning(df)
        df['genres'] = pd.Categorical(df['genres'])
        df['size'] = df['size'].astype('Int32')
        self.installs_cleaning(df)
        self.price(df)
        self.rating_fillna(df)
        self.reviews_to_int(df)
        self.drop_na_values(df)
        df.drop(columns=['current_ver', 'android_ver'], inplace=True)
        self.transform_age(df, 'content_rating')
        self.rename_categories(df)
        self.comma_replacer(df, 'app')
        self.quotatione_marks_replacer(df, 'app')
        self.drop_duplicates(df, 'app')
        return df
    
    
    def pipeline_reviews(self, df):
        self.format_column_names(df)                                                                                 #       Def Pipeline_Reviews:
        df = df.dropna()                                                                                        # - Drops rows with missing values
        df = df[['app', 'translated_review']]                                                                   # - Selects 'App' and 'Translated_Review' columns
        return df                                                                                               # - Returns the updated DataFrame
    
    def format_column_names(self, df):
        df.columns = df.columns.str.lower()
        df.columns = df.columns.str.replace(' ', '_')
    
    def drop_outdated(self, df):                                                                                #       Def Drop_Outdated:
                                                                                                                # - Sorts the DataFrame by 'Last Updated'
        df.sort_values(by='last_updated', inplace=True)                                                         # - Drops duplicate entries, keeping the most recent one
                                                                                                                # - Ignores 'Reviews', 'Category', and 'Last Updated'
        df.drop_duplicates(
            subset = ['app', 'rating', 'size', 'installs', 'type',
                'price', 'content_rating', 'genres', 'current_ver',
                'android_ver'], 
            keep = 'last', 
            inplace = True)

    def item_to_bytes(self, item):                                                                              #       Def Item_To_Bytes:
                                                                                                                # - Converts an item to bytes based on its unit (kB, MB) using the function To_Bytes
        if item.isdigit():                                                                                      # - Returns the converted value or the original item if not a size unit
            return int(item)
        elif item[-1] == 'k':
            return int(float(item[:-1]) * 1_024) 
        elif item[-1] == 'M':
            return int(float(item[:-1]) * 1_024 * 1_024) 
        else:
            return item



    def to_bytes(self, df, column):                                                                             #       Def To_Bytes
                                                                                                                # - Applies the 'item_to_bytes' function to the specified column
        df[column] = df[column].apply(self.item_to_bytes)                                                       # - Updates the DataFrame with the converted values



    def estimate_size(self, df):                                                                                                            #       Def Estimate Size                                                                                                                                   
                                                                                                                                            # - Estimates the average size for each category
        categories_mean_size = {}                                                                                                           # - Fills missing size values with the category average

        for category in df['category'].unique():
            category_mean = df.loc[(df['category'] == category) & (df['size'] != 'Varies with device'), 'size'].mean()
            categories_mean_size[category] = math.floor(category_mean)

        for category in df['category'].unique():
            df.loc[(df['category'] == category) & (df['size'] == 'Varies with device'), 'size'] = categories_mean_size[category]
            df.loc[df['size'].isna(), 'size'] = categories_mean_size[category]



    def genre_cleaning(self, df):                                                                               #       Def Genre_Cleaning
                                                                                                                # - Cleans the 'Genres' column by keeping only the first genre if multiple are present
        df['genres'] = df['genres'].str.split(';', expand=True)[0]                                              



    def size_to_int(self, df):                                                                                  #       Def Size_to_Int
                                                                                                                # - Converts 'Size' column to integer type.                                                                   
        df['size'] = df['size'].astype('Int32')



    def installs_cleaning(self, df):                                                                                                        #       Def Installs_Cleaning:
                                                                                                                                            # - Cleans the 'Installs' column by extracting numerical values
        df['installs'] = df['installs'].astype('str').str.extractall('(\d+)').unstack().fillna('').sum(axis=1).astype(int)                  # - Converts the column to an integer data type
        


    def price(self, df):                                                                                        #       Def Price:
                                                                                                                # - Removes the '$' sign from the 'Price' column
        df['price'] = np.array([value.replace('$', '') for value in df['price']]).astype(float)                 # - Converts the column to a float data type
        
 

    def rating_fillna(self, df):                                                                                #       Def Rating_Fillna:
                                                                                                                # - Replaces NaN values in the 'Rating' column with the mean rating         
        mean = round(df['rating'].dropna().mean(), 1)                                                           
        df['rating'].fillna(mean, inplace=True)
        


    def reviews_to_int(self, df):                                                                               #       Def Reviews_to_Int:
                                                                                                                # - Converts the 'Reviews' column to an integer data type
        n=0                                                                                                     # - Handles non-integer values gracefully
        except_ls=[]
        for i in df['reviews']:
            try:
                int_value=int(i)
                df['reviews'].values[n]=int_value
            except:
                except_ls.append([n,i])
            n+=1
        df['reviews']=df['reviews'].astype({'reviews':'Int32'}, copy=False)
        


    def drop_na_values(self, df):                                                                               #       Def Drop_na_Values:                                                                           
                                                                                                                # - Drops any remaining rows with NaN values
        if df.isna().sum().any()>0:
            df.dropna(inplace=True)



    def transform_age(self, df, column):                                                                        #       Def Transform_Age:     
                                                                                                                # - Maps age restriction strings to corresponding integer values
        age_map = {                                                                                             # - Creates a new 'Age Restriction' column with the mapped values
            "Everyone" : 0,                                                                                     # - Updates 'Content Rating' for 'Unrated' entries to 'Everyone'
            "Everyone 10+": 10,
            "Teen": 13,
            "Mature 17+": 17,
            "Adults only 18+": 18, 
            "Unrated" : 0
        }
        df['age_restriction'] = df[column].map(age_map).fillna(0).astype(int)
        df.loc[df['content_rating'] == 'Unrated', 'content_rating'] = 'Everyone'
    

    
    def drop_unnamed(self, df):                                                                                 #       Def Drop_Unnamed:
                                                                                                                # - Drops the 'Unnamed: 0' column from the DataFrame
        df = df.drop(columns=['Unnamed: 0'])

    def rename_categories(self, df):                                                                            #       Def Rename_Categories:
                                                                                                                # - Rename Categories in a nicer fashion
        df['category'] = df['category'].str.replace('_', ' ').str.capitalize()

    def comma_replacer(self, df, col):                                                                          #       Def Comma_Replacer:
                                                                                                                # - Replace comma with empty string
        df[col] = df[col].str.replace(',', '')

    def quotatione_marks_replacer(self, df, col):                                                               #       Def quotatione_marks_replacer:
                                                                                                                # - Replace quotation marks with empty string
        df[col] = df[col].str.replace("'", '')
        df[col] = df[col].str.replace('"', '')   

    def drop_duplicates(self, df, col):

        df = df.drop_duplicates(subset=col, keep='first')                                                    