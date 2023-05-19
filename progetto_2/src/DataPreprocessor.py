import math
import numpy as np
import pandas as pd

class DataPreprocessor:
    """
    DataPreprocessor class for preprocessing dataframes.

    Attributes:
        None.

    Methods:
        pipeline: Preprocesses the given dataframe.
        pipeline_reviews: Preprocesses the reviews dataframe.
        drop_outdated: Drops outdated app entries from the dataframe.
        item_to_bytes: Converts a string representation of a size to bytes.
        to_bytes: Converts the size values in the specified column to bytes.
        estimate_size: Estimates and replaces missing or 'Varies with device' size values.
        genre_cleaning: Cleans up the 'Genres' column by keeping only the primary genre.
        size_to_int: Converts the 'Size' column to integer type.
        installs_cleaning: Cleans the 'Installs' column by converting it to integer type.
        price: Cleans the 'Price' column by removing the dollar sign and converting it to float.
        rating_fillna: Fills missing values in the 'Rating' column with the mean rating.
        reviews_to_int: Converts the 'Reviews' column to integer values.
        drop_na_values: Drops rows with missing values from the DataFrame.
        transform_age: Transforms the 'Content Rating' column to an 'Age Restriction' column.
        drop_unnamed: Drops the 'Unnamed: 0' column from the DataFrame.
        rename_categories: Renames the categories in the 'Category' column.
        comma_replacer: Replaces commas in a column with an empty string.
        quotatione_marks_replacer: Replaces quotation marks in a column with an empty string.
        drop_duplicates: Drops duplicate rows based on a specific column.
    """
    def __init__(self):
        pass

    def pipeline(self, df, copy: bool = False):
        """
        Preprocesses the given dataframe.

        Args:
            df (pd.DataFrame): The input dataframe.
            copy (bool): Flag indicating whether to create a copy of the dataframe (default: False).

        Returns:
            pd.DataFrame: The preprocessed dataframe.
        """
        df = df.drop(df[df['App'] == 'Life Made WI-Fi Touchscreen Photo Frame'].index)
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
        """
        Preprocesses the reviews dataframe.

        Args:
            df (pd.DataFrame): The input reviews dataframe.

        Returns:
            pd.DataFrame: The preprocessed reviews dataframe.
        """                                                                        
        df = df.dropna()                                                                                        
        df = df[['App', 'Translated_Review']]                                                                   
        return df                                                                                               
      
    def drop_outdated(self, df):   
        """
        Drops outdated app entries from the dataframe.

        Args:
            df (pd.DataFrame): The input dataframe.

        Returns:
            None. The dataframe is modified in-place.
        """                                                                                   
        df.sort_values(by='Last Updated', inplace=True)                                                         
        df.drop_duplicates(
            subset = ['App', 'Rating', 'Size', 'Installs', 'Type',
                'Price', 'Content Rating', 'Genres', 'Current Ver',
                'Android Ver'], 
            keep = 'last', 
            inplace = True)
        
    def item_to_bytes(self, item):    
        """
        Converts a string representation of a size to bytes.

        Args:
            item (str): The input size value.

        Returns:
            int or str: The size converted to bytes if it can be converted,
                otherwise the original value is returned as is.
        """                                                                          
        if item.isdigit():                                                                                     
            return int(item)
        elif item[-1] == 'k':
            return int(float(item[:-1]) * 1_024) 
        elif item[-1] == 'M':
            return int(float(item[:-1]) * 1_024 * 1_024) 
        else:
            return item

    def to_bytes(self, df, column):   
        """
        Convert the size values in the specified column to bytes.

        Args:
            df (pandas.DataFrame): The DataFrame containing the data.
            column (str): The name of the column containing the size values.

        Returns:
            None. The conversion is applied directly to the DataFrame.
        """                                                                          
        df[column] = df[column].apply(self.item_to_bytes)                                                       

    def estimate_size(self, df):                                                                                                            
        """
        Estimate and replace missing or 'Varies with device' size values with the mean size per category.

        Args:
            df (pandas.DataFrame): The DataFrame containing the data.

        Returns:
            None. The size values are updated directly in the DataFrame.
        """
        categories_mean_size = {}                                                                                                           

        for category in df['Category'].unique():
            category_mean = df.loc[(df['Category'] == category) & (df['Size'] != 'Varies with device'), 'Size'].mean()
            categories_mean_size[category] = math.floor(category_mean)

        for category in df['Category'].unique():
            df.loc[(df['Category'] == category) & (df['Size'] == 'Varies with device'), 'Size'] = categories_mean_size[category]
            df.loc[df['Size'].isna(), 'Size'] = categories_mean_size[category]

    def genre_cleaning(self, df):     
        """
        Clean up the 'Genres' column by keeping only the primary genre.

        Args:
            df (pandas.DataFrame): The DataFrame containing the data.

        Returns:
            None. The 'Genres' column is updated directly in the DataFrame.
        """                                                                          
        df['Genres'] = df['Genres'].str.split(';', expand=True)[0]                                              

    def size_to_int(self, df):  
        """
        Convert the 'Size' column to integer type.

        Args:
            df (pandas.DataFrame): The DataFrame containing the data.

        Returns:
            None. The 'Size' column is updated directly in the DataFrame.
        """                                                                                
        df['Size'] = df['Size'].astype('Int32')

    def installs_cleaning(self, df):    
        """
        Clean the 'Installs' column by converting it to integer type.

        Args:
            df (pandas.DataFrame): The DataFrame containing the data.

        Returns:
            None. The 'Installs' column is updated directly in the DataFrame.
        """                                                                                                    
        df['Installs'] = df['Installs'].astype('str').str.extractall('(\d+)').unstack().fillna('').sum(axis=1).astype(int)                 
        
    def price(self, df):           
        """
        Clean the 'Price' column by removing the dollar sign and converting it to float.

        Args:
            df (pandas.DataFrame): The DataFrame containing the data.

        Returns:
            None. The 'Price' column is updated directly in the DataFrame.
        """                                                                             
        df['Price'] = np.array([value.replace('$', '') for value in df['Price']]).astype(float)                 

    def rating_fillna(self, df):    
        """
        Fill missing values in the 'Rating' column with the mean rating.

        Args:
            df (pandas.DataFrame): The DataFrame containing the data.

        Returns:
            None. The missing values in the 'Rating' column are filled in-place with the mean rating.
        """                                                                            
        mean = round(df['Rating'].dropna().mean(), 1)                                                           
        df['Rating'].fillna(mean, inplace=True)

    def reviews_to_int(self, df):    
        """
        Convert the 'Reviews' column to integer values.

        Args:
            df (pandas.DataFrame): The DataFrame containing the data.

        Returns:
            None. The 'Reviews' column is converted to integer values in-place.
        """                                                                           
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
        """
        Drop rows with missing values from the DataFrame.

        Args:
            df (pandas.DataFrame): The DataFrame containing the data.

        Returns:
            None. Rows with missing values are dropped from the DataFrame in-place.
        """                                                                           
        if df.isna().sum().any()>0:
            df.dropna(inplace=True)

    def transform_age(self, df, column):   
        """
        Transform the 'Content Rating' column to an 'Age Restriction' column.

        Args:
            df (pandas.DataFrame): The DataFrame containing the data.
            column (str): The name of the 'Content Rating' column.

        Returns:
            None. The 'Content Rating' column is transformed to an 'Age Restriction' column in-place.
        """                                                                     
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
        """
        Drop the 'Unnamed: 0' column from the DataFrame.

        Args:
            df (pandas.DataFrame): The DataFrame containing the data.

        Returns:
            None. The 'Unnamed: 0' column is dropped from the DataFrame in-place.
        """                                                                           
        df = df.drop(columns=['Unnamed: 0'])

    def rename_categories(self, df): 
        """
        Rename the categories in the 'Category' column.

        Args:
            df (pandas.DataFrame): The DataFrame containing the data.

        Returns:
            None. The categories in the 'Category' column are renamed in-place.
        """                                                                           
        df['Category'] = df['Category'].str.replace('_', ' ').str.capitalize()

    def comma_replacer(self, df, col):  
        """
        Replace commas in a column with an empty string.

        Args:
            df (pandas.DataFrame): The DataFrame containing the data.
            col (str): The name of the column.

        Returns:
            None. Commas in the specified column are replaced with an empty string in-place.
        """                                                                        
        df[col] = df[col].str.replace(',', '')

    def quotatione_marks_replacer(self, df, col):  
        """
        Replace quotation marks in a column with an empty string.

        Args:
            df (pandas.DataFrame): The DataFrame containing the data.
            col (str): The name of the column.

        Returns:
            None. Quotation marks in the specified column are replaced with an empty string in-place.
        """                                                             
        df[col] = df[col].str.replace("'", '')
        df[col] = df[col].str.replace('"', '')   

    def drop_duplicates(self, df, col):
        """
        Drop duplicate rows based on a specific column.

        Args:
            df (pandas.DataFrame): The DataFrame containing the data.
            col (str): The name of the column used to identify duplicates.

        Returns:
            None. Duplicate rows based on the specified column are dropped from the DataFrame in-place.
        """
        df = df.drop_duplicates(subset=col, keep='first')                                                    