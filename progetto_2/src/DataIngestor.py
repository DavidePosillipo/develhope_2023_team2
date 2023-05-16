import pandas as pd
from PIL import Image
import os
from typing import Literal

class DataIngestor:
    """
    A class that provides methods for loading and saving data files, loading specific columns of a file into a list,
    and loading and displaying image files.

    Methods:
        load_file(path): Load a data file from the specified path.
        save_file(df, path): Save a DataFrame to a file in the specified path.
        load_to_list(path, col): Load a specific column of a data file into a list.
        load_image(format, library): Load and display an image file.

    Usage:
        ingestor = DataIngestor()
        data = ingestor.load_file('data.csv')
        ingestor.save_file(data, 'data.pkl')
        column_data = ingestor.load_to_list('data.xlsx', 2)
        ingestor.load_image('png', 'seaborn')
    """
    def __init__(self):
        pass


    def load_file(self, path):                                              
        """
        Load a data file from the specified path.

        Args:
            path (str): The path of the file to load.

        Returns:
            pd.DataFrame: The loaded data as a DataFrame.
        """ 
        format = path.rsplit(".")[1]
                                                                                    
        if format == 'pkl':                                                     
            return pd.read_pickle(path)                                             
        elif format == 'csv':                     
            return pd.read_csv(path)
        elif format == 'xlsx':
            return pd.read_excel(path)
        else:
            return 'Apoligies, but this format has not been implemented yet.'
        
        
        
    def save_file(self, df, path):                                          
        """
        Save a DataFrame to a file in the specified path.

        Args:
            df (pd.DataFrame): The DataFrame to save.
            path (str): The path of the file to save.

        Returns:
            None.
        """ 
        format = path.rsplit(".")[1] 
                                                                                    
        if format == 'pkl':                                                      
            return df.to_pickle(path)                                              
        elif format == 'csv':                     
            return df.to_csv(path)
        elif format == 'xlsx':
            return df.to_excel(path)
        else:
            return 'Apoligies, but this format has not been implemented yet.'
        
        
        
    def load_to_list(self, path, col):                                     
        """
        Load a specific column of a data file into a list.

        Args:
            path (str): The path of the file to load.
            col (int): The index of the column to load.

        Returns:
            list: The specified column as a list.
        """
        format = path.rsplit(".")[1] 
                                                                                    
        if format == 'pkl':                                                      
            df = pd.read_pickle(path)                                               
            return df.iloc[:, col].tolist()                                       
        elif format == 'csv':                                                       
            df = pd.read_csv(path)               
            return df.iloc[:, col].tolist()     
        elif format == 'xlsx':
            df = pd.read_excel(path).reset_index(drop= True)
            return df.iloc[:, col].tolist() 
        else:
            return 'Apoligies, but this format has not been implemented yet.'
        
        
        
    def load_image(self, format, path, library: Literal["seaborn", "matplotlib"]):     
        """
        Load and display an image file.

        Args:
            format (str): The format of the image file.
            library (Literal["seaborn", "matplotlib"]): The library to use for displaying the image.

        Returns:
            None.
        """
        if format == 'png':                                   
            for filename in os.listdir(path):                              
                if library == 'seaborn' and 'sns' not in filename:
                    continue
                if library == 'matplotlib' and 'mat' not in filename:
                    continue
            
                filepath = os.path.join(path, filename)
                img = Image.open(filepath)
                img.show()
        else:
            return 'Apologies, but this format has not been implemented yet.'