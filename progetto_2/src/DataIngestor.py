import pandas as pd
from PIL import Image
import os
from typing import Literal

class DataIngestor:

    def __init__(self):
        pass


    def load_file(self, path):                                              #     Def Load_File:
         
        format = path.rsplit(".")[1]
                                                                                    # - Reads a file from the specified path
        if format == 'pkl':                                                      # - Supports loading files in 'pickle', 'csv', and 'xlsx' formats
            return pd.read_pickle(path)                                             # - Returns a DataFrame or an error message for unsupported formats
        elif format == 'csv':                     
            return pd.read_csv(path)
        elif format == 'xlsx':
            return pd.read_excel(path)
        else:
            return 'Apoligies, but this format has not been implemented yet.'
        
        
        
    def save_file(self, df, path):                                          # -     Def Save_File:
         
        format = path.rsplit(".")[1] 
                                                                                    # - Saves a DataFrame to the specified path 
        if format == 'pkl':                                                      # - Supports saving files in 'pickle', 'csv', and 'xlsx' formats
            return df.to_pickle(path)                                               # - Returns an error message for unsupported formats
        elif format == 'csv':                     
            return df.to_csv(path)
        elif format == 'xlsx':
            return df.to_excel(path)
        else:
            return 'Apoligies, but this format has not been implemented yet.'
        
        
        
    def load_to_list(self, path, col):                                      #       Def Load_to_List:
        
        format = path.rsplit(".")[1] 
                                                                                    # - Reads a file from the specified path ('pickle', 'csv', and 'xlsx' formats)
        if format == 'pkl':                                                      # - Extracts a specified column from the DataFrame
            df = pd.read_pickle(path)                                               # - Returns the column values as a list
            return df.iloc[:, col].tolist()                                        # - Returns an error message for unsupported formats
        elif format == 'csv':                                                       
            df = pd.read_csv(path)               
            return df.iloc[:, col].tolist()     
        elif format == 'xlsx':
            df = pd.read_excel(path).reset_index(drop= True)
            return df.iloc[:, col].tolist() 
        else:
            return 'Apoligies, but this format has not been implemented yet.'
        
        
        
    def load_image(self, format, library: Literal["seaborn", "matplotlib"]):        #       Def Load_Image
                                                                                    # - Loads and displays images from a specified directory
        if format == 'png':                                                         # - Filters images (png format) based on the specified library
            directory = './database/output/graphs'                                  # - Returns an error message for unsupported formats
            for filename in os.listdir(directory):                              
                if library == 'seaborn' and 'sns' not in filename:
                    continue
                if library == 'matplotlib' and 'mat' not in filename:
                    continue
            
                filepath = os.path.join(directory, filename)
                img = Image.open(filepath)
                img.show()
        else:
            return 'Apologies, but this format has not been implemented yet.'