import pandas as pd
from PIL import Image
import os
from typing import Literal

class DataIngestor:

    def __init__(self):
        pass


    def load_file(self, path):                                              
         
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
        
        
        
    def load_image(self, format, library: Literal["seaborn", "matplotlib"]):     
           
        if format == 'png':                                                         
            directory = './database/output/graphs'                                  
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