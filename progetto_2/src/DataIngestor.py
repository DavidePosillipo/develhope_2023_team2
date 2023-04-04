import pandas as pd

class DataIngestor:

    def __init__(self):
        pass

    def load_file(self, path, format):
        if format == 'pickle':
            return pd.read_pickle(path)
        elif format == 'csv':
            return pd.read_csv(path)
        elif format == 'xlsx':
            return pd.read_excel(path)
        else:
            return 'Apoligies, but this format has not been implemented yet.'
        
    def save_file(self, df, path, format):
        if format == 'pickle':
            return df.to_pickle(path)
        elif format == 'csv':
            return df.to_csv(path)
        elif format == 'xlsx':
            return df.to_excel(path)
        else:
            return 'Apoligies, but this format has not been implemented yet.'
        
    def load_to_list(self, path, col, format):

        if format == 'pickle':
            df = pd.read_pickle(path)
            return df.iloc[:, col]
        elif format == 'csv':
            df = pd.read_csv(path)
            return df.iloc[:, col]
        elif format == 'xlsx':
            df = pd.read_excel(path)
            return df.iloc[:, col]
        else:
            return 'Apoligies, but this format has not been implemented yet.'
        