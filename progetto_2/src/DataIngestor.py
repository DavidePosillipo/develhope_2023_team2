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
            return pd.read_xlsx(path)
        else:
            return 'Apoligies, but this format has not been implemented yet.'
        
    def save_file(self, path, format):
        if format == 'pickle':
            return pd.to_pickle(path)
        elif format == 'csv':
            return pd.to_csv(path)
        elif format == 'xlsx':
            return pd.to_xlsx(path)
        else:
            return 'Apoligies, but this format has not been implemented yet.'