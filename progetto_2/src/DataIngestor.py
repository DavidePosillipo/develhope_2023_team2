import pandas as pd

class DataIngestor:

    def __init__(self):
        pass

    def load_file(self, path):
        return pd.read_csv(path)