import pandas as pd
import numpy as np

from src.DataIngestor import DataIngestor
from src.DataPreprocessor import DataPreprocessor
from src.DataVisualizer import DataVisualizer

di = DataIngestor()
dp = DataPreprocessor()
dv = DataVisualizer('seaborn')
#da = DataAnalyzer()

df = di.load_file('database/raw/googleplaystore.csv', 'csv')
df = dp.pipeline(df)
df= di.load_file('database/output/processed_googleplaystore.csv', 'csv')
dv.pipeline(df)

