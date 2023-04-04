import pandas as pd
import numpy as np

from src.DataIngestor import DataIngestor
from src.DataPreprocessor import DataPreprocessor
from src.DataVisualizer import DataVisualizer

di = DataIngestor()

df = di.load_file('database/raw/googleplaystore.csv')


dp = DataPreprocessor()

df = dp.pipeline(df)

df= di.load_file('database/output/processed_googleplaystore.pickle')

dv = DataVisualizer("seaborn")

dv.pipeline(df)