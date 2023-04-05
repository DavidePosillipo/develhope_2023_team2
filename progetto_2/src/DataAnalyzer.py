from afinn import Afinn
import pandas as pd

class DataAnalyzer():

    def __init__(self):
        pass
    
    def pipeline(self, df):
        sentiment_score = df.groupby('App')['Score'].agg(Sentiment_score='mean').reset_index()
        return sentiment_score
        

    def sentiment_score(self, df):
        
        afn = Afinn()
        df['Score'] = df['Translated_Review'].apply(afn.score)
        return df