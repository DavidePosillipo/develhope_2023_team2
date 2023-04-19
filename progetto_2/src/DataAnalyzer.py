from afinn import Afinn
import pandas as pd

class DataAnalyzer():

    def __init__(self):
        pass
    
    def pipeline(self, df, df_reviews, n_words, p_words):                                                  #      Def Pipeline:
        return self.sentiment_score(df, df_reviews, n_words, p_words)                                      # - Runs DataAnalizer pipeline's methods 

    def sentiment_score(self, df, df_reviews, p_words, n_words):                                           #      Def Sentiment_Score:                                                                                                                                                         
                                                                                                           # - Filters out rows with missing translated reviews
        df_reviews = df_reviews[~df_reviews["Translated_Review"].isna()].reset_index(drop= True)           # - Aggregates sentiment scores by App
                                                                                                           # - Calculates sentiment scores for each review using Afinn
        afinn = Afinn()                                                                                    # - Merges aggregated scores with the original DataFrame #
                                                                                                           # - Returns the updated DataFrames
        score_list = []

        for review in df_reviews["Translated_Review"]:

            score_tot = 0
            review_words = str(review).lower().split()

            for word in review_words:
                word = word.lower()
                if (word in p_words) or (word in n_words):
                    score_tot += afinn.score(word)

            score_list.append(score_tot)

        df_reviews["sentiment score"] = pd.Series(score_list)
        
        df_sentiment = df_reviews.groupby("App")["sentiment score"].mean()
        df_sentiment = df_sentiment.rename_axis('app') ### CORREZIONE HARD CODED --> BISOGNA IMPLEMENTARE UN METODO CHE RENDA TUTTI I NOMI DELLE COLONNE MINUSCOLI


        print(df.head(5))
        print(df_sentiment.head(5))
        
        df_all = df.merge(df_sentiment, on= "app")
        
        return df_reviews, df_sentiment, df_all