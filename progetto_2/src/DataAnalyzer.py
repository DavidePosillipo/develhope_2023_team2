from afinn import Afinn
import pandas as pd

class DataAnalyzer():

    def __init__(self):
        pass
    
    def pipeline(self, df, n, p):
        return self.sentiment_score(df, n, p)

    def sentiment_score(self, df_reviews, p_words, n_words):
        
        p_words = p_words.tolist()
        n_words = n_words.tolist()
        
        df_reviews = df_reviews[~df_reviews["Translated_Review"].isna()].reset_index(drop= True)
        
        afinn = Afinn()

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
        
        return df_reviews, df_sentiment
