from afinn import Afinn
import pandas as pd

class DataAnalyzer():

    def __init__(self):
        pass
    
    def pipeline(self, df, n, p):
        self.sentiment_score(df, n , p)

    def sentiment_score(self, df, n, p):
        pos_words = n['abound'].tolist()
        neg_words = p['faced'].tolist()
        afinn = Afinn()

        score_list = []

        for review in df["Translated_Review"]:
            score_tot = 0
            review_words = str(review).lower().split()

            for word in review_words:
                word = word.lower()
                if (word in pos_words) or (word in neg_words):
                    score_tot += afinn.score(word)

            score_list.append(score_tot)

        df["sentiment score"] = pd.Series(score_list)
        print(df["sentiment score"])