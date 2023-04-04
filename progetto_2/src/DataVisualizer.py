import numpy as np
import pandas as pd
from afinn import Afinn


pos_words = pd.read_excel("../database/raw/p.xlsx")["Positive Words"].tolist()
neg_words = pd.read_excel("../database/raw/n.xlsx")["Negative Words"].tolist()
#print(pos_words)
df_reviews = pd.read_csv("../database/raw/googleplaystore_user_reviews.csv")
#print(df_reviews)
afinn = Afinn()

score_list = []

for review in df_reviews["Translated_Review"]:
    score_tot = 0
    review_words = str(review).lower().split()

    for word in review_words:
        word = word.lower()
        if (word in pos_words) or (word in neg_words):
            score_tot += afinn.score(word)

    score_list.append(score_tot)


df_reviews["sentiment score"] = pd.Series(score_list)

print(df_reviews["sentiment score"])