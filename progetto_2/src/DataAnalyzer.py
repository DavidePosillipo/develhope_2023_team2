from afinn import Afinn
import pandas as pd

#import the sentiment words files into dataframes
pos_words = pd.read_excel(r"C:\Users\Crypto.gunner\Desktop\CODING\DevelHope\Progetto_APP\develhope_2023_team2\progetto_2\database\raw\p.xlsx")['abound'].tolist()
neg_words = pd.read_excel(r"C:\Users\Crypto.gunner\Desktop\CODING\DevelHope\Progetto_APP\develhope_2023_team2\progetto_2\database\raw\n.xlsx")['faced'].tolist()

#import the google users reviews file into a dataframe
df = pd.read_csv(r'C:\Users\Crypto.gunner\Desktop\CODING\DevelHope\Progetto_APP\develhope_2023_team2\progetto_2\database\raw\googleplaystore_user_reviews.csv')
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