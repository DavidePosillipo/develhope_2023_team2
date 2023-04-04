from afinn import Afinn
import pandas as pd
import os

# ottieni il percorso della directory contenente il file corrente
curr_dir = os.path.dirname(os.path.abspath(__file__))

# costruisci il percorso completo al file di dati
data_file_path_p = os.path.join(curr_dir, "..", "database", "raw", "p.xlsx")
data_file_path_n = os.path.join(curr_dir, "..", "database", "raw", "n.xlsx")
#import the sentiment words files into dataframes
pos_words = pd.read_excel(data_file_path_p)['abound'].tolist()
neg_words = pd.read_excel(data_file_path_n)['faced'].tolist()

#import the google users reviews file into a dataframe
data_file_path_reviews = os.path.join(curr_dir, "..", "database", "raw", "googleplaystore_user_reviews.csv")
df = pd.read_csv(data_file_path_reviews)
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