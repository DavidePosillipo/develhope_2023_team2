'''Creare una classe Analyser dove viene effettuata la sentiment analysis
Il file system viene modificato come segue:'''
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from afinn import Afinn


# 1-Find if there is a correlation between the price of the apps and the category (Teen, Everyone, Mature). 
# L’età misurata per fasce (0-13 anni, 13-17 anni, 17-18 anni, 18 in su.) è una variabile qualitativa ordinale 
# e pertanto sarà necessario scegliere un altro metodo di analisi, come la correlazione di SPEARMAN o la correlazione di KENDALL.
def scatter_plot(df, col1, col2, method, hue, selected_categories=False): 
    
    if selected_categories==True: # Documentazione convenzione google
        df = df.loc[(df[hue]=='Teen') | (df[hue]=='Everyone') | (df[col2]>=17)]
        x=df[col1]
        y=df[col2]
        result = x.corr(y, method=method)
        print(result)

    else:
        x=df[col1]
        y=df[col2]
        result = x.corr(y, method=method)
        print(result)

    #seaborn
    sns.scatterplot(x=x, y=y, data=df, hue=hue)
    plt.title(f"The {method} correlation coefficient: {result}")
    plt.xlabel(f'Number of {col1}')
    plt.ylabel(f'Total {col2}')
    plt.show()

    #matplotlib.pyplot
    plt.scatter(x=col1, y=col2, data=df, color='c')
    plt.title(f"The {method} correlation coefficient: {result}")
    plt.xlabel(f'Number of {col1}')
    plt.ylabel(f'Total {col2}')
    plt.show()
    
scatter_plot(df, 'Price', 'Age Restriction', 'spearman', 'Content Rating', selected_categories=False)



#2-find the sentiment of all apps using np files (negative words and positive words) and "afinn" lib 
#link for np files https://drive.google.com/drive/folders/1824UvFm8WBcOX_iiev0kNMrDu7aUARra?usp=share_link
#ask them to search about afinn lib 
afn = Afinn()

#loading data
df_rev=pd.read_csv('googleplaystore_user_reviews.csv')
#cleaning data
df_rev.dropna(subset='Translated_Review',inplace=True)
#scoring each review wirh afinn method
df_rev['review_score']=df_rev['Translated_Review'].map(afn.score)
#computing the mean score for each App
df_rev.groupby(by='App').agg({'review_score':'mean'}).rename(columns={'review_score':'review_score_mean_by_app'})




#3-for paid apps only list the top 5 highest and lowest sentiment numbers with the name of the app and the app category 

df_app = pd.read_csv("processed_googleplaystore.csv")
df_ur = pd.read_csv('googleplaystore_user_reviews.csv')

print(f"Original shape: {df_ur.shape}")

print(f"NaN per column:\n{df_ur.isna().sum()}")

df_ur.dropna(inplace=True)

print(f"Shape after dropna: {df_ur.shape}")


paid_app = set(df_app.loc[df_app['Type'] == 'Paid', 'App'])
print(paid_app)

paid_app_ur = df_ur.loc[df_ur['App'].isin(paid_app)]

print(paid_app_ur)

afn = Afinn()

paid_app_ur['Sentiment_Score'] = paid_app_ur['Translated_Review'].apply(afn.score)

print(paid_app_ur)

sentiment_scores = paid_app_ur.groupby(by='App')['Sentiment_Score'].agg('mean')

print(sentiment_scores)

print(f"\n\nHighest avg sentiment score: {sentiment_scores.nlargest(5)}")
print(f"\n\nLowest avg sentiment score: {sentiment_scores.nsmallest(5)}")




#4-what is the best category according to sentiment values
df = pd.read_csv("processed_googleplaystore.csv") # Specificare nome file in directory repo
afn = Afinn()

#loading data
df_rev=pd.read_csv('googleplaystore_user_reviews.csv')
#cleaning data
df_rev.dropna(subset='Translated_Review',inplace=True)
#scoring each review wirh afinn method
df_rev['review_score']=df_rev['Translated_Review'].map(afn.score)

#computing the mean score for each App
app_score = df_rev.groupby(by='App').agg({'review_score':'mean'}).rename(columns={'review_score':'review_score_mean_by_app'})

#adding the scores column to the original df 
df_scored_apps = df.join(app_score['review_score_mean_by_app'], on= "App").dropna()

#find the best category
best_category = df_scored_apps.groupby("Category")["review_score_mean_by_app"].mean().sort_values(ascending= False).head(1)

print(best_category)
