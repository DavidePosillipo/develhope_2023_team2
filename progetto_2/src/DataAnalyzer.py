from afinn import Afinn
import pandas as pd

class DataAnalyzer():
    """
    DataAnalyzer class for performing sentiment analysis on app reviews.

    Attributes:
        None.

    Methods:
        pipeline: Runs the DataAnalyzer pipeline to calculate sentiment scores and merge dataframes.
        sentiment_score: Calculates sentiment scores for reviews and merges them with the original dataframe.
    """
    def __init__(self):
        pass
    
    def pipeline(self, df, df_reviews, n_words, p_words):  
        """
        Runs the DataAnalyzer pipeline to calculate sentiment scores and merge dataframes.

        Args:
            df (pd.DataFrame): The original dataframe.
            df_reviews (pd.DataFrame): The dataframe containing reviews.
            n_words (list): List of negative words.
            p_words (list): List of positive words.

        Returns:
            tuple: A tuple containing the updated dataframes:
                - df_reviews: The dataframe with sentiment scores.
                - df_sentiment: The dataframe with aggregated sentiment scores by app.
                - df_all: The merged dataframe of the original data and sentiment scores.
        """                                                
        return self.sentiment_score(df, df_reviews, n_words, p_words)                                     

    def sentiment_score(self, df, df_reviews, p_words, n_words):                                           
        """
        Calculates sentiment scores for reviews and merges them with the original dataframe.

        Args:
            df (pd.DataFrame): The original dataframe.
            df_reviews (pd.DataFrame): The dataframe containing reviews.
            p_words (list): List of positive words.
            n_words (list): List of negative words.

        Returns:
            tuple: A tuple containing the updated dataframes:
                - df_reviews: The dataframe with sentiment scores.
                - df_sentiment: The dataframe with aggregated sentiment scores by app.
                - df_all: The merged dataframe of the original data and sentiment scores.
        """                                                                                                   
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
        
        df_all = df.merge(df_sentiment, on= "App")
        
        return df_reviews, df_sentiment, df_all