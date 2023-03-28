import seaborn as sns
import matplotlib.pyplot as plt
from typing import Literal
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

class DataVisualizer:

    def __init__(self, library: Literal["seaborn", "matplotlib"],seaborn_theme: Literal["darkgrid","whitegrid","dark","white","ticks",False] = False):
        self.library = library
        if seaborn_theme:
            sns.set_theme(style=seaborn_theme)


    def barh_by_grouping(self, df, column, group_by, agg):
        data = df[[group_by, column]].groupby(by=group_by).agg(agg).reset_index()
        
        fig, ax = plt.subplots(figsize=(15, 6))

        if self.library == "seaborn":

            sns.barplot(data=data.sort_values(by=column, ascending=False), #FYI Purtroppo seaborn e matplotlib ordinano i valori in maniera opposta
                        y=group_by,
                        x=column,
                        color="b")
            plt.title(f'{column} by {group_by}')

        else:
            data = df[[group_by, column]].groupby(by=group_by).agg(agg).reset_index().sort_values(by=column, ascending=True) # FYI Purtroppo seaborn e matplotlib ordinano i valori in maniera opposta

            ax.barh(y=group_by, width=column, data=data)
            
        ax.set(title = f'{column} by {group_by}',
                xlabel = column,
                ylabel= group_by)
        plt.show()


    def countplot(self, df, var:str, hue:str=None):
        # Automatically create a countplot of the specified categorical variable.
        # Optionally a hue can be specified to split the each entry into multiple bars.
        # No need to specify x or y, they are automatically assigned.

        fig, ax = plt.subplots()

        if len(df[var].unique()) < 5:
            if not hue:
                if self.library == 'seaborn':
                    sns.countplot(x=data.items, color='blue', order=df[var].value_counts().index)
                else:
                    data = df['Category'].value_counts().sort_values(ascending=True)
                    plt.bar(x=data.index, height=data.values, color='blue')
            else:
                if self.library == 'seaborn':
                    sns.countplot(x=df[var], hue=df[hue], order=df[var].value_counts().index)
                else:
                    data = df.groupby(by=[var, hue])[var, hue].size().unstack(fill_value=0)
                    data = data.sort_values(by=list(data.columns)[0], ascending=False)

                    x = np.arange(len(data.index))
                    width = 0.50 # Width of bars
                    multiplier = 0
                    for attribute, measurment in data.items():
                        offset = width * multiplier
                        bar = ax.bar(x + offset, measurment, width, label=attribute)
                        ax.bar_label(bar, padding=3)
                        multiplier += 1

                    ax.set_xticks(x + width, data.index)
                    
        else:
            if not hue:
                if self.library == 'seaborn':
                    sns.countplot(y=df[var], color='blue', order=df[var].value_counts().index)
                else:
                    data = df['Category'].value_counts(ascending=True)
                    plt.barh(y=data.index, width=data.values,color='blue')
            else:
                if self.library == 'seaborn':
                    sns.countplot(y=df[var], hue=df[hue], order=df[var].value_counts().index)
                else:
                    data = df.groupby(by=[var, hue])[var, hue].size().unstack(fill_value=0)
                    data = data.sort_values(by=list(data.columns)[0])

                    y = np.arange(len(data.index))
                    height = 0.50 # height of bars
                    multiplier = 0
                    for attribute, measurment in data.items():
                        offset = height * multiplier
                        bar = ax.barh(y + offset, measurment, height, label=attribute)
                        ax.bar_label(bar, padding=3)
                        multiplier += 1

                    ax.set_yticks(y + height, data.index)

                    

        ax.set(title=f'Number of apps with for each {var} value')
        plt.show()
        

    def scatter_plot(self, df, col1, col2): 
        def rho(col1, col2):
            r = np.corrcoef(col1, col2)
            return r[0,1]
        
        x = df[col1]
        y = df[col2]


        if self.library == "seaborn":
            sns.regplot(x=x, y=y, data=df)

        else:
            plt.plot(x, y, 'o', color='blue')
            m, b = np.polyfit(x, y, 1)
            plt.plot(x, m*x+b, color='blue')
        
        plt.title(f"Pearson's correlation coefficient: {rho(x, y)}")
        plt.xlabel(f'Number of {col1}')
        plt.ylabel(f'Total {col2}')
        plt.show()

    def cluster_scatter(self, df, col1, col2, group_by):
        nun_df = df.select_dtypes(exclude='object')
        nun_df = nun_df.drop(columns=['Unnamed: 0'])

        def z_score(nun_df):
            return (nun_df-nun_df.mean())/nun_df.std()

        std_df = nun_df.agg(z_score, axis=0)
        std_df = std_df[['Rating', 'Reviews', 'Size', 'Installs', 'Price']]
        subset = df[['App', 'Type', 'Content Rating']]
        subset_1 = df[['Category']]
        frames = [subset_1, subset, nun_df, std_df]
        data = pd.merge(subset_1, subset, left_index=True, right_index=True)
        data1 = pd.merge(nun_df, std_df, left_index=True, right_index=True, suffixes=('', '_std'))
        final_data = pd.merge(data, data1, left_index=True, right_index=True)


        unique_categories = final_data[group_by].unique()
        label_dict = {}
        for i, category in enumerate(unique_categories):
            label_dict[category] = i

        '''kmeans = KMeans(n_clusters=len(df[group_by].unique()), random_state=0)
        final_data[col1] = kmeans.labels_
        final_data[col2] = [label_dict.get(i) for i in final_data[col1]]'''

        sns.scatterplot(x=col1, y=col2, data=final_data, hue=group_by)
        plt.show()
        print(final_data)