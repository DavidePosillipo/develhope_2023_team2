import seaborn as sns
import matplotlib.pyplot as plt
from typing import Literal
import numpy as np
import pandas as pd
#from sklearn.cluster import KMeans

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
                    data = df.groupby(by=[var, hue]).size().reset_index()
                    
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
                    data = df.groupby(by=[var, hue])[var, hue].size().reset_index(name='Count')
                    print(data)
                    

        ax.set(title=f'Number of rows with for each {var} value')
        plt.show()
        

    def scatter_plot(self, df, col1, col2): #Impostare booleano per allungare query e fare group_by
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

    def cluster_scatter(slef, df, col1, col2, group_by):
        nun_df = df.select_dtypes(include='number')#try exclude

        def z_score(nun_df):
            return (nun_df-nun_df.mean())/nun_df.std()

        std_df = nun_df.transform(z_score, axis=0)
        std_df = std_df[['Rating', 'Reviews', 'Size', 'Installs', 'Price']]
        subset = df[['App', 'Category', 'Type', 'Content Rating']]
        new_df = pd.concat([subset, std_df], axis=1)
        new_df

        unique_categories = df['Category'].unique()
        label_dict = {}
        for i, category in enumerate(unique_categories):
            label_dict[category] = i

        kmeans = KMeans(n_clusters=len(df[group_by].unique()), random_state=0).fit(std_df)
        new_df[col1] = kmeans.labels_
        new_df[col2] = [label_dict.get(i) for i in new_df[col1]]
        new_df
        


# obiettivi:
#if len(df['group_by'])<=8 bar.plot(verticale quindi x e y invertite)
#scatterplot e regplot
# Ma se usassimo l' ereditarietà per creare 2 sotto classi di data_visualizer, una per seaborn e una per matplotlib