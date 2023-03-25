import seaborn as sns
import matplotlib.pyplot as plt
from typing import Literal

class DataVisualizer:

    def __init__(self, library: Literal["seaborn", "matplotlib"],seaborn_theme: Literal["darkgrid","whitegrid","dark","white","ticks",False] = False):
        self.library = library
        if seaborn_theme:
            sns.set_theme(style=seaborn_theme)

    def column_by_grouping(self, df, column, group_by, function):
        data = df[[group_by, column]].groupby(by=group_by).agg(function).reset_index()
        
        fig, ax = plt.subplots(figsize=(15, 5))

        if self.library == "seaborn":

            sns.barplot(data=data.sort_values(by=column, ascending=False), #FYI Purtroppo seaborn e matplotlib ordinano i valori in maniera opposta
                        y=group_by,
                        x=column,
                        color="b")
            plt.title(f'{column} by {group_by}')

        else :
            data = df[[group_by, column]].groupby(by=group_by).agg(function).reset_index().sort_values(by=column, ascending=True) # FYI Purtroppo seaborn e matplotlib ordinano i valori in maniera opposta

            ax.barh(y=group_by, width=column, data=data)
            
        ax.set(title = f'{column} by {group_by}',
                xlabel = column,
                ylabel= group_by)
        plt.show()

