import seaborn as sns
import matplotlib.pyplot as plt
from typing import Literal

class DataVisualizator:

    def __init__(self, library: Literal["seaborn", "matplotlib"],seaborn_theme: Literal["darkgrid","whitegrid","dark","white","ticks",False] = False):
        self.library = library
        if seaborn_theme:
            sns.set_theme(style=seaborn_theme, context="paper")

    def column_by_grouping(self, df, column, group_by, function):

        # Create a pandas DataFrame with the requested data to be plotted
        data = df[[group_by, column]].groupby(by=group_by).agg(function).reset_index().sort_values(by=column, ascending=False) 
        

        fig, ax = plt.subplots(figsize=(17, 5))

        if self.library == "seaborn":
            sns.barplot(data=data,
                        y=group_by,
                        x=column,
                        color="b")
            plt.title(f'{column} by {group_by}')
            plt.show()

        else:
            ax.barh(y=data[group_by], width=data[column])
        ax.set(title = f'{column} by {group_by}',
                xlim=  0,
                xlabel = column,
                ylabel= group_by)
        plt.show()
