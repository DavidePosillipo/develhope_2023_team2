import seaborn as sns
import matplotlib.pyplot as plt
from typing import Literal

class DataVisualizzator:

    def __init__(self, library: Literal["seaborn", "mathplotlib"] = "seaborn",seaborn_theme: Literal["darkgrid","whitegrid","dark","white","ticks",False] = False):
        self.library = library
        if seaborn_theme:
            sns.set_theme(style=seaborn_theme, context="paper")

    def installs_by_category(self, df):
        # Displays a barplot for the total installs by category. Sorted by total installs.

        
        data = df[['Category','Installs']].groupby(by='Category').agg('sum').reset_index() # Create a pandas DataFrame with every category paired with the total amount of installs
        


        fig, ax = plt.subplots(figsize=(17, 5))

        if self.library == "seaborn":
            sns.barplot(data=data,
                        y="Category",
                        x="Installs",
                        order=data.sort_values(by='Installs', ascending=False).Category,
                        color="b")
        else:
            data = data.sort_values('Installs')
            ax.barh(y=data.Category, width=data.Installs)

        ax.set(title = "Installs by category",
                xlim=  0,
                xlabel = "Installs",
                ylabel= "Category")
        plt.show()