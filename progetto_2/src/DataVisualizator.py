import seaborn as sns
import matplotlib.pyplot as plt
from typing import Literal

class DataVisualizzator:

    def __init__(self, seaborn_theme: Literal["darkgrid","whitegrid","dark","white","ticks",False] = "darkgrid"):
        sns.set_theme(style=seaborn_theme, context="paper")

    def installs_by_category(self, df, lib:  Literal["seaborn","matplotlib"] = "seaborn"):
        # Displays a barplot for the total installs by category. Sorted by total installs.

        # Create a pandas DataFrame with svery category paired with the total installs
        data = df[['Category','Installs']].groupby(by='Category').agg('sum').reset_index()

        if lib == "seaborn":
            sns.set_theme(style="darkgrid")
            f, ax = plt.subplots(figsize=(5, 5))
            sns.barplot(data=data,
                        y="Category",
                        x="Installs",
                        order=data.sort_values(by='Installs', ascending=False).Category,
                        color="b")
            ax.set(title = "Installs by category",
                xlim=  0,
                xlabel = "Installs",
                ylabel= "Category")
            plt.show()