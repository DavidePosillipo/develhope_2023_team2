import seaborn as sns
import matplotlib.pyplot as plt
from typing import Literal
import numpy as np
import pandas as pd
from afinn import Afinn

class DataVisualizer:

    def __init__(self, library: Literal["seaborn", "matplotlib"],seaborn_theme: Literal["darkgrid","whitegrid","dark","white","ticks",False] = False):
        self.library = library
        if seaborn_theme:
            sns.set_theme(style=seaborn_theme)

    def pipeline(self, df, df_all):
        sns_vis = DataVisualizer(library="seaborn")
        sns_vis.barh_by_grouping(df, column="Rating", group_by="Category", agg='sum')
        sns_vis.scatter_plot(df, 'Installs', 'Reviews')
        sns_vis.countplot(df, var='Category', hue='Type')
        sns_vis.grouped_rating(df, ["Category", "Type"], "Rating")                          #average Rating devided in free and paid Apps for each Category
        sns_vis.grouped_rating(df, "Category", "Rating")                                    #average Rating per Category
        sns_vis.popularity_score(df)                                                        #top 10 Apps by Popularity (Rating*Installs)
        sns_vis.rating_counter(df, "Rating", "Category")                                    #number of Apps in each Category for each Rating range
        sns_vis.rating_counter(df, "Rating", "Type")                                      #number of Apps in each Type (free, paid) for each Rating range
        sns_vis.growth_trend(df)
        sns_vis.correlation_heatmap(df)
        sns_vis.sentiment_by_category(df_all)

        plt_vis = DataVisualizer(library="matplotlib")
        plt_vis.barh_by_grouping(df, column="Rating", group_by="Category", agg='sum')
        plt_vis.scatter_plot(df, 'Installs', 'Reviews')
        plt_vis.countplot(df, var='Category', hue='Type')
        plt_vis.grouped_rating(df, ["Category", "Type"], "Rating")                          #average Rating devided in free and paid Apps for each Category
        plt_vis.grouped_rating(df, "Category", "Rating")                                    #average Rating per Category
        plt_vis.popularity_score(df)                                                        #top 10 Apps by Popularity (Rating*Installs)
        plt_vis.rating_counter(df, "Rating", "Category")                                    #number of Apps in each Category for each Rating range
        plt_vis.rating_counter(df, "Rating", "Type") 
        plt_vis.growth_trend(df)
        plt_vis.correlation_heatmap(df)
        plt_vis.sentiment_by_category(df_all)

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
        plt.savefig('./database/output/graphs/barplot.png')
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
        plt.savefig('./database/output/graphs/countplot.png')
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
        plt.savefig('./database/output/graphs/scatterplot.png')
        plt.show()

    def grouped_rating(self, df, by: Literal["Category", "Type"], column, n= None, ascending= False):
    
        df_group = df.groupby(by= by)[column].agg(["mean", "max", "min"]).sort_values(["mean", "max", "min"], ascending= [ascending, ascending, ascending]).head(n)

        fig, ax = plt.subplots(figsize= (16, 8))

        if self.library == "seaborn":
            if type(by) != list:
                
                sns.barplot(data= df_group, x= df_group.index, y= "max", color= "g", label= "Max")
                sns.barplot(data= df_group, x= df_group.index, y= "mean", color= "r", label= "Average")
                sns.barplot(data= df_group, x= df_group.index, y= "min", color= "b", label= "Min")
                ax.set_xticklabels(ax.get_xticklabels(), rotation="vertical")
                ax.set(xlabel= by, ylabel= column)
                ax.legend() 
                ax.set_title(f"Rating by {by}")                 

            else:
                df_group = df.groupby(by)[column].mean().unstack().sort_values(["Free", "Paid"], ascending = [ascending, ascending]).reset_index().head(n)
                df_melted = pd.melt(df_group, id_vars= "Category", var_name='Type', value_name='Rating')
                sns.barplot(x= "Category", y= "Rating", data=df_melted, hue= "Type", palette= ["r", "g"])
                ax.set_ylabel("Rating")
                ax.set_xticklabels(df_melted.Category.unique(), rotation=90)
                ax.set_title("Average Rating of free and paid Apps in each Category")
                ax.legend()
            
        else:   
            if type(by) != list:
                plt.bar(df_group.index, df_group["max"], color= "g", label='Max')
                plt.bar(df_group.index, df_group["mean"], color= "r", label='Average')
                plt.bar(df_group.index, df_group["min"], color= "b", label='Min')
                plt.legend() 
                plt.ylabel("Rating")
                plt.xticks(rotation= "vertical")
                plt.title(f"Rating by {by}") 

            else:
                df_group = df.groupby(by)[column].mean().unstack().sort_values(["Free", "Paid"], ascending = [ascending, ascending]).head(n)
                bar_width = 0.35
                x = np.arange(len(df_group.index))
                y_free = df_group.Free.fillna(0).values
                y_paid = df_group.Paid.fillna(0).values                
                plt.bar(x - bar_width / 2, y_free, bar_width, color= "r", label="Free")
                plt.bar(x + bar_width / 2, y_paid, bar_width, color= "g", label="Paid")
                plt.ylabel("Rating")
                plt.xticks(x, df_group.index, rotation= "vertical")
                plt.title("Average Rating of free and paid Apps in each Category")
                plt.legend()
        plt.savefig('./database/output/graphs/group_rating.png')        
        plt.show()
        
    
    def popularity_score(self, df, n= 10, ascending= False, all_info= False, free= "all"):
        df_copy = df.copy()
        df_copy["Popularity"] = round(df_copy.Installs * df_copy.Rating / (int(str(max(df_copy.Installs))[:-3]) if len(str(max(df.Installs))) > 7 else 10), 4)

        if free != "all":
            if free == True:
                df_popularity = df_copy[df_copy["Type"] == "Free"].sort_values(by= ["Popularity", "Installs", "Rating"], ascending= [ascending, ascending, ascending])[df_copy.columns if all_info else ["App","Popularity"]].head(n)
            else:
                df_popularity = df_copy[df_copy["Type"] == "Paid"].sort_values(by= ["Popularity", "Installs", "Rating"], ascending= [ascending, ascending, ascending])[df_copy.columns if all_info else ["App", "Popularity"]].head(n)
        else:
            df_popularity = df_copy.sort_values(by= ["Popularity", "Installs", "Rating"], ascending= [ascending, ascending, ascending])[df_copy.columns if all_info else ["App", "Popularity"]].head(n)
        
        fig, ax = plt.subplots(figsize= (16, 8))
        
        if self.library == "seaborn":
            sns.barplot(x= df_popularity["App"], y= df_popularity["Popularity"], palette=  "viridis")
            ax.set_xticklabels(ax.get_xticklabels(), rotation="vertical")
            ax.set(xlabel= "Apps", ylabel= f"Popularity (Installs*Rating/{int(str(max(df.Installs))[:-3]) if len(str(max(df.Installs))) > 7 else 10})")
            ax.set_title("Top 10 Apps by Popularity")
            
        else:
            plt.bar(df_popularity["App"], df_popularity["Popularity"])
            plt.xticks(rotation= "vertical")
            plt.xlabel("Apps")
            plt.ylabel(f"Popularity (Installs*Rating/{int(str(max(df.Installs))[:-3]) if len(str(max(df.Installs))) > 7 else 10})")
            plt.title("Top 10 Apps by Popularity")
        plt.savefig('./database/output/graphs/popularity_rating.png')
        plt.show()
    
    def rating_counter(self, df, column, by: Literal["Category", "Type"], n= None, ascending= False):

        data = df.groupby(by= by)[column].apply(lambda x: pd.cut(x, bins= [1,2,3,4,5]).value_counts()).unstack()
        
        fig, ax = plt.subplots(figsize= (16, 8))

        if self.library == "seaborn":
            data.columns = ["1-2", "2-3", "3-4", "4-5"]  

            if by == "Category":    
                data = data[["4-5", "3-4", "2-3", "1-2"]]
            elif "Type" in by:
                data.columns = ["4-5", "3-4", "2-3", "1-2"]

            data = data.sort_values(by= ["4-5", "3-4", "2-3", "1-2"], ascending= [ascending,ascending,ascending,ascending])
            data_melted = data.reset_index().melt(id_vars= by, var_name= "Rating", value_name= "App Count")
            sns.barplot(x= by, y= "App Count", data= data_melted, hue= "Rating")
            ax.set_xticklabels(ax.get_xticklabels(), rotation= "vertical")
            ax.set(xlabel= "Categories", ylabel= "App Count")   
            ax.set_title(f"Number of Apps in each Rating range devided by {by}")
            
        else:
            if "Category" in by:
                data.columns = ["1-2", "2-3", "3-4", "4-5"]
            elif "Type" in by:
                data.columns = ["4-5", "3-4", "2-3", "1-2"]
                 
            data = data.sort_values(by= ["4-5", "3-4", "2-3", "1-2"], ascending= [ascending,ascending,ascending,ascending])
            x = np.arange(df[by].nunique())*1.75
            x1 = data["4-5"]
            x2 = data["3-4"]
            x3 = data["2-3"]
            x4 = data["1-2"]
                
            plt.bar(x - (9*0.1) / 2, x1, 0.3, label= "4-5")
            plt.bar(x - (3*0.1) / 2, x2, 0.3, label= "3-4")
            plt.bar(x + (3*0.1) / 2, x3, 0.3, label= "2-3")
            plt.bar(x + (9*0.1) / 2, x4, 0.3, label= "1-2")
            plt.xticks(x, df[by].unique(), rotation= "vertical")
            plt.ylabel("App Count")
            plt.legend()
            plt.title(f"Number of Apps in each Rating range devided by {by}")
        plt.savefig('./database/output/graphs/rating_counter.png')   
        plt.show()  

    def growth_trend(self, df):

        df = df[['App', 'Category', 'Last Updated']]
        #1 Selezione categorie da mostrare nel grafico
        categories = ['ENTERTAINMENT', 'BUSINESS', 'FAMILY', 'FINANCE', 'PRODUCTIVITY']
        df_main = df[df['Category'].isin(categories)]
        df_main.loc[:, 'Last Updated'] = pd.to_datetime(df['Last Updated'])
        #2 group by anno e conteggio numero app per categoria per ogni anno
        grouped = df_main.groupby([df_main['Last Updated'].dt.year, 'Category'])['Category'].count()
        #3 Riempi i valori nan con 0
        trend = grouped.unstack(level=1, fill_value=0)

        # Ripeti i passaggi 1, 2, 3 per creare un altro dataframe che possa computare la media delle categorie mancanti
        df_else = df[~df['Category'].isin(categories)]
        df_else.loc[:, 'Last Updated'] = pd.to_datetime(df_else['Last Updated'])
        grouped_else = df_else.groupby([df_else['Last Updated'].dt.year, 'Category'])['Category'].count()
        trend_else = grouped_else.unstack(level=1, fill_value=0)
        trend_else_mean = trend_else.mean(axis=1)

        # Aggiungi la media ricavata al dataframe principale
        trend['Average of other categories'] = trend_else_mean
        print(df_else)

        # Creazione plot
        # Seaborn
        if self.library=='seaborn':
            sns.lineplot(data=trend)
            plt.title('Growth of number of Apps by Category Over Time')
            plt.xlabel('Year')
            plt.ylabel('Average Number of Apps')
        
        else:
            trend.plot(kind='line', figsize=(10,5))
            plt.title('Growth of number of Apps by Category Over Time')
            plt.xlabel('Year')
            plt.ylabel('Average Number of Apps')
        plt.savefig('./database/output/graphs/growth_trend.png')
        plt.show()

    def correlation_heatmap(self, df):
        std_df = df.corr()
        std_df = std_df.drop(columns=['Unnamed: 0'], index=['Unnamed: 0'])
        
        if self.library=='seaborn':
            plt.figure(figsize = (15,6))
            sns.heatmap(std_df, annot=True)
        else:
            # Create a colormap
            cmap = plt.get_cmap('magma')

            # Plot the matrix
            fig, ax = plt.subplots(figsize=(15, 6))
            im = ax.imshow(std_df, cmap=cmap, extent=[0, len(std_df.columns), 0, len(std_df.columns)], origin='lower')

            # Set ticks and labels
            xticks = np.arange(0.5, len(std_df.columns), 1)
            yticks = np.arange(0.5, len(std_df.columns), 1)
            ax.set_xticks(xticks, minor=False)
            ax.set_yticks(yticks, minor=False)
            ax.set_xticklabels(std_df.columns, fontsize=15, rotation=65)
            ax.set_yticklabels(std_df.columns, fontsize=15)

            # Set colorbar
            cbar = fig.colorbar(im, ax=ax, orientation='vertical')
            cbar.ax.tick_params(labelsize=15, rotation=90)  # Set font size and rotation of colorbar labels

            # Loop over data dimensions and create text annotations
            for i in range(len(std_df.columns)):
                for j in range(len(std_df.columns)):
                    text = ax.text(j+0.5, i+0.5, round(std_df.to_numpy()[i, j], 2),
                                ha="center", va="center", color="white", fontsize=12)
        plt.title('Correlation heatmap')
        plt.savefig('./database/output/graphs/correlation_heatmap.png')
        plt.show()
        
    def sentiment_by_category(self, df_all):
        result = df_all.groupby("Category")["sentiment score"].mean().sort_values(ascending= False)

        fig, ax = plt.subplots(figsize= (16, 8))
        plt.bar(result.index, result.values)
        plt.xticks(rotation= 35, ha= "right", fontsize= 8)
        plt.ylabel("Avg Sentiment")
        plt.legend()
        plt.title("Average sentiment per Category")
        plt.savefig('./database/output/graphs/sentiment_by_category.png')
        plt.show()

