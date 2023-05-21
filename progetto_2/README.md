# Python & SQL based ETL

Python & SQL based ETL is a project designed to perform ETL (Extract, Transform, Load) operations on the Google Play Store dataset. It utilizes Python and SQL to extract data from the dataset, transform and preprocess it, and load it into a database for analysis and visualization.

## Introduction

The Python & SQL based ETL project aims to provide insights and analysis on the Google Play Store dataset. By leveraging Python and SQL, it offers capabilities for data cleaning, transformation, sentiment analysis, and data visualization.

## Features

- Data ingestion from CSV files
- Data preprocessing and cleaning
- Sentiment analysis of user reviews
- Data visualization using Matplotlib and Seaborn
- Loading data into a PostgreSQL database

## Prerequisites

- Python 3
- PostgreSQL database
- Airflow

## Getting Started

Follow these steps to get the project up and running on your local machine:

1. Clone the repository:

   bash
   git clone https://github.com/DavidePosillipo/develhope_2023_team2.git

2. Go to the project directory:

   bash
   cd develhope_2023_team2/progetto_2

3. Activate the virtual environment:

   bash
   source .venv/bin/activate

4. Install the required Python libraries:

    bash
    pip install -r requirements.txt

5. Run Postgres:

   This may change according to your machine and where you have postgres installed.
   In case you don't have it installed: https://www.postgresql.org/download/

6. Set up the PostgreSQL database in local machine:

   Update the database connection details in the main.py file Line 60:

   db = DB_Handler(database = 'xxx', user = 'xxx', password='xxx', host='xxx', port=5432, path='this should stay the same for localhost')

   I suggest you use these inputs, just update the password with the one you set up when installing Postgres:
   Port default value is 5432, but it can vary according to postgresql version and configuration setup. For me port=5434 since I have postgresql version 14 and 15.

   db = DB_Handler(database = 'postgres', user = 'postgres', password='xxx', host='localhost', port=5432, path='database/output/processed_googleplaystore.csv')

7. Set up the PostgreSQL database in cloud server:

   Update the database connection details in the main.py file Line 80:
   To create a free account of your own you can register here: https://www.elephantsql.com/

   The final result should look like this:
   dh_cloud = DB_Handler(database='yhpzbiwk', user='yhpzbiwk', password='gNxA8nZrA_vFYCAQ143gVn-HRg6XTF1-', host='snuffleupagus.db.elephantsql.com', port=5432, path='database/output/processed_googleplaystore.csv')


8. Set up the Airflow connection:

   Create a new connection in Airflow using the connection details used at point 6.
   You might have to adjust some parameters according to your machine and airflow setup.
   
   There are many ways to connect the repository to airflow, the simplest way is to paste the files from develhope_2023_team2/progetto_2 repository to your airflow/dags folder. In this way you can keep the same path variables as in prj_dag.py file.
   Otherwise you will have to update the path variables according to your setup.