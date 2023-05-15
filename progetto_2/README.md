# Python & SQL based ETL

Python & SQL based ETL is a project designed to perform ETL (Extract, Transform, Load) operations on the Google Play Store dataset. It utilizes Python and SQL to extract data from the dataset, transform and preprocess it, and load it into a database for analysis and visualization.

## Introduction

The Python & SQL based ETL project aims to provide insights and analysis on the Google Play Store dataset. By leveraging Python and SQL, it offers capabilities for data cleaning, transformation, sentiment analysis, and data visualization.

## Features

- Data ingestion from CSV files
- Data preprocessing and cleaning
- Loading data into a PostgreSQL database
- Sentiment analysis of user reviews
- Data visualization using Matplotlib and Seaborn

## Prerequisites

- Python 3
- PostgreSQL database
- Airflow

## Getting Started

Follow these steps to get the project up and running on your local machine:

1. Clone the repository:

   bash
   git clone https://github.com/DavidePosillipo/develhope_2023_team2.git

2. Activate the virtual environment:

   bash
   source .venv/bin/activate

3. Install the required Python libraries:

    bash
    pip install -r requirements.txt

4. Set up the PostgreSQL database:

   Update the database connection details in the main.py file Line 13:
   db = DB_Handler(database = 'xxx', user = 'xxx', password='xxx', host='xxx', database_name = 'googleplaystore')

5. Set up the Airflow connection:

   Create a new connection in Airflow using the connection details set for the PostgreSQL database.
   You might have to adjust some parameters according to your machine and airflow setup.



