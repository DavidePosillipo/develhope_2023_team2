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

4. Start PostgreSQL

   For wsl or Linux:
   sudo service postgresql start

   Windows:
   Open the Start menu and search for "Services" or "Services.msc".
   Open the "Services" application.
   Scroll down and locate the PostgreSQL service in the list. The service name may vary depending on the version you have installed (e.g., "PostgreSQL Server", "postgresql-x64-13").
   Right-click on the PostgreSQL service and select "Start" or "Restart" from the context menu.
   Wait for the PostgreSQL service to start. You may see a status change or progress indicator in the Services application.
   Once the PostgreSQL service has started successfully, you can proceed with your database operations or connect to the database using the appropriate client or tools.

   Mac:
   brew services start postgresql
   
5. Set up the PostgreSQL database:

   Update the database connection details in the main.py file Line 13:
   db = DB_Handler(database = 'xxx', user = 'xxx', password='xxx', host='xxx', database_name = 'googleplaystore')

   I suggest you use these inputs, just update the password with the one set up when installing Postgres:
   db = DB_Handler(database = 'postgres', user = 'postgres', password='xxx', host='localhost', port=543x, database_name = 'googleplaystore')

6. Set up the Airflow connection:

   Create a new connection in Airflow using the connection details set for the PostgreSQL database.
   You might have to adjust some parameters according to your machine and airflow setup.