# ASOS Big Data Engineer Code Test

# Objective:

To create a data ingestion pipeline using PySpark for the given MOVIELENS dataset.

# Overview:

## movieLensETL.ipynb 

This is a Databricks notebook written in PySpark. 

In this notebook initially various file operations such as Importing Libraries and Global Variable Declarations are done. 

## Data Ingestion:

Then we ingest the data as dataframes and specified schema to maintain consistency across loads and casted to the relevant data types.

## Data Load to Staging:

Here we are creating the delta lake if not exists otherwise upserting based on the key columns.

Paritioning the ratings data based on Movie Id since in our transformation layer we will be using this as join condition.

## Data Transformation:

Here we are unpivoting Data to hold one genre per row and writing it in Parquet format. Since in the requirements file it is mentioned to save results in any format.

Finally Calculating the top 10 movies based on the Average Ratings. Here aggregating the ratings data frame before joining with movies data for optimization

# Prerequisites

## Databricks

A Databricks subscription with at least contributor access required.

A cluster configured with 9.1 LTS (includes Apache Spark 3.1.2, Scala 2.12)

## Spark configuration

To fine tune Spark jobs, I'm providing a custom Delta lake Spark configuration properties in the cluster configuration.

spark.databricks.delta.preview.enabled true

## Environment variables

PYSPARK_PYTHON=/databricks/python3/bin/python3

# Datafiles:

The given data set consists of:

	* 100,000 ratings (1-5) from 943 users on 1682 movies. 
	
	* Each user has rated at least 20 movies. 
	
        * Simple demographic info for the users (age, gender, occupation, zip)
        
(Download u.data (Ratings Data), u.item (Movies Data) from MOVIELENS DATASET - https://grouplens.org/datasets/movielens/100k/)

# Building:
Step 1 - Create a cluster and notebooks in databricks environment.

Step 2 - Upload the data files into databricks filesystem and copy the path file where the data is located. Click on upload data and specify the DBFS Target directory and drop the data files to the specific location to upload it.

Step 3 – Copy the contents of the .py files and update the data file loctions where required.
```py
File locations
moviesFile = "dbfs:/FileStore/shared_uploads/movieLens/u-1.item"
ratingsFile = "dbfs:/FileStore/shared_uploads/movieLens/u-2.data"
ratings_delta = "dbfs:/FileStore/shared_uploads/dataLakeML/ratings"
movies_delta = "dbfs:/FileStore/shared_uploads/dataLakeML/movies"
moviesNgenres = "dbfs:/FileStore/shared_uploads/dataLakeML/moviesNgenres"
top10movies = "dbfs:/FileStore/shared_uploads/dataLakeML/top10movies"
```

Step 4 - Run the Notebook to get the required results in each stages.
