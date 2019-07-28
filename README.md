# Sparkify Data Warehouse

## Project Description

Sparkify is a grower music streaming startup which aims to move their processes and data onto the cloud. Their data resides in S3, in two directories consisting of a JSON logs on the app users activities, as well as a JSON metadata with songs in their app.

This is an example of what a single song file looks like:
<html>
    <head>
        {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
    </head>
</html>

This is an example of what a single log file looks like:
![log dat](/log-data.png)

## ETL Pipeline

This repository contains an ETL pipeline that extracts their data from those JSON files in S3, stages them in Redshift, and transforms data into a set of dimensional tables for the analytics team to continue finding insights in what songs their users are listening to, as can be seen below:

![data chart](/data-chart.png)

## Schema definition

## Configuration and SETUP

<ul>
<li>Step 1</li>
    <ul>
<li>Create a new IAM user in your AWS account.</li>
<li>Give it AdministratorAccess and Attach policies.</li>
<li>Use access key and secret key to create clients for EC2, S3, IAM, and Redshift.</li>
    </ul>
<li>Step 2</li>
    <ul>
<li>See doc IAM Role. </li>
<li>Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly). </li>
    </ul>
<li>Step 3</li>
    <ul>
<li>See doc Create Cluster. </li>
<li>Create a RedShift Cluster and get the DWH_ENDPOIN(Host address) and DWH_ROLE_ARN and fill the config file. </li>
    </ul>
</ul> 

## Files

<ol>
    <li>sql_queries.py: This file contains all the queries to create, insert data onto and drop the tables.</li>
    <li>create_tables.py: Contains functions to open connection with the cluster, drop the tables if they exist and create them.</li>
    <li>etl.py: Get data from S3, stage them into Redshift and insert into the final tables.</li>
    <li>dwf.cfg: Contains the parameters of the cluster, IAM role and path to JSON files.<li>
</ol>

## How to run

<html>
    <head>
         python3 create_tables.py
    </head>
</html>

<html>
    <head>
         python3 etl.py
    </head>
</html>
