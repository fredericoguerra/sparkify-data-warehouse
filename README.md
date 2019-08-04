# Sparkify Data Warehouse

## Project Description

Sparkify is a grower music streaming startup which aims to move their processes and data onto the cloud. Their data resides in S3, in two directories consisting of a JSON logs on the app users activities, as well as a JSON metadata with songs in their app.

This is an example of what a single song and log files looks like:

### Songs:
Each song json file contains information about the song and artist such as the title, artist Id, location, name and duration of the song.
```python
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```
### Logs:
Each line of this json brings information about the users (name, gender, location and account payment level), artists, song's title, page in the app and time.
![log dat](/images/log-data.png)


## ETL Pipeline

This repository contains an ETL pipeline that extracts their data from those JSON files in S3, stages them in Redshift, and transforms data into a set of dimensional tables for the analytics team to continue finding insights in what songs their users are listening to, as can be seen below:

![data chart](/images/data-chart.png)

The data warehouse is based on PySpark (Apache Spark with Python) in order to optimize the ETL proccess in speed and flexibilty in management of the cloud-based-infraestructure. The Redshift choice give to the sparkify analytics team high perfomance in query proccess, avoid worries about hardware maintenance, the facility to move the data from S3 to Redshift and possibility to scale up clusters in future.

## Schema definition

![schema](/images/schema.png)

## Configuration and SETUP

<ul>
<li>Step 1</li>
    <ul>
<li>Create a new [IAM user](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html) in your AWS account.</li>
<li>Give it [Administrator Access](https://docs.aws.amazon.com/IAM/latest/UserGuide/getting-started_create-admin-group.html) and Attach policies.</li>
<li>Use access key and secret key to create clients for EC2, S3, IAM, and Redshift.</li>
    </ul>
<li>Step 2</li>
    <ul>
<li>See doc [IAM Role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html). </li>
<li>Create an IAM Role that makes [Redshift](https://aws.amazon.com/pt/redshift/) able to access S3 bucket (ReadOnly). </li>
    </ul>
<li>Step 3</li>
    <ul>
<li>See doc [Create Cluster](https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-launch-sample-cluster.html). </li>
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

## How to run and create the database

```
         python3 create_tables.py
```
```
         python3 etl.py
```

## Example queries

### 10 Most Popular songs over time

```
        SELECT sp.title, count(*) as count
        FROM songplays sp
        INNER JOIN songs s ON s.song_id = sp.song_id
        GROUP BY s.title
        ORDER BY count DESC, s.title ASC
        LIMIT 10
```

### 10 Most Popular artists and their songs over time

```
        SELECT ar.name, s.title, count(*) as count
        FROM songplays sp
        INNER JOIN songs s ON s.song_id = sp.song_id
        INNER JOIN artists ar ON ar.artist_id = sp.artist_id
        GROUP BY ar.name, s.title
        ORDER BY count DESC, ar.name, s.title ASC;
```
