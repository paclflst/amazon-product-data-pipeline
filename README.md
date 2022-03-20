## Amazon Product Data Pipeline

This is a demo a data pipeline to import sample [Amazon product data](http://jmcauley.ucsd.edu/data/amazon/links.html) into postgres db

The solution consists of 4 parts:
- Airflow to orchestrate the process and start jobs
- Spark to process downloaded data and aggregate it for data marts
- Postgres db to accomodate the data
- Flask web api (simple application) to display transforamtion/aggregation results
- Jupyter notebook to explore Spark transformations (uncomment in docker-compose file)

### Prerequisites

- Install [Docker](https://www.docker.com/)
- Install [Docker Compose](https://docs.docker.com/compose/install/)


### Usage

Start needed services with docker-compose (since the project contains Spark installation please allow at least 6GiB of memory for Docker needs)

```shell
docker-compose -f docker/docker-compose.yml up -d
```

To execute the import use Airflow web UI:

```shell
http://localhost:8282/  
```


### Database
To connect to database and check the results use 

```shell
psql -U airflow -h localhost -p 5432 -d test
```
then enter *airflow*

### Logging
Solution logs can be accessed in *logs* folder