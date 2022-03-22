## Amazon Product Data Pipeline

This is a demo a data pipeline to import sample [Amazon product data](http://jmcauley.ucsd.edu/data/amazon/links.html) into postgres db

The solution consists of 4 parts:
- Airflow to orchestrate the process and start jobs
- Spark to process downloaded data and aggregate it for data marts
- Postgres db to accomodate the data
- Flask web api (simple application) to display transforamtion/aggregation results
- Jupyter notebook to explore Spark transformations (uncomment in docker-compose file)

![alt text](https://github.com/paclflst/amazon-product-data-pipeline/blob/main/images/prj_setup_schema.png?raw=true)

### Prerequisites

- Install [Docker](https://www.docker.com/)
- Install [Docker Compose](https://docs.docker.com/compose/install/)


### Usage

Start needed services with docker-compose (since the project contains Spark installation please allow at least 6GiB of memory for Docker needs)

```shell
$ docker-compose -f docker/docker-compose.yml up -d
```

To execute the import use Airflow web UI to trigger amazon-product-data-pipeline DAG:

[http://localhost:8282/](http://localhost:8282/)

![alt text](https://github.com/paclflst/amazon-product-data-pipeline/blob/main/images/dag_main_screen.png?raw=true)

or use command

```shell
$ docker-compose -f docker/docker-compose.yml run airflow-webserver \
    airflow trigger_dag amazon-product-data-pipeline 
```

### Web API
To check import results use simple web API to see top 5, bottom 5 and top 5 most imporved rating wise movies for a given month:

[http://localhost:5000/](http://localhost:5000/)

note that URL parameters can be provided:
```shell
http://localhost:5000/?month=2013-05&items_per_list=5
```

### Database
To connect to database and check the results use 

```shell
$ psql -U airflow -h localhost -p 5432 -d test
```
then enter *airflow*

<img src="https://github.com/paclflst/amazon-product-data-pipeline/blob/main/images/db_schema.png?raw=true" width="500">

### Logging
Solution logs can be accessed in *logs* folder

### Useful urls
*Airflow:* [http://localhost:8282/](http://localhost:8282/)

*Spark Master:* [http://localhost:8181/](http://localhost:8181/)

*Jupyter Notebook:* [http://127.0.0.1:8888/](http://127.0.0.1:8888/)

- For Jupyter notebook, you must copy the URL with the token generated when the container is started and paste in your browser. The URL with the token can be taken from container logs using:

```shell
$ docker logs -f docker_jupyter-spark_1
```