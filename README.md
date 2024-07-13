# de-2024
2024 Data Engineering PF Study 

## Directory Structure

```
| data
  |- your data goes here
| jobs
  |- your pyspark .py files go here
| notebooks
  |- jupyter notebooks for practice go here
| resources
  |- .jars for spark third-party app go here
docker-compose.yml
```

## Preparation
### Docker
(download & use wsl if you use Windows OS)
https://docs.docker.com/engine/install/ubuntu/


## How to run pyspark project

run containers:

``` bash
$ docker-compose up -d

 ✔ Network de-2024_default-network    Created
 ✔ Container de-2024-spark-master-1   Started
 ✔ Container de-2024-jupyter-spark-1  Started
 ✔ Container de-2024-spark-worker-1   Started
```

spark-master UI: localhost:9090

spark-submit:

``` bash
$ docker exec -it de-2024-spark-master-1 spark-submit --master spark://spark-master:7077 jobs/hello-world.py data/<filename>
```

## Jupyter Notebook

test codes in jupyter notebook environment - localhost:8888
