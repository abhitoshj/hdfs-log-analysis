# Installation Guide

The following components need to be installed in order to run this project. 
- Apache Hadoop 
- Apache Spark
- Apache Kafka
- Influx DB
- Grafana

The subsequent sections describe the installation steps for these components on Ubuntu 22.04 running in WSL.

## Apache Hadoop Installation ##

The steps to install a single node Hadoop cluster can be found at [this](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html) link

## Apache Spark Installation ##

- Download Spark using the following link:
  ```bash
  curl --location -o https://dlcdn.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
  ```

- Extract the download binary
  ```bash
  tar -xzf spark-3.5.3-bin-hadoop3.tgz
  ```

## Apache Kafka Installation ##

- Download Kafka using the following command
  ```bash
  curl --location -o https://dlcdn.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz
  ```

- Extract the downloaded binary
  ```bash
  tar -xzf kafka_2.13-3.9.0.tgz
  ```

- Change the working directory and provide execution permissions
  ```bash
  cd ./kafka_2.13-3.9.0/bin
  chmod -R +x ./kafka_2.13-3.9.0/bin
  ```

- Start Zookeeper
  ```bash
  ./kafka-server-start.sh ../config/server.properties
  ```

- Start Kafka broker
  ```bash
  ./zookeeper-server-start.sh ../config/zookeeper.properties
  ```


## InfluxDB Installation ##

- Download the InfluxDB package for amd64 systems.
  ```bash
  curl --location -O https://download.influxdata.com/influxdb/releases/influxdb2-2.7.10_linux_amd64.tar.gz
  ```

- Extract the downloaded binary
  ```bash
  tar xvzf ./influxdb2-2.7.10_linux_amd64.tar.gz
  ```

- Start InfluxDB server
  ```bash
  ./influxdb2-2.7.10/usr/bin/influxd
  ```

- Access InfluxDB dashboard at [this](http://localhost:8086/) link.
  
## Grafana Installation ##

- Install the prerequisite packages
  ```bash
  sudo apt-get install -y apt-transport-https software-properties-common wget
  ```

- Run the following command to update the list of available packages
  ```bash
  sudo apt-get update
  ```

- To install Grafana OSS, run the following command:
  ```bash
  sudo apt-get install grafana
  ```

- Once, Grafana is installed, we can start the Grafana server in WSL with the following command
  ```bash
  sudo service grafana-server start
  ```

- In your browser, the Grafana Dashboard can be accessed using [this](http://localhost:3000) link.