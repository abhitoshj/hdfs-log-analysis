#!/bin/bash

start_ssh() {
    echo "Starting SSH service..."
    sudo service ssh start
    if [ $? -eq 0 ]; then
        echo "SSH service started successfully."
    else
        echo "Failed to start SSH service. Please check the logs."
        exit 1
    fi
}

start_hdfs() {
    echo "Starting HDFS services..."
    /usr/local/hadoop/sbin/start-dfs.sh
    if [ $? -eq 0 ]; then
        echo "HDFS services started successfully."
    else
        echo "Failed to start HDFS services. Please check the logs."
        exit 1
    fi
}

start_spark() {
    echo "Starting Spark services..."
    ./spark-3.5.3-bin-hadoop3/sbin/start-all.sh
    if [ $? -eq 0 ]; then
        echo "Spark services started successfully."
    else
        echo "Failed to start Spark services. Please check the logs."
        exit 1
    fi
}

echo "Starting all services..."
start_ssh
start_hdfs
start_spark
echo "All services started successfully!"