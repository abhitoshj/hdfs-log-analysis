#!/bin/bash

# Function to stop Spark services
stop_spark() {
    echo "Stopping Spark services..."
    ./spark/spark-3.5.3-bin-hadoop3/sbin/stop-all.sh
    if [ $? -eq 0 ]; then
        echo "Spark services stopped successfully."
    else
        echo "Failed to stop Spark services. Please check the logs."
    fi
}

# Function to stop HDFS service
stop_hdfs() {
    echo "Stopping HDFS services..."
    /usr/local/hadoop/sbin/stop-dfs.sh
    if [ $? -eq 0 ]; then
        echo "HDFS services stopped successfully."
    else
        echo "Failed to stop HDFS services. Please check the logs."
    fi
}

# Function to stop SSH service
stop_ssh() {
    echo "Stopping SSH service..."
    sudo service ssh stop
    if [ $? -eq 0 ]; then
        echo "SSH service stopped successfully."
    else
        echo "Failed to stop SSH service. Please check the logs."
    fi
}

# Main script execution
echo "Stopping all services..."
stop_spark
stop_hdfs
stop_ssh
echo "All services stopped successfully!"