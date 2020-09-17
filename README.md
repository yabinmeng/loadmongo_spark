# Overview

This repo demostrates how to use DSE Analytics (Spark) to load data from collections in a MongoDB cluster into a DSE cluster. 

The Spark program reads data from MongoDB using [MongoDB Connector for Spark](https://docs.mongodb.com/spark-connector/master/) and writes data into DSE using DataStax [Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector). 

