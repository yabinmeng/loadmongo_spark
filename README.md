# Overview

This repo demostrates how to use DSE Analytics (Spark) to load data from collections in a MongoDB cluster into a DSE cluster. 

The Spark program reads data from MongoDB using [MongoDB Connector for Spark](https://docs.mongodb.com/spark-connector/master/) and writes data into DSE using DataStax [Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector). 

The environment setup is as below:
* OS (of all server instances): Ubuntu 16.04.7 LTS (Xenial Xerus)
* One MongoDB version 4.2.9 replica set cluster with 1 primary and 2 secondaries 
* One DSE version 6.8.3 cluster with Analytics enabled
* sbt version: 1.3.13
* Scala version: 2.11.12

## Load Sample Data Set into MongoDB

In this repo, we're going to use a sample data set from the following website as the source data we're going to load from MongoDB into DSE/C*. 
* https://github.com/ozlerhakan/mongodb-json-files

In particular, the "grades" dataset is used as our source data
* https://github.com/ozlerhakan/mongodb-json-files/blob/master/datasets/grades.json

Once we downloaded the dataset, run "**mongoimport**" command to load it into MongoDB (**NOTE**: port 27019 is the MongoDB primary server port number)
```
$ mongoimport --port 27019 --db mytestdb --collection grades --drop --file ./grades.json
2020-09-17T01:44:54.345+0000	connected to: mongodb://localhost:27019/
2020-09-17T01:44:54.346+0000	dropping: mytestdb.grades
2020-09-17T01:44:54.399+0000	280 document(s) imported successfully. 0 document(s) failed to import
``` 

After bulk loading the sample data set into the MongoDB database, let's verify its document structure. 
```
rs0:PRIMARY> db.grades.find().limit(1).pretty()
{
	"_id" : ObjectId("50b59cd75bed76f46522c34f"),
	"student_id" : 0,
	"class_id" : 28,
	"scores" : [
		{
			"type" : "exam",
			"score" : 39.17749400402234
		},
		{
			"type" : "quiz",
			"score" : 78.44172815491468
		},
		{
			"type" : "homework",
			"score" : 20.81782269075502
		},
		{
			"type" : "homework",
			"score" : 70.44520452408949
		},
		{
			"type" : "homework",
			"score" : 50.66616327819226
		},
		{
			"type" : "homework",
			"score" : 53.84983118363991
		}
	]
}
```  

**NOTE** that the documents in this collection have a homogeneous structures and therefore we can check the collection schema by querying one single document. For collections that have heterogeneous document structure, we can use specialized MongoDB schema analyzer tools like [MongoEye](https://github.com/mongoeye/mongoeye) or [Variety](https://github.com/variety/variety).   

# Access MongoDB from DSE Spark Shell

Now we have the source data in MongoDB, let's verify the connection from the DSE Analytics (Spark) cluster to the MongoDB cluster. The easiest way to do so is through Spark shell. 

When we start a Spark Shell in the DSE Analytics cluster, we need to make sure MongoDB Spark Connector libraries are included and visible to Spark driver and executors. Otherwise, we'll fail to retrieve data from MongoDB.

```
$ dse spark --conf "spark.mongodb.input.uri=mongodb://<mongodb_primary_srv_ip>/?readPreference=primaryPreferred" \
            --conf "spark.jars.packages=org.mongodb.spark:mongo-spark-connector_2.11:2.4.2" \
            --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.2
```     

There are a few things to note here:
* **spark.mongodb.input.uri** configuration specifies the connection URI for MongoDB
* **spark.jar.packages** tells Spark driver and executors to include MongoDB Spark Connector library in their classpaths
* **packages** option tells Spark shell to get and (if needed, to download) MongoDB Spark Connector library.

After getting into Spark Shell REPL, run the following commands to verify data read from MongoDB:
```
scala> val mongoDF = (
     |     spark.read
     |     .format("com.mongodb.spark.sql.DefaultSource")
     |     .option("database", "mytestdb")
     |     .option("collection", "grades")
     |     .load()
     | )
mongoDF: org.apache.spark.sql.DataFrame = [_id: struct<oid: string>, class_id: int ... 2 more fields]

scala> mongoDF.show(1)
+--------------------+--------+--------------------+----------+
|                 _id|class_id|              scores|student_id|
+--------------------+--------+--------------------+----------+
|[50b59cd75bed76f4...|      27|[[exam, 60.194736...|         0|
+--------------------+--------+--------------------+----------+
only showing top 1 row
```    