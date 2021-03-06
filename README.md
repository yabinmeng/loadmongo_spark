- [1. Overview](#1-overview)
  - [1.1. Load Sample Data Set into MongoDB](#11-load-sample-data-set-into-mongodb)
- [2. Access MongoDB from DSE Spark Shell](#2-access-mongodb-from-dse-spark-shell)
- [3. Implicitly Infer Schema for MongoDB Data Loading](#3-implicitly-infer-schema-for-mongodb-data-loading)
  - [3.1. Challenges of Writing into C* using Spark Cassandra Connector](#31-challenges-of-writing-into-c-using-spark-cassandra-connector)
- [4. Explicit Schema Specification for MongoDB Data Loading](#4-explicit-schema-specification-for-mongodb-data-loading)
  - [4.1. Challenges of Writing into C* using Spark Cassandra Connector](#41-challenges-of-writing-into-c-using-spark-cassandra-connector)
- [5. Write to C* with a Flattened Schema](#5-write-to-c-with-a-flattened-schema)
- [6. Final Solution with Proper Data Transformation](#6-final-solution-with-proper-data-transformation)
- [7. Execute the Program](#7-execute-the-program)

# 1. Overview

This repo demostrates how to use DSE Analytics (Spark) to load data from collections in a MongoDB cluster into a DSE cluster. 

The Spark program reads data from MongoDB using [MongoDB Connector for Spark](https://docs.mongodb.com/spark-connector/master/) and writes data into DSE using DataStax [Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector). 

The environment setup is as below:
* OS (of all server instances): Ubuntu 16.04.7 LTS (Xenial Xerus)
* One MongoDB version 4.2.9 replica set cluster with 1 primary and 2 secondaries 
* One DSE version 6.8.3 cluster with Analytics enabled
* sbt version: 1.3.13
* Scala version: 2.11.12

## 1.1. Load Sample Data Set into MongoDB

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

# 2. Access MongoDB from DSE Spark Shell

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

# 3. Implicitly Infer Schema for MongoDB Data Loading

MongoDB is a schemaless database. This means that the documents in a collection may have different JSON structures (although they could also follow the same one). On the other side Spark DataFrames and DataSets do require a schema. 

Therefore, when reading MongoDB data into Spark, the Spark Connector automatically infers the schema from some randomly chosen documents and assign the inferred schema to the DataFrame.

In the above example, although the documents in "grades" collection share the same document structure, but since we don't explicitly specify the schema, an inferred schema is assigned to the DataFrame, as below:

```
scala> mongoDF.printSchema()
root
 |-- _id: struct (nullable = true)
 |    |-- oid: string (nullable = true)
 |-- class_id: integer (nullable = true)
 |-- scores: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- type: string (nullable = true)
 |    |    |-- score: double (nullable = true)
 |-- student_id: integer (nullable = true)
```

By the above inferred schema, the "**scores**" column is of the following type:
* ArraryType(StructType)

## 3.1. Challenges of Writing into C* using Spark Cassandra Connector

Assuming in the target C* schema, we only want to keep columns "class_id", "student_id", and "scores", We can try to create a C* table from a DataFrame by utilizing Spark Cassandra Connector's features
. But it fails with "IllegalArgumentException", as below:
```
scala> mongoDF0.drop($"_d").createCassandraTable(
     |     "testks",
     |     "grades",
     |     partitionKeyColumns = Some(Seq("student_id")))
java.lang.IllegalArgumentException: Unsupported type: StructType(StructField(oid,StringType,true))
... ... 
```

The issue here is that Spark Cassandra Connector doesn't support Spark SQL **StructType**.
 
# 4. Explicit Schema Specification for MongoDB Data Loading

Looking at the original document structure, the natural column type for "scores" column would be a List/Arrary of Map items. Based on this understanding, let's explicitly specify the schema when loading the data from MongoDB. 

```
scala> import org.apache.spark.sql.types._
import org.apache.spark.sql.types._

scala> import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._

scala> val scoreMapType = DataTypes.createMapType(StringType, StringType)
scoreMapType: org.apache.spark.sql.types.MapType = MapType(StringType,StringType,true)

scala> val gradesSchema = ( new StructType()
     |     .add("_id", StringType)
     |     .add("class_id", IntegerType)
     |     .add("student_id", IntegerType)
     |     .add("scores", ArrayType(scoreMapType))
     | )
gradesSchema: org.apache.spark.sql.types.StructType = StructType(StructField(_id,StringType,true), StructField(class_id,IntegerType,true), StructField(student_id,IntegerType,true), StructField(scores,ArrayType(MapType(StringType,StringType,true),true),true))

scala> val mongoDF = (
     |     spark.read
     |     .schema(gradesSchema)
     |     .format("com.mongodb.spark.sql.DefaultSource")
     |     .option("database", "mytestdb")
     |     .option("collection", "grades")
     |     .load()
     | )
mongoDF: org.apache.spark.sql.DataFrame = [_id: string, class_id: int ... 2 more fields]
)

scala> mongoDF.printSchema
root
 |-- _id: string (nullable = true)
 |-- class_id: integer (nullable = true)
 |-- student_id: integer (nullable = true)
 |-- scores: array (nullable = true)
 |    |-- element: map (containsNull = true)
 |    |    |-- key: string
 |    |    |-- value: string (valueContainsNull = true)
```

Check the DataFrame schema and we now see that the "**scores**" column is of the following type:
* ArraryType(Map(String, String))

## 4.1. Challenges of Writing into C* using Spark Cassandra Connector

**Map** type is supported in Spark Cassandra Connector, but writing the above DataFrame still triggers an issue, as below:

```
scala> val df = mongoDF.drop($"_id")
df: org.apache.spark.sql.DataFrame = [class_id: int, student_id: int ... 1 more field]

scala> df.createCassandraTable(
     |   "testks",
     |   "grades",
     |   partitionKeyColumns = Some(Seq("student_id")))
com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException: Invalid type list<map<text, text>> for column scores: non-frozen collections are only supported at top-level: subtype map<text, text> of list<map<text, text>> must be frozen
... ...
``` 

This error looks like Spark Cassandra Connector's "createCassandraTable()" function tries to create a table with a column ("scores") of CQL type "***list<map<text, text>>***". 

This is invalid because in C* such a column type (collection within a collection) requires "frozne" keyword like "***list<frozen<map<text, text>>>***".

One workaround here is to create proper C* schema in advance with the right CQL type (e.g. with "frozen" keyword) and the Spark program simply writes data in C* without the need to create the table first.

**NOTE** that there is a JIRA ticket opened to make frozen nested collections.

# 5. Write to C* with a Flattened Schema

In C*, there are several different techniques to do data denormalization. Using collection is one way; but there are some minor caveats associated with it. Another probably better approach is through "clustering" keys, as exampled in the following C* schema:

```
CREATE TABLE testks.grades (
    class_id int,
    student_id int,
    score_type text,
    score_value double,
    PRIMARY KEY ((class_id, student_id), score_type)
```

Based on this understanding, we can do the following data transformation using Spark:

```
// Drop "_id" column 
scala> var df = mongoDF.drop($"_id")
df: org.apache.spark.sql.DataFrame = [class_id: int, student_id: int ... 1 more field]

// Flatten the array
scala> df = df.select($"class_id", $"student_id", explode($"scores") as "score_types")
df: org.apache.spark.sql.DataFrame = [class_id: int, student_id: int ... 1 more field]

// Get the individual items within the map
scala> df = df.select($"class_id", $"student_id", $"score_types.type" as "score_type", $"score_types.score" as "score_value")
df: org.apache.spark.sql.DataFrame = [class_id: int, student_id: int ... 2 more fields]

// Convert the "score_value" to float type
df = df.withColumn("score_value", col("score_value").cast("Float"))
```

At this point, the schema is flattened out and we can call Spark Cassandra Connector functions to create a table and insert data:

```
scala> df.createCassandraTable(
     |     "testks",
     |     "grades2",
     |     partitionKeyColumns = Some(Seq("class_id","student_id")),
     |     clusteringKeyColumns = Some(Seq("score_type")))

scala> (df.write
     | .cassandraFormat("grades2", "testks")
     | .mode("append")
     | save()
     | )
```

Now log into CQLSH and verify the results:

```
cqlsh:testks> select * from grades2 limit 10;

 class_id | student_id | score_type | score_value
----------+------------+------------+-------------------
       20 |         41 |       exam | 89.86568333880862
       20 |         41 |   homework | 41.18037387528079
       20 |         41 |       quiz | 86.67438818752774
        7 |         29 |       exam | 63.15698088911974
        7 |         29 |   homework | 9.362100057782852
        7 |         29 |       quiz | 30.41484529536909
       24 |          0 |       exam | 4.444435759027499
       24 |          0 |   homework | 86.79352850434199
       24 |          0 |       quiz | 28.63057857803885
       16 |         29 |       exam | 91.10262572056217
```

---

![#f03c15](https://via.placeholder.com/15/f03c15/000000?text=+) **NOTE**: In the above flattened C* schema, the primary key is the combination of "class_id", "student_id", and "score_type". This means that for each student within a class, there can only be one record per score type. If the original Mongo collection does have multiple score records under one record type for one student, the above C* schema and transformation will be problematic. 

# 6. Final Solution with Proper Data Transformation

In the original MongoDB collection, the "**scores**" column is an **array** of maps with the following key/value part as an example:
* *Map Item Key*: "type", or "score"
* *Map Item Value*:  "exam", or 78.44172815491468
 
In the target C* table, we want **scores** column to be a **list** of map items with the following format:
* *Map Item Key*: 'exam'
* *Map Value Key*: 78.44172815491468

The corresponding C* table schema would be like below:
```
CREATE TABLE testks.grades (
    class_id int,
    student_id int,
    score_map list<frozen<map<text, float>>>,
    PRIMARY KEY ((class_id, student_id))
)
```

In order to achieve this result, we need to do the following data transformation steps:
```
// Drop MongoDB "_id" pirmary key column
// -- mongoDF is the source data from MongoDB (see early sections)
scala> val df = mongoDF.drop($"_id")
df: org.apache.spark.sql.DataFrame = [class_id: int, student_id: int ... 1 more field]

// Flatten "scores" column from one record having an array of maps to multiple records having one map
scala> val df1 = df.select($"class_id", $"student_id", explode($"scores") as "scores")
df1: org.apache.spark.sql.DataFrame = [class_id: int, student_id: int ... 1 more field] 

scala> df1.printSchema()
root
 |-- class_id: integer (nullable = true)
 |-- student_id: integer (nullable = true)
 |-- scores: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)

scala> df1.show(5)
+--------+----------+--------------------+
|class_id|student_id|              scores|
+--------+----------+--------------------+
|      28|         0|[type -> exam, sc...|
|      28|         0|[type -> quiz, sc...|
|      28|         0|[type -> homework...|
|      28|         0|[type -> homework...|
|      28|         0|[type -> homework...|
+--------+----------+--------------------+

// Only keep map value items (no key items) 
val df2 = df1.select($"class_id", $"student_id", map_values($"scores") as "score_values")
df2: org.apache.spark.sql.DataFrame = [class_id: int, student_id: int ... 1 more field]

scala> df2.printSchema()
root
 |-- class_id: integer (nullable = true)
 |-- student_id: integer (nullable = true)
 |-- score_values: array (nullable = true)
 |    |-- element: string (containsNull = true)

scala> df2.show(5)
+--------+----------+--------------------+
|class_id|student_id|        score_values|
+--------+----------+--------------------+
|      28|         0|[exam, 39.1774940...|
|      28|         0|[quiz, 78.4417281...|
|      28|         0|[homework, 20.817...|
|      28|         0|[homework, 70.445...|
|      28|         0|[homework, 50.666...|
+--------+----------+--------------------+

// Convert to the desired map format
val df3 = df2.select($"class_id", $"student_id", $"score_values"(0) as "score_type", $"score_values"(1) as "score_value").withColumn("score_map", map(col("score_type"), col("score_value"))).drop("score_type", "score_value")

scala> df3.printSchema()
root
 |-- class_id: integer (nullable = true)
 |-- student_id: integer (nullable = true)
 |-- score_map: map (nullable = false)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)

scala> df3.show(5)
+--------+----------+--------------------+
|class_id|student_id|           score_map|
+--------+----------+--------------------+
|      28|         0|[exam -> 39.17749...|
|      28|         0|[quiz -> 78.44172...|
|      28|         0|[homework -> 20.8...|
|      28|         0|[homework -> 70.4...|
|      28|         0|[homework -> 50.6...|
+--------+----------+--------------------+

// Consolidate multiple map itemss (from multiple rows) into a list of maps (under one row) 
scala> val df4 = df3.groupBy("class_id", "student_id").agg(collect_list("score_map") as "score_map")
df4: org.apache.spark.sql.DataFrame = [class_id: int, student_id: int ... 1 more field]

scala> df4.printSchema()
root
 |-- class_id: integer (nullable = true)
 |-- student_id: integer (nullable = true)
 |-- score_map: array (nullable = true)
 |    |-- element: map (containsNull = true)
 |    |    |-- key: string
 |    |    |-- value: string (valueContainsNull = true)

scala> df4.show(5)
+--------+----------+--------------------+
|class_id|student_id|           score_map|
+--------+----------+--------------------+
|       3|        30|[[exam -> 62.5280...|
|       0|        25|[[exam -> 60.2918...|
|      11|        23|[[exam -> 92.6824...|
|      16|         3|[[exam -> 0.59987...|
|      30|         0|[[exam -> 14.3434...|
+--------+----------+--------------------+
```

At this point, we're ready to write the transformed data into C*, following the target schema

```
// Write data into C*
scala> (df4.write
     | .cassandraFormat("grades", "testks")
     | .mode("append")
     | save()
     | )
``` 

Now let's verify the result, using one document ("student_id": 29, "class_id": 7) from MongoDB as an example.

* Check the source document content from MongoDB
```
rs0:PRIMARY> db.grades.find( {student_id:{$eq:29}, class_id:{$eq:7}} ).pretty()
{
	"_id" : ObjectId("50b59cd75bed76f46522c3f1"),
	"student_id" : 29,
	"class_id" : 7,
	"scores" : [
		{
			"type" : "exam",
			"score" : 63.15698088911974
		},
		{
			"type" : "quiz",
			"score" : 30.41484529536909
		},
		{
			"type" : "homework",
			"score" : 9.362100057782852
		},
		{
			"type" : "homework",
			"score" : 4.489357836698893
		}
	]
}
```

* Verify the target data content inserted in C*
```
cqlsh:testks> select * from grades where class_id = 7 and student_id = 29;

 class_id | student_id | score_map
----------+------------+---------------------------------------------------------------------------------------
        7 |         29 | [{'exam': 63.15698}, {'quiz': 30.41484}, {'homework': 9.3621}, {'homework': 4.48936}]
```

# 7. Execute the Program

* Use the following command to build a Uber jar ("loadmongo-assembly-1.0.jar")
```
$ sbt clean assembly 
```

* Once the Uber jar file is created, copy it to some location on the Spark master node in the DSE cluster. 

* Then run the following command to submit the Spark job (assuming the submitting client machine is remotely outside the DSE cluster).
```
$ dse spark-submit --master dse://<dse_node_ip>:9042 --deploy-mode cluster --class com.example.loadmongo <some_location>/loadmongo-assembly-1.0.jar