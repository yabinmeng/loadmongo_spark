package com.example

import java.util.logging.{Level, Logger}

import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import com.mongodb.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._

object loadmongo extends App {

  /**
   * Test direct client connection to MongoDB (not through Spark)
   */
  /*  def TestRegMongoClient(): Unit = {
      val mongoLogger = Logger.getLogger("org.mongodb.driver")
      mongoLogger.setLevel(Level.WARNING)

      val client: MongoClient = MongoClients.create(mongoURI)
      val db: MongoDatabase = client.getDatabase(mongoDBName)
      val collNames = db.listCollectionNames().iterator()

      while (collNames.hasNext) {
        System.out.println(collNames.next())
      }
    }*/
  //TestRegMongoClient()


  // On-prem Mongo server (replica set) connection URL
  val mongoURI =
    "mongodb://10.101.35.51:27017,10.101.35.51:27018,10.101.35.51:27019/"
  System.setProperty("org.mongodb.async.type", "netty")

  val dseSrvIp = "10.101.33.244"
  val dseSrvPort = "9042"
  val dseSparkMasterUrl = "dse://" +  dseSrvIp + ":" + dseSrvPort

  val mongoConf = new SparkConf()
    .set("spark.mongodb.input.uri", mongoURI)
  val dseConf = new SparkConf()
    .set("spark.cassandra.connection.host", dseSrvIp)
    .set("spark.cassandra.connection.port", dseSrvPort)

  val spark = SparkSession.builder()
    .appName("MongoToDSE_SupplySales")
    .master(dseSparkMasterUrl)
    .config(mongoConf)
    .config(dseConf)
    .getOrCreate();
  import spark.implicits._

  val scoreMapType = DataTypes.createMapType(StringType, StringType)
  val gradesSchema = ( new StructType()
    .add("_id", StringType)
    .add("class_id", IntegerType)
    .add("student_id", IntegerType)
    .add("scores", ArrayType(scoreMapType))
    )

  //val mongoDF = MongoSpark.load(spark)
  val mongoDF = spark.read
    .schema(gradesSchema)
    .format("com.mongodb.spark.sql.DefaultSource")
    .option("database", "mytestdb")
    .option("collection", "grades")
    .load()
    .drop("_id")

  //--> Debug purpose
  //mongoDF.explain()
  //mongoDF.printSchema()

  // Drop Mongo document "_id" column
  var df = mongoDF.drop($"_id")

  // Flatten "scores" arrary
  df = df.select($"class_id", $"student_id", explode($"scores") as "score_types")

  // Get individual score type and value columns
  df = df.select($"class_id", $"student_id", $"score_types.type" as "score_type", $"score_types.score" as "score_value")

  // Convert score value to the right type
  df = df.withColumn("score_value", col("score_value").cast("Float"))

  df.printSchema()

  // Create C* table
  df.createCassandraTable(
    "testks",
    "grades_c",
    partitionKeyColumns = Some(Seq("class_id"," student_id")),
    clusteringKeyColumns = Some(Seq("score_type")))

  // Write to C* table
  df.write
    .cassandraFormat("grades_c", "testks")
    .mode("append")
    .save()

  System.exit(0)
}
