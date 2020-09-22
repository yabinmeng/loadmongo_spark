package com.example

import java.util.logging.{Level, Logger}

import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import com.mongodb.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

object loadmongo extends App {

  val mongoDBName = "mytestdb"
  val mongoCollName = "grades"

  val cassKSName = "testks"
  // C* table name for simple-flatten writes
  val cassTblName_sf = "grades_cf"
  // C* table name for list-of-map writes
  val cassTblName_lm = "grades_lm"

  /**
   * Load MongoDB data; flatten the document array; and write to C*
   */
  def SimpleFlattenWriteCass(mongoDF: DataFrame): Unit = {

    val df = mongoDF
      .select(
        col("class_id"),
        col("student_id"),
        explode(col("scores")) as "score_types"
      )
      .select(
        col("class_id"),
        col("student_id"),
        col("score_types.type") as "score_type",
        col("score_types.score") as "score_value"
      )
      .withColumn("score_value", col("score_value").cast("Float"))

    df.printSchema()

    // Create C* table
    df.createCassandraTable(
      cassKSName,
      cassTblName_sf,
      partitionKeyColumns = Some(Seq("class_id", "student_id")),
      clusteringKeyColumns = Some(Seq("score_type")))

    // Write to C* table
    df.write
      .cassandraFormat(cassTblName_sf, cassKSName)
      .mode("append")
      .save()
  }

  /**
   * Load MongoDB data; flatten the document array; and write to C*
   */
  def ListOfMapWriteCass(mongoDF : DataFrame): Unit = {

    val df = mongoDF
      .select(
        col("class_id"),
        col("student_id"),
        explode(col("scores")) as "scores"
      )
      .select(
        col("class_id"),
        col("student_id"),
        map_values(col("scores")) as "score_values"
      )
      .select(
        col("class_id"),
        col("student_id"),
        col("score_values")(0) as "score_type",
        col("score_values")(1) as "score_value"
      )
      .withColumn(
        "score_map",
        map(col("score_type"), col("score_value"))
      )
      .drop("score_type", "score_value")
      .groupBy("class_id", "student_id")
      .agg(collect_list("score_map") as "score_map")

    df.printSchema()

    // Write to C* table
    df.write
      .cassandraFormat(cassTblName_lm, cassKSName)
      .mode("append")
      .save()
  }


  // On-prem Mongo server (replica set) connection URL
  val mongoURI = 
    "mongodb://<mongo_srv_ip>:27017,<mongo_srv_ip>:27018,<mongo_srv_ip>:27019/"
  System.setProperty("org.mongodb.async.type", "netty")

  val dseSrvIp = "<dse_srv_ip>"
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
    .getOrCreate()
  import spark.implicits._

  // Explicit Spark schema definition
  val scoreMapType = DataTypes.createMapType(StringType, StringType)
  val gradesSchema = new StructType()
    .add("_id", StringType)
    .add("class_id", IntegerType)
    .add("student_id", IntegerType)
    .add("scores", ArrayType(scoreMapType))

  //val mongoDF = MongoSpark.load(spark)
  val mongoDF = spark.read
    .schema(gradesSchema)
    .format("com.mongodb.spark.sql.DefaultSource")
    .option("database", mongoDBName)
    .option("collection", mongoCollName)
    .load()
    .drop("_id")
  //--> Debug purpose
  //mongoDF.explain()
  //mongoDF.printSchema()

  /**
   * Transform and Write to C* using simple-flatten method
   */
  //SimpleFlattenWriteCass(mongoDF)

  /**
   * Transform and Write to C* using list-map method
   */
  // Create keyspace and table
  CassandraConnector(spark.sparkContext).withSessionDo { session =>
    session.execute(
      "CREATE TABLE IF NOT EXISTS " + cassKSName + "." + cassTblName_lm + " ( " +
        "class_id int, " +
        "student_id int, " +
        "score_map list<frozen<map<text, float>>>, " +
        "PRIMARY KEY ((class_id, student_id)))"
    )
  }
  ListOfMapWriteCass(mongoDF)

  spark.stop()
  System.exit(0)
}



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
