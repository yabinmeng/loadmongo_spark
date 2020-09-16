package com.example

import java.util.logging.{Level, Logger}

import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import com.mongodb.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra.DataFrameReaderWrapper

object loadmongo extends App {

  // On-prem Mongo server (replica set) connection URL
  val mongoURI =
    "mongodb://10.101.35.51:27017,10.101.35.51:27018,10.101.35.51:27019/"
  val mongoDBName = "mytestdb"
  val mongoCollName = "resturants"

  System.setProperty("org.mongodb.async.type", "netty")

  /**
   * Test direct client connection to MongoDB (not through Spark)
   */
  def TestRegMongoClient(): Unit = {
    val mongoLogger = Logger.getLogger("org.mongodb.driver")
    mongoLogger.setLevel(Level.WARNING)

    val client: MongoClient = MongoClients.create(mongoURI)
    val db: MongoDatabase = client.getDatabase(mongoDBName)
    val collNames = db.listCollectionNames().iterator()

    while (collNames.hasNext) {
      System.out.println(collNames.next())
    }
  }
  //TestRegMongoClient()

  val dseSrvIp = "10.101.33.244"
  val dseSrvPort = "9042"

  val mongoAtlasConf = new SparkConf()
    .set("spark.mongodb.input.uri", mongoURI)
    //.set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.2")
  val dseConf = new SparkConf()
    .set("spark.cassandra.connection.host", dseSrvIp)
    .set("spark.cassandra.connection.port", dseSrvPort)

  val dseSparkMasterUrl = "dse://" +  dseSrvIp + ":" + dseSrvPort

  val spark = SparkSession.builder()
    .appName("MongoToDSE_SupplySales")
    .master(dseSparkMasterUrl)
    .config(mongoAtlasConf)
    .config(dseConf)
    .getOrCreate();
  import spark.implicits._

  //val mongoDF = MongoSpark.load(spark)
  val mongoDF = spark.read
    .format("com.mongodb.spark.sql.DefaultSource")
    .option("database", mongoDBName)
    .option("collection", mongoCollName)
    .load()

  mongoDF.explain()
  mongoDF.printSchema()
  mongoDF.show()

/*  val dseDF = spark.read
    .cassandraFormat("film_actor", "testks")
    .load()
    .filter($"film_id".between(20,25))

  dseDF.explain()
  dseDF.printSchema()
  dseDF.show()*/

  System.exit(0)
}
