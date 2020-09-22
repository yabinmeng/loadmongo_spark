lazy val root = (project in file(".")).
  settings(
    name := "loadmongo",
    version := "1.0",
    scalaVersion := "2.11.12",
    mainClass in Compile := Some("com.example.loadmongo")
  )

resolvers += "DataStax Repo" at "https://repo.datastax.com/public-repos/"
val dseVersion = "6.8.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.7" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided",

  "com.datastax.dse" % "dse-spark-dependencies" % dseVersion % "provided",
  //"com.datastax.spark" %% "spark-cassandra-connector" % "2.5.1",

  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.2",

  "com.typesafe" % "config" % "1.4.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}