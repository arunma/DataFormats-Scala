name := "DataFormats-Scala"

version := "0.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.thrift" % "libthrift" % "0.12.0",
  "org.apache.avro" % "avro" % "1.8.2",
  "org.apache.spark" %% "spark-core" % sparkVersion exclude("org.scalatest", "scalatest_2.11"), //% Provided
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "org.apache.parquet" % "parquet-avro" % "1.8.2",
  "org.apache.parquet" % "parquet-thrift" % "1.8.2",
  "org.apache.parquet" % "parquet-hadoop" % "1.8.2",
  "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.20",
  "org.apache.spark" %% "spark-sql" % sparkVersion //% Provided
)

enablePlugins(ProtobufPlugin)
