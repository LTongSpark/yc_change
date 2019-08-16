name := "yc"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "com.typesafe" % "config" % "1.2.1"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.1.0"
libraryDependencies += "com.alibaba" % "fastjson" % "1.2.12"
libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.10.0.1"
libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.4.9"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.3"
libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-server-resourcemanager" % "2.7.3"
libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-server-nodemanager" % "2.7.3"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.45"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.6"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.6"

        