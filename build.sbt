name := "OpenWeatherMapAPI"

version := "0.1"

scalaVersion := "2.12.14"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-scala" % "1.13.2",
  "org.apache.flink" %% "flink-streaming-scala" % "1.13.2",
  "org.json4s" %% "json4s-native" % "4.0.3",
  "com.lihaoyi" %% "requests" % "0.6.6",
  "org.slf4j" % "slf4j-log4j12" % "1.7.32",
  "org.apache.flink" %% "flink-clients" % "1.13.2",
  "com.typesafe.akka" %% "akka-http" % "10.2.6",
  "org.apache.flink" %% "flink-connector-kafka" % "1.13.2",
  "org.apache.kafka" % "kafka-clients" % "2.8.0"

)
