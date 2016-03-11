name := """data-player"""

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor" % "2.3.11",
    "joda-time" % "joda-time" % "2.9.2",
    "org.joda" % "joda-convert" % "1.8.1",
    "org.apache.kafka" % "kafka-clients" % "0.8.2.1",
    "ch.qos.logback" % "logback-classic" % "1.1.3",

    "io.spray" %% "spray-can" % "1.3.3",

    "com.typesafe.akka" %% "akka-testkit" % "2.3.11" % "test",
    "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)


fork in run := true
