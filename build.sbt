name := "Spark_SQL"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.1"

scapegoatVersion in ThisBuild := "1.3.8"
