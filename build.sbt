name := "myspark"

version := "1.0.0"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.1.3" % "provided"
)
