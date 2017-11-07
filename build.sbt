name := "HW1scala"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion
)
libraryDependencies += "com.databricks" % "spark-xml_2.11" % "0.4.1"