name := "spark-job-timeout-listener"

version := "0.1"

scalaVersion := "2.12.15"

idePackagePrefix := Some("io.iftech")

val sparkVersion = "3.2.1"

libraryDependencies ++= List(
  "org.apache.spark" %% "spark-core" % sparkVersion,
)
