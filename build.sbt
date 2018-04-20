name := "csv_reader"

version := "0.0.1"

scalaVersion := "2.11.8"

lazy val sparkVersion = "2.2.0"
libraryDependencies ++= Seq(
  "io.hydrosphere" %% "mist-lib" % "1.0.0-RC14",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.8.0" % "test",
  "org.scalactic" %% "scalactic" % "3.0.5",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)
