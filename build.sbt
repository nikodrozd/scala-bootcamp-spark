name := "scala-bootcamp-spark"

version := "0.1"

scalaVersion := "2.12.14"

libraryDependencies ++= Seq (
  "org.scalatest" %% "scalatest" % "3.1.1" % "test",
  "org.mockito" %% "mockito-scala" % "1.15.0" % "test",
  "org.apache.spark" %% "spark-core" % "3.1.2"
)