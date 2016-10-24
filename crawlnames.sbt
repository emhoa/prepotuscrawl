name := "Crawling for names"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.5.2" % "provided",
    "org.apache.hadoop" % "hadoop-core" % "1.0.0" % "provided"
)

jarName in assembly := "prepotuscrawl.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

