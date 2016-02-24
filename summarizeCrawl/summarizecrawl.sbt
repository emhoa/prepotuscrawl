name := "Summarize Crawl"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "1.5.2" % "provided",
        "org.apache.hadoop" % "hadoop-core" % "1.0.0" % "provided",
        "org.apache.hbase" % "hbase-client" % "1.1.2" % "provided",
        "org.apache.hbase" % "hbase-common" % "1.1.2" % "provided",
        "org.apache.hbase" % "hbase-server" % "1.1.2" % "provided"
)

jarName in assembly := "summarize-crawl.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
