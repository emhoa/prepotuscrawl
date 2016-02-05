import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.storage.StorageLevel

import org.apache.spark.rdd.RDD

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{HConnectionManager, Connection, HTable}

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.fs.Path


import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output

object crawlNames {
 
  def main(args: Array[String]) {

	val crawl_file_locations_dir = "s3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2015-48"
	val crawl_file_index = "wet.paths.gz"

	val conf = new SparkConf().setAppName("Crawling for names")
	conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	conf.registerKryoClasses(Array(classOf[TextInputFormat],classOf[LongWritable], classOf[Text]))

	val sc = new SparkContext(conf)

	//val awsAccessKeyId = sys.env("AWS_ACCESS_KEY_ID")

	//val awsSecretAccessKey = sys.env("AWS_SECRET_ACCESS_KEY")

	val awsSecretAccessKey = "MB35BZsiDvnqyZAIW/J308ZAV3OrnnrQbxlQAr3r"
	val awsAccessKeyId = "AKIAIJZWSTLHYX636UEQ"
	val hadoopConf = sc.hadoopConfiguration
	hadoopConf.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)
	hadoopConf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)

	val localConfig = new Configuration()
	localConfig.set("textinputformat.record.delimiter", "WARC-Target-URI: ")
	
	localConfig.set("textinputformat.record.delimiter", "WARC-Target-URI: ")
	localConfig.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)
	localConfig.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)

	val gzcrawlFiles = sc.textFile(crawl_file_locations_dir + "/" + crawl_file_index)
	println("No. files = %d; Wet.paths = %s".format(gzcrawlFiles.count(), gzcrawlFiles))
	
	val hbaseconf = HBaseConfiguration.create(localConfig)	
	val hbaseConn = HConnectionManager.createConnection(hbaseconf)

	hbaseconf.set(TableOutputFormat.OUTPUT_TABLE, "candidate_crawl")
	val hbasejob = Job.getInstance(hbaseconf)
	hbasejob.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
	hbasejob.setMapOutputValueClass(classOf[KeyValue])

	val protocol = "s3n://"
	val crawlHeader = "aws-publicdatasets/"
	val crawlFileIDpattern1 = """^.*/segments/([\s\d\D]+)/wet/.*$""".r
	val crawlFileIDpattern2 = """^.*wet/CC-MAIN-([\d\D]+)-ip.*$""".r


	// crawlFiles.take(1).foreach(printCounts)
	gzcrawlFiles.collect().foreach(parseGzipFile)
//	gzcrawlFiles.foreachPartition(parseGzipFile)

	def parseGzipFile(crawlFile: String) {

		val searchFile = protocol + crawlHeader + crawlFile
		val crawlFileIDpattern1(crawlFileID1) = crawlFile
		val crawlFileIDpattern2(crawlFileID2) = crawlFile
		val crawlFileID = crawlFileID1 + crawlFileID2

		val getLastFourDigitPattern = """^.*-.*-.*-([\d]+)""".r
	//	val getLastFourDigitPattern(crawlFileNum) = crawlFileID
		val fullCrawlName = searchFile


		val crawlData =	sc.newAPIHadoopFile(fullCrawlName, classOf[TextInputFormat],classOf[LongWritable], classOf[Text], localConfig).map(_._2.toString)

		val keyValCrawlData = crawlData.map(x=>(x.split("\n")(0), x))
		// Connect to the table	
		val hbasetable = new HTable(TableName.valueOf("candidate_crawl"), hbaseConn)
		HFileOutputFormat.configureIncrementalLoad(hbasejob, hbasetable)
		val candidates = List("Donald Trump", "Hillary Clinton", "Ted Cruz", "Bernie Sanders")
		for (candidate <- candidates) {
		
			val urls = keyValCrawlData.filter{ case (key, value) => value.contains(candidate) }.keys
//			val numUrls = urls.count
//			println("No. of webpages searched: %d(new)\nNo. of lines candidate %s is mentioned: %d".format(keyValCrawlData.count()-1, candidate, numUrls))

			val candidateSaveToDB = urls.map{ url => {
			val kv: KeyValue = new KeyValue(Bytes.toBytes(url), "cc".getBytes(), candidate.getBytes(), "1".getBytes()).shallowCopy
			(new ImmutableBytesWritable(url.getBytes()), kv)
				}}

			candidateSaveToDB.saveAsNewAPIHadoopFile("/tmp/" + candidate + crawlFileID, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], hbaseconf)
			val bulkLoader = new LoadIncrementalHFiles(hbaseconf)
			bulkLoader.doBulkLoad(new Path("/tmp/" + candidate + crawlFileID), hbasetable)
		}
			
	hbasetable.close
	}		
	hbaseConn.close	
  }
}
