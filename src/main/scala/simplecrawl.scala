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
import org.apache.hadoop.fs.Path

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output

import scala.collection.mutable.ArrayBuffer

object crawlNames {

  def main(args: Array[String]) {

        var startFileNum = 0
        var endFileNum = 0
        var x = 0
        if (args.length > 0) {

                if (args.length > 1) {
                        endFileNum = args(1).toInt
                }
                startFileNum = args(0).toInt
        }

        val crawl_file_locations_dir = "s3a://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2015-48"
        val crawl_file_index = "wet.paths.gz"

        val conf = new SparkConf().setAppName("Crawling for names")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.registerKryoClasses(Array(classOf[TextInputFormat],classOf[LongWritable], classOf[Text]))

        val sc = new SparkContext(conf)

        val awsAccessKeyId = sys.env("AWS_ACCESS_KEY_ID")
        val awsSecretAccessKey = sys.env("AWS_SECRET_ACCESS_KEY")

        // Setting up configuration variables
       val hadoopConf = sc.hadoopConfiguration
        hadoopConf.set("fs.s3a.awsAccessKeyId", awsAccessKeyId)
        hadoopConf.set("fs.s3a.awsSecretAccessKey", awsSecretAccessKey)
        
        // Increase connection configs to prevent S3 socket timeout errors
        hadoopConf.set("fs.s3a.connection.maximum", "500")
        hadoopConf.set("fs.s3a.connection.timeout", "10000")

        val localConfig = new Configuration()
        localConfig.set("textinputformat.record.delimiter", "WARC-Target-URI: ")

        localConfig.set("fs.s3a.awsAccessKeyId", awsAccessKeyId)
        localConfig.set("fs.s3a.awsSecretAccessKey", awsSecretAccessKey)
        localConfig.set("fs.s3a.connection.maximum", "500")
        localConfig.set("fs.s3a.connection.timeout", "10000")
        
        // Grab the file off S3 giving the location of Common Crawl text files
        val gzcrawlFiles = sc.textFile(crawl_file_locations_dir + "/" + crawl_file_index)

        val protocol = "s3a://"
        val crawlHeader = "aws-publicdatasets/"
        val crawlFileIDpattern1 = """^.*/segments/([\s\d\D]+)/wet/.*$""".r
        val crawlFileIDpattern2 = """^.*wet/CC-MAIN-([\d\D]+)-ip.*$""".r

        // Setting variables to be used for caching of RDDs
        var i=1
       var hdFiles = new Array[RDD[(LongWritable, Text)]](3)

        // Iterate through the 35,700 file names
        val allgzcrawlFiles = gzcrawlFiles.collect()

        for (crawlFile <- allgzcrawlFiles) {

                if (x > startFileNum ) {

                val searchFile = protocol + crawlHeader + crawlFile

                val crawlFileIDpattern1(crawlFileID1) = crawlFile
                val crawlFileIDpattern2(crawlFileID2) = crawlFile
                val crawlFileID = crawlFileID1 + crawlFileID2

                val fullCrawlName = searchFile

                // Grab file off Amazon's S3
                val hdFile = sc.newAPIHadoopFile(fullCrawlName, classOf[TextInputFormat],classOf[LongWritable], classOf[Text], localConfig)

                // Hold on to file and process only if we possess three
                hdFiles(i-1) = hdFile

                if (i % 3 == 0) {  // Act only on batches of three RDDs

                        val hdFile = hdFiles(i-3).union(hdFiles(i-2).union(hdFiles(i-1)))
                        // Send the three-large RDD for saving
                        //saveCrawlData(crawlFileID, hdFile)

                        // Reset batch counter
                        i=0
                }
                i = i+1
            }
          x = x+1

        } // end of do for (crawlFile <- allgzcrawlFiles)


  }
}
