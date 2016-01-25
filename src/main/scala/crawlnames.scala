import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.storage.StorageLevel

import org.apache.spark.rdd.RDD

object crawlNames {

 def main(args: Array[String]) {

//	val crawl_file_locations_dir = "./crawl_file_locations"
	val crawl_file_locations_dir = "s3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2015-48"
	val crawl_file_index = "wet.paths.gz"
	val conf = new SparkConf().setAppName("Crawling for names")
	val sc = new SparkContext(conf)
	val i=1

	val crawlFiles = sc.textFile(crawl_file_locations_dir + "/" + crawl_file_index)
	println("Wet.paths = %s".format(crawlFiles))
	val crawlFile = crawlFiles.take(i)
	crawlFile.foreach(printCounts)
	
	def printCounts(crawlFile: String) {
	
		val protocol = "s3n://"
		val crawlHeader = "aws-publicdatasets/"
		val searchFile = protocol + crawlHeader + crawlFile
		val crawlFileIDpattern = """^.*/wet/([\D\d]+)-ip.*$""".r
		val crawlFileIDpattern(crawlFileID) = crawlFile

		val fullCrawlName = searchFile
		println("%s".format(fullCrawlName))

		val crawlData = sc.textFile(fullCrawlName)
		crawlData.persist(StorageLevel.DISK_ONLY)
		crawlData.saveAsTextFile("All" + crawlFileID)

		val categorizeEachLine = crawlData.flatMap(line => ( {
			
			if ( line.contains("WARC-Target-URI: ") == true ) "header" else "line" }, line, { if ( line.contains("Donald Trump") == true) 1 else 0 }))
//		categorizeEachLine.saveAsTextFile("Categorized" + crawlFileID)

//		val pageurllines = crawlData.filter(x=> x.contains("WARC-Target-URI:"))
//		val pageurls = pageurllines.flatMap(line => ( {
//			val webpageNamePattern = """^.*WARC-Target-URI: ([\D\d]+)""".r;
//			val webpageNamePattern(pageurl) = line;
//			pageurl
//			}))
		val trumpCount = crawlData.filter(x=> x.contains("Donald Trump")).count()
		println("No. of webpages searched: %d\nNo. of lines Donald Trump is mentioned: %d".format(pageurls.count(), trumpCount))

//		val webpageCrawlData = crawlData.map(file=>file.split("WARC/1.0"))
		
//		webpageCrawlData.persist()
//		webpageCrawlData.foreach(trumpCount)
	
	}

	def trumpCount(webpageCrawlData: Array[String]) { 
		val pc = prezCount(webpageCrawlData, "Donald Trump")  
	}

	def prezCount(webpageCrawlData: Array[String], candidate: String) {

		val prezPattern = """^.*(Donald Trump).*$""".r
//		val prezPattern(prezFilter) = webpageCrawlData
 
		val prezFilter = webpageCrawlData.filter(name => name.contains(candidate))
		val prezCtr = prezFilter.length
//		Trump.saveAsTextFile("Trump" + crawlFileID)
//		val TrumpCount = Trump.count()
//		if ( prezFilter == candidate ) {
		if ( prezCtr > 0)  {
			println("%s".format(webpageCrawlData(0)))
			val webpageNameLine = webpageCrawlData.filter(name => name.contains("WARC-Target-URI"))
			val webpageNamePattern = """^.*WARC-Target-URI: ([\D\d]+) WARC.*$""".r
			val webpageNamePattern(webpageName) = webpageNameLine(0)

			val webpageText = webpageCrawlData.filter(line=> !line.contains("WARC-"))
//
//	 			val warcHeaders = "^WARC-.*$".r
//				val warcHeaders(webpageData) = line

			val webpageDataKeys = webpageText.flatMap(words=>words.split(" "))
			val wordCountPairs = webpageDataKeys.groupBy(n=>n).map(t=>(t._1,t._2.length))
//			val wordCountPairs = webpageDataKeys.map(x=> (x,1))
//			val reducedWordCountPairs = wordCountPairs.reduce((x,y)=>x+y)
		}
		
	}
//		val Clinton = crawlData.filter(name => name.contains("Hillary Clinton"))
//		Clinton.saveAsTextFile("Clinton" +  crawlFileID)
//		val ClintonCount = Clinton.count()

//		println("Trump count: " + TrumpCount + "Clinton count: " + ClintonCount)

 }
}
