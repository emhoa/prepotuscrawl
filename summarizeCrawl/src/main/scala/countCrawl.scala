import scala.collection.JavaConverters._

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName, HColumnDescriptor}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

import org.apache.hadoop.hbase.KeyValue.Type
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.HTable

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan

import org.apache.spark._

object summarizeCrawl {

 def main(args: Array[String]) {

        val newTableName = "new_candidate_crawl"
        val sparkConf = new SparkConf().setAppName("summarizeCrawl")
        val sc = new SparkContext(sparkConf)

        val conf = HBaseConfiguration.create()

        conf.set(TableInputFormat.INPUT_TABLE, "new_crawl")
        conf.set("hbase.rpc.timeout", "1800000")

        val scan = new Scan()

        scan.addColumn(Bytes.toBytes("cc"), Bytes.toBytes("candidate"))
        val hTable = new HTable(conf, "new_crawl")

        val scanner = hTable.getScanner(scan)

        // Reading values from scan result
        var result=scanner.next()
        
        //Quick and dirty counting of URI by candidate. Longer term solution should look at more elegant way of saving results
        var trump=0
        var clinton=0
        var sanders=0
        var cruz=0
        var dh=0
        var db=0
        var dt=0
        var hb=0
        var ht=0
        var bt=0
        var dhb=0
        var dht=0
        var dbt=0
        var hbt=0
        var dhbt=0
        while (result != null) {
                var tmptrump=0
                var tmpclinton=0
                var tmpsanders=0
                var tmpcruz=0
                val candidates = new String(result.value)
                for (c <- candidates) {
                        if (c == 'D') { tmptrump=1; trump=trump+1 }
                        if (c == 'H') { tmpclinton=1; clinton=clinton+1 }
                        if (c == 'B') { tmpsanders=1; sanders=sanders+1 }
                         if (c == 'T') { tmpcruz=1; cruz=cruz+1 }
                }
                if (tmptrump==1 & tmpclinton==1) dh=dh+1
                if (tmptrump==1 & tmpsanders==1) db=db+1
                if (tmptrump==1 & tmpcruz==1) dt=dt+1
                if (tmpclinton==1 & tmpsanders==1) hb=hb+1
                if (tmpclinton==1 & tmpcruz==1) ht=ht+1
                if (tmpsanders==1 & tmpcruz==1) bt=bt+1
                if (tmptrump==1 & tmpclinton==1 & tmpsanders==1) dhb = dhb+1
                if (tmptrump==1 & tmpclinton==1 & tmpcruz==1) dht = dht+1
                if (tmptrump==1 & tmpsanders==1 & tmpcruz==1) dbt = dbt+1
                if (tmpclinton==1 & tmpsanders==1 & tmpcruz==1) hbt = hbt+1
                if (tmptrump==1 & tmpclinton==1 & tmpsanders==1 & tmpcruz==1) dhbt = dhbt+1
                result = scanner.next()

        }
        //Print to stdout for now but long-term should direct to file
        println("Name,value\ntrump,%d\nclinton,%d\ncruz,%d\nsanders,%d\nTrump-Clinton,%d\nTrump-Cruz,%d\nTrump-Sanders,%d\nClinton-Cruz,%d\nClinton-Sanders,%d\nCruz-Sanders,%d\nTrump-Clinton-Cruz,%d\nTrump-Clinton-Sanders,%d\nTrump-Cruz-Sanders,%d\nClinton-Cruz-Sanders,%d\nTrump-Clinton-Cruz-Sanders,%d\n".format(trump, clinton, cruz, sanders, dh, dt, db, ht, hb, bt, dht, dhb, dbt, hbt, dhbt))
    }
  }
