# Crawling for names
Welcome to my Insight data engineering project

Written over the course of three weeks, this project has two goals. The first is to collect a count on the number of web pages that mention some of the presidential front-runners as of January/February 2016. The program uses data from the Common Crawl, which consists of more than 150 terabytes of data scrapped from 1.2 billion URIs during November 2015.

The task itself is a simple exercise in counting but the sheer size of the data and some of their characteristics -- multi-line record format spread across 37,500 files -- presented the biggest engineering challenge. Tools and language used were Scala, Spark, HBase and Python/Flask scripts.

In addition to this repository of code, there are important configuration files to update. Of particular note is the spark-defaults.conf file which is maintained at the root of this repository but should be stored on the master node at this directory: /usr/local/spark/conf

For an overview of this project, refer to the presentation: http://www.slideshare.net/HoaNguyen317/insight-data-engineering-project
