package com.cloudxlab.logparsing

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class Utils extends Serializable {
    val PATTERN = """^\S+ \S+ \S+ \[[\w:\/]+\s[+\-]\d{4}\] "\S+ \S+.*" (\d{3}) .+$""".r

    def isPatternMatched(line: String): Boolean = return (PATTERN.findFirstMatchIn(line) != None)

    def extractCode(line: String): String = {
        val PATTERN(code: String) = line
        return code
    }

    def getTopN(accessLogs:RDD[String], sc:SparkContext, topn:Int):Array[(String,Int)] = {
        // Filter invalid lines and extract only Code
        var validLogs = accessLogs.filter(isPatternMatched)
        var cleanURLs = validLogs.map(extractCode(_))

        // Calculate the frequencies
        var urlTuples = cleanURLs.map((_, 1))
        var frequencies = urlTuples.reduceByKey(_ + _)

        // Get the top N results
        var sortedfrequencies = frequencies.sortBy(x => x._2, false)
        return sortedfrequencies.take(topn)
    }
}

object EntryPoint {
    val usage = """
        Usage: EntryPoint <how_many> <file_or_directory_in_hdfs>
        Eample: EntryPoint 10 /data/spark/project/access/access.log.45.gz
    """
    
    def main(args: Array[String]) {
        
        if (args.length != 3) {
            println("Expected:3 , Provided: " + args.length)
            println(usage)
            return;
        }

        var utils = new Utils

        // Create a local StreamingContext with batch interval of 10 second
        val conf = new SparkConf().setAppName("WordCount")
        val sc = new SparkContext(conf);
        sc.setLogLevel("WARN")

        var accessLogs = sc.textFile(args(2))
        var results = utils.getTopN(accessLogs, sc, args(1).toInt)
        println("===== TOP " + args(1) + " Results =====")
        for(i <- results){
            println(i)
        }
    }
}
