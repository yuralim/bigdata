package com.cloudxlab.logparsing

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.util.matching.Regex

class Utils extends Serializable {
    val HOUR_PATTERN = """^\S+ \S+ \S+ \[\d{2}\/\w{3}\/\d{4}:(\d{2}):(\d{2}):(\d{2})\s[+\-]\d{4}\] .*$""".r
    val DAY_PATTERN = """^\S+ \S+ \S+ \[(\d{2}\/\w{3}\/\d{4}):\d{2}:\d{2}:\d{2}\s[+\-]\d{4}\] .*$""".r
    val dateFormat = new java.text.SimpleDateFormat("dd/MMM/yyyy")
    val dayOfWeekFormat = new java.text.SimpleDateFormat("EE")

    def isPatternMatched(pattern: Regex, line: String): Boolean = return (pattern.findFirstMatchIn(line) != None)

    def extractHour(line: String): String = {
        val HOUR_PATTERN(hour:String, minute: String, second: String) = line
        return hour
    }

    def extractDay(line: String): String = {
        val DAY_PATTERN(timeStamp: String) = line
        return getDayOfWeek(timeStamp)
    }

    def getDayOfWeek(timeStamp: String): String = {
        var date = dateFormat.parse(timeStamp)
        return dayOfWeekFormat.format(date)
    }

    def getTopN(accessLogs: RDD[String], sc: SparkContext, topn: Int,
        timeframe: String, sortOrder: String):Array[(String,Int)] = {
        
        // Get timeframe from log
        var cleanLogs: RDD[String] = null
        if(timeframe == "hour") {
            var validLogs = accessLogs.filter(isPatternMatched(HOUR_PATTERN, _))
            cleanLogs = validLogs.map(extractHour(_))
        } else {
            var validLogs = accessLogs.filter(isPatternMatched(DAY_PATTERN, _))
            cleanLogs = validLogs.map(extractDay(_))
        }

        // Calculate the frequencies
        var timeframeTuples = cleanLogs.map((_, 1))
        var frequencies = timeframeTuples.reduceByKey(_ + _)

        // Get the top N results
        var ascending = (sortOrder == "asc")
        var sortedfrequencies = frequencies.sortBy(x => x._2, ascending)
        return sortedfrequencies.take(topn)
    }
}

object EntryPoint {
    val usage = """
        Usage: EntryPoint <how_many> <timeframe(hour|day)> <sort_order> <file_or_directory_in_hdfs>
        Eample: EntryPoint 10 hour desc /data/spark/project/access/access.log.45.gz
    """
    
    def main(args: Array[String]) {
        
        if (args.length != 5) {
            println("Expected:5 , Provided: " + args.length)
            println(usage)
            return;
        }

        var utils = new Utils

        // Create a local StreamingContext with batch interval of 10 second
        val conf = new SparkConf().setAppName("WordCount")
        val sc = new SparkContext(conf);
        sc.setLogLevel("WARN")

        var accessLogs = sc.textFile(args(4))
        var results = utils.getTopN(accessLogs, sc, args(1).toInt, args(2), args(3))
        println("===== TOP " + args(1) + " Results =====")
        for(i <- results){
            println(i)
        }
    }
}
