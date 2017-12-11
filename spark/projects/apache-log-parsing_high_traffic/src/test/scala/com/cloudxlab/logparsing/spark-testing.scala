package com.cloudxlab.logparsing

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}

class SampleTest extends FunSuite with SharedSparkContext {
    test("Get which day of week has high traffic") {
        var line1 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
        var line2 = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0\" 200 1839"
        var line3 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""

        val utils = new Utils

        val list = List(line1, line2, line3)
        val rdd = sc.parallelize(list);

        val results = utils.getTopN(rdd, sc, 2, "day", "desc")

        assert(results.length == 2)
        assert(results(0)._1 == "Mon")
        assert(results(1)._1 == "Tue")
    }

    test("Get which day of week has least traffic") {
        var line1 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
        var line2 = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0\" 200 1839"
        var line3 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""

        val utils = new Utils

        val list = List(line1, line2, line3)
        val rdd = sc.parallelize(list);

        val results = utils.getTopN(rdd, sc, 2, "day", "asc")

        assert(results.length == 2)
        assert(results(0)._1 == "Tue")
        assert(results(1)._1 == "Mon")
    }

    test("Get which hour of day has high traffic") {
        var line1 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
        var line2 = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0\" 200 1839"
        var line3 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""

        val utils = new Utils

        val list = List(line1, line2, line3)
        val rdd = sc.parallelize(list);

        val results = utils.getTopN(rdd, sc, 2, "hour", "desc")

        assert(results.length == 2)
        assert(results(0)._1 == "06")
        assert(results(1)._1 == "00")
    }

    test("Get which hour of day has least traffic") {
        var line1 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
        var line2 = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0\" 200 1839"
        var line3 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""

        val utils = new Utils

        val list = List(line1, line2, line3)
        val rdd = sc.parallelize(list);

        val results = utils.getTopN(rdd, sc, 2, "hour", "asc")

        assert(results.length == 2)
        assert(results(0)._1 == "00")
        assert(results(1)._1 == "06")
    }
}
