package com.cloudxlab.logparsing

import org.scalatest.FlatSpec

class LogParserSpec extends FlatSpec {

  "isPatternMatched" should "Check if the hour pattern is matched" in {
    val utils = new Utils
    val HOUR_PATTERN = """^\S+ \S+ \S+ \[\d{2}\/\w{3}\/\d{4}:(\d{2}):(\d{2}):(\d{2})\s[+\-]\d{4}\] .*$""".r

    // Return true if the pattern is matched
    var line1 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
    assert(utils.isPatternMatched(HOUR_PATTERN, line1))

    // Return false if the pattern isn't matched
    var line2 = "invalid line"
    assert(!utils.isPatternMatched(HOUR_PATTERN, line2))
  }

  "isPatternMatched" should "Check if the day pattern is matched" in {
    val utils = new Utils
    val DAY_PATTERN = """^\S+ \S+ \S+ \[(\d{2})\/(\w{3})\/(\d{4}):\d{2}:\d{2}:\d{2}\s[+\-]\d{4}\] .*$""".r

    // Return true if the pattern is matched
    var line1 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
    assert(utils.isPatternMatched(DAY_PATTERN, line1))

    // Return false if the pattern isn't matched
    var line2 = "invalid line"
    assert(!utils.isPatternMatched(DAY_PATTERN, line2))
  }

  "extractHour" should "Extract the hour in the day" in {
    val utils = new Utils

    var line = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
    assert(utils.extractHour(line) == "06")
  }

  "getDayOfWeek" should "Return the day of week" in {
    val utils = new Utils
    assert(utils.getDayOfWeek("03/Aug/2015") == "Mon")
  }

  "extractDay" should "Extrac the day of of week" in {
    val utils = new Utils

    var line = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
    assert(utils.extractDay(line) == "Mon")
  }
}
