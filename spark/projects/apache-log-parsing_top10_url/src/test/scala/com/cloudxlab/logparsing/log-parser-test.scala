package com.cloudxlab.logparsing

import org.scalatest.FlatSpec

class LogParserSpec extends FlatSpec {

  "isPatternMatched" should "Check if the pattern is matched" in {
    val utils = new Utils

    // Return true if the pattern is matched
    var line1 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
    assert(utils.isPatternMatched(line1))

    // Return false if the pattern isn't matched
    var line2 = "invalid line"
    assert(!utils.isPatternMatched(line2))
  }

  "extractURL" should "Extact the URL" in {
    val utils = new Utils
    var line = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""

    var result = utils.extractURL(line)
    assert(result == "/mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us")  
  }
}
