# Problem Statement

Churning the logs of NASA Kennedy Space Center WWW server.

Dataset is located at /data/spark/project/NASA_access_log_Aug95.gz in [CloudxLab](https://cloudxlab.com) HDFS.

Above dataset is access log of NASA Kennedy Space Center WWW server in Florida. The logs are an ASCII file with one line per request, with the following columns:

1. **host** - making the request. A hostname when possible, otherwise the Internet address if the name could not be looked up.

2. **timestamp** - in the format "DAY MON DD HH:MM:SS YYYY", where DAY is the day of the week, MON is the name of the month, DD is the day of the month, HH:MM:SS is the time of day using a 24-hour clock, and YYYY is the year. The timezone is -0400.

3. **request url** - Request URL.

4. **HTTP reply code**

5. **bytes returned by the server**

Note that from 01/Aug/1995:14:52:01 until 03/Aug/1995:04:36:13 there are no accesses recorded, as the Web server was shut down, due to Hurricane Erin.

Based on the above data, please answer following questions:

**Q5: Write spark code to find out unique HTTP codes returned by the server along with count (this information is helpful for devops team to find out how many requests are failing so that appropriate action can be taken to fix the issue)**

**Sample output**

HTTP code - Count
200 - 15400
404 - 324