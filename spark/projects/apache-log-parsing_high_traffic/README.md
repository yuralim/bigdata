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

**Q3: Write spark code to find out top 5 time frame for high traffic (which day of the week or hour of the day receives peak traffic, this information will help company to manage resources for handling peak traffic load)**

**Q4: Write spark code to find out 5 time frames of least traffic (which day of the week or hour of the day receives least traffic, this information will help company to do production deployment in that time frame so that less number of users will be affected if some thing goes wrong during deployment)**