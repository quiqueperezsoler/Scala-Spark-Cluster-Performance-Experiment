import scala.util.matching
import org.apache.spark.rdd.RDD

val t1 = System.nanoTime

case class Cal(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int)                

case class Row(host: String, clientID: String, userID: String, dateTime: Cal, method: String, endpoint: String,
			   protocol: String, responseCode: Int, contentSize: Long)                

val month_map = Map("Jan" -> 1, "Feb" -> 2, "Mar" -> 3, "Apr" -> 4, "May" -> 5, "Jun" -> 6, "Jul" -> 7, "Aug" -> 8,
					"Sep" -> 9, "Oct" -> 10, "Nov" -> 11, "Dec" -> 12)

//------------------------------------------------
// A regular expression pattern to extract fields from the log line
val APACHE_ACCESS_LOG_PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)""".r

//------------------------------------------------
def parseApacheTime(s: String): Cal = {
	return Cal(s.substring(7, 11).toInt, month_map(s.substring(3, 6)), s.substring(0, 2).toInt, 
			   s.substring(12, 14).toInt, s.substring(15, 17).toInt, s.substring(18, 20).toInt)
}

//------------------------------------------------
def parseApacheLogLine(logline: String): (Either[Row, String], Int) = {
	val ret = APACHE_ACCESS_LOG_PATTERN.findAllIn(logline).matchData.toList
	if (ret.isEmpty)
		return (Right(logline), 0)

	val r = ret(0)
	val sizeField = r.group(9)

	var size: Long = 0
	if (sizeField != "-")
		size = sizeField.toLong

	return (Left(Row(r.group(1), r.group(2), r.group(3), parseApacheTime(r.group(4)), r.group(5), r.group(6),
					 r.group(7), r.group(8).toInt, size)), 1)
}

//------------------------------------------------
def parseLogs(): (RDD[(Either[Row, String], Int)], RDD[Row], RDD[String]) = {
	val fileName = "hdfs://cluster-fc32-m:8020/user/hara_p_kumar/apache.log" 
	val parsedLogs = sc.textFile(fileName).map(parseApacheLogLine).cache()
	val accessLogs = parsedLogs.filter(x => x._2 == 1).map(x => x._1.left.get)
	val failedLogs = parsedLogs.filter(x => x._2 == 0).map(x => x._1.right.get)

	val failedLogsCount = failedLogs.count()
	
	if (failedLogsCount > 0) {
		println(s"Number of invalid logline: $failedLogs.count()")
		failedLogs.take(20).foreach(println)
	}
	
	println(s"Read $parsedLogs.count() lines, successfully parsed $accessLogs.count() lines, and failed to parse $failedLogs.count()")
	
	return (parsedLogs, accessLogs, failedLogs)
}
	
// create RDDs from log files
val (parsedLogs, accessLogs, failedLogs) = parseLogs()

// make pairs of (day, host)
val dayToHostPairTuple = accessLogs.map(x => (x.dateTime.day, x.host)).distinct

// group by day
val dayGroupedHosts = dayToHostPairTuple.groupBy(_._1)

// make pairs of (day, number of host in that day)
val dayHostCount = dayGroupedHosts.map(x => (x._1, x._2.size))

// sort by day
val dailyHosts = dayHostCount.sortBy(_._1).cache()

// make pairs of (day, host)
val dayAndHostTuple = accessLogs.map(x => (x.dateTime.day, x.host))

// group by day
val groupedByDay = dayAndHostTuple.groupBy(_._1)

// sort by day
val sortedByDay = groupedByDay.sortBy(_._1)

// calculate the average request per day
val totalDailyReqPerHost = sortedByDay.map(x => (x._1, x._2.size))
val avgDailyReqPerHost = totalDailyReqPerHost.join(dailyHosts).mapValues(x => x._1/x._2).sortBy(_._1).cache()

// return the records as a list
val avgDailyReqPerHostList = avgDailyReqPerHost.take(30)

println("Average number of daily requests per Hosts is: ")
avgDailyReqPerHostList.foreach(println)

val duration = (System.nanoTime - t1) / 1e9d

println("Execution time: " + duration)
