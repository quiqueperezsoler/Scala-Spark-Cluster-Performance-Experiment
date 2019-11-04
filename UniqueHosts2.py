from dataclasses import dataclass, replace
from datetime import date

#import scala.util.matching
#import org.apache.spark.rdd.RDD
import re
import time
import sys

#t1 = System.nanoTime
#t1 = datetime.now()
start_time = time.time()

def apache_output(line):
    split_line = line.split()
    return {'remote_host': split_line[0],
            'apache_status': split_line[8],
            'date_time': split_line[3],
    }


def final_report(logfile):
    line_dict = []
    for line in logfile:
        line_parsed = apache_output(line)
        if (line_parsed["apache_status"] == "200"):
            line_dict.append([line_parsed["remote_host"],int(line_parsed["date_time"].split('/')[0].split('[')[1])])
    return line_dict

infile = open("apache.log", 'r')
log_report = final_report(infile)
print (log_report[0]) #PRINTS: ['izksdsk-com', 1]
infile.close()

#log_report now contains a list of arrays that look like this: ['in24.inetnebr.com', 1]
#the first value in array is the host
#the second value is the day of the month stored as int

#I think this is how we start, but I am not sure because I don't have an enviroment to test it in.
from pyspark import SparkContext
sc = SparkContext("local", "Map app")
rdd = sc.parallelize(log_report)

#create RDDs from log files
parsedLogs = log_report
accessLogs = log_report
failedLogs = log_report
#IN SCALA: val (parsedLogs, accessLogs, failedLogs) = parseLogs()

print("Past MAKING DAY-HOST TUPLES WITH dayToHostPairTuple = accessLogs.map(lambda x: (x.datetime.day, x.host)).distinct")

# group by day
dayGroupedHosts = rdd.map(lambda x: (x[1], x[0])).groupByKey()

# make pairs of (day, number of host in that day)
dayHostCount = dayGroupedHosts.map(lambda x: (x._1, x._2.size))

# sort by day
#dailyHosts = dayHostCount.sortBy(_._1).cache()
dailyHosts = dayHostCount.sortByKey().cache()

# make pairs of (day, host)????????? NOT NECESSARY ???????????????????
#dayAndHostTuple = accessLogs.map(lambda x: (x.datetime.day, x.host))
dayAndHostTuple = rdd.map(lambda x: (x.datetime.day, x.host))

# group by day
groupedByDay = dayAndHostTuple.groupByKey()

# sort by day
sortedByDay = groupedByDay.sortByKey()

# calculate the average request per day (python)
#totalDailyReqPerHost = sortedByDay.map(lambda x: (x._1, x._2.size))

avgDailyReqPerHost = sortedByDay.join(dailyHosts).mapValues(lambda x: x._1/x._2).sortByKey().cache()
print("Are we there yet-----------------------------------------------------------------------------")

# return the records as a list (python)
avgDailyReqPerHostList = avgDailyReqPerHost[:30]

print("Average number of daily requests per Hosts is: ")

for val in avgDailyReqPerHostList:
        print(val)

elapsed_time = time.time() - start_time

#duration = (datetime.now() - t1) / 1e9d
#duration = duration.total_seconds() / 60.0
#print("Execution time: ",duration)
print("Execution time: ",elapsed_time)