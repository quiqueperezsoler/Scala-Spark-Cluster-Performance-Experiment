import sys

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
#print (log_report[0])
infile.close()

#log_report now contains a list of arrays that look like this: ['in24.inetnebr.com', 1]
#the first value in array is the host
#the second value is the day of the month stored as int

#I think this is how we start, but I am not sure because I don't have an enviroment to test it in.
from pyspark import SparkContext
sc = SparkContext("local", "Map app")
rdd = sc.parallelize(log_report)