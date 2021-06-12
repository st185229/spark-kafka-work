from pyspark.sql import SparkSession

# TO-DO: create a variable with the absolute path to the text file
# /home/workspace/lesson-1-streaming-dataframes/exercises/starter/Test.txt
logFile = "/mnt/c/Users/sthoe09258/source/udacity/nd029-c2-apache-spark-and-spark-streaming-starter/lesson-1-streaming-dataframes/exercises/starter/Test.txt"

# TO-DO: create a Spark session

spark = SparkSession.builder.appName("HelloSpark").getOrCreate()

# TO-DO: set the log level to WARN

spark.sparkContext.setLogLevel('WARN')

# TO-DO: using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file

logData = spark.read.text(logFile).cache()

numAs = 0
numBs = 0


def count_char(row, char_to_be_found='a'):
    global numAs
    numAs += row.value.count(char_to_be_found)
    print("Total %i Count" % numAs)


def count_char1(row, char_to_be_found='b'):
    global numBs
    numBs += row.value.count(char_to_be_found)
    print("Total %i Count" % numAs)


logData.foreach(count_char)

logData.foreach(count_char1)



spark.stop()
