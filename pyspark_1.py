from pyspark.sql import SparkSession,SQLContext
import os
import sys
import pyspark

def main():
    inputfile = str(sys.argv[2])
    outputfile = str(sys.argv[3])
    numofcpu = str(sys.argv[1])
    os.environ["SPARK_WORKER_CORES"] = numofcpu
    sc = pyspark.SparkContext(appName="PySpark")
    sqlContext= SQLContext(sc)
    q1 = sqlContext.sparkSession.builder.appName('q1').getOrCreate()
    df = q1.read.csv(inputfile, inferSchema=True, header=True)
    df.createOrReplaceTempView("airports")
    q1_result = df.groupBy("country").count()
    q1_result.toPandas().to_csv(outputfile, index= False)
if __name__ == "__main__":
    main()