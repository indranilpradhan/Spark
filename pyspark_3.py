from pyspark.sql import SparkSession,SQLContext
import os
import sys
import pyspark
from pyspark.sql import functions as fun

def main():
    inputfile = str(sys.argv[2])
    outputfile = str(sys.argv[3])
    numofcpu = str(sys.argv[1])
    os.environ["SPARK_WORKER_CORES"] = numofcpu
    sc = pyspark.SparkContext(appName="PySpark")
    sqlContext= SQLContext(sc)
    q3 = sqlContext.sparkSession.builder.appName('q1').getOrCreate()
    df = q3.read.csv(inputfile, inferSchema=True, header=True)
    df.createOrReplaceTempView("airports")
    temp_df = df.filter(fun.col("LATITUDE").between(10,90))
    q3_result = temp_df.filter(fun.col("LONGITUDE").between(-90,-10)).select("NAME")
    q3_result.toPandas().to_csv(outputfile, index= False)

if __name__ == "__main__":
    main()