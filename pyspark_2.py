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
    q2 = sqlContext.sparkSession.builder.appName('q2').getOrCreate()
    df = q2.read.csv(inputfile, inferSchema=True, header=True)
    df.createOrReplaceTempView("airports")
    temp_result = df.groupBy("country").count()
    q2_result = temp_result.orderBy(temp_result["count"].desc()).select("country").limit(1)
    q2_result.toPandas().to_csv(outputfile, index= False)

if __name__ == "__main__":
    main()