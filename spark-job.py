import sys
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-job <file> <output>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("ProductCountByCountry")\
        .getOrCreate()

    outfile = sys.argv[2]
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    def mapper(line):
        words = line.split(",")
        return (words[7],1)
    
    counts = lines.map(mapper).reduceByKey(add)
    output = counts.collect()

    schema = StructType([StructField('country', StringType(), True),StructField('product count', StringType(), True)])

    rdd = spark.sparkContext.parallelize(output)
    df = spark.createDataFrame(rdd,schema)
    df.write.format("com.databricks.spark.csv").option("header", "true").save(
        path = outfile , mode = "overwrite")
    
    for (grade, index) in output:
        print("%s: %s" % (grade, index))

    spark.stop()
