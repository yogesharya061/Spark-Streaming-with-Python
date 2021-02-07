from pyspark.sql import SparkSession

from lib.logger import Log4j

if __name__ == "__main__":
    # Creating Spark Session for the processing of data.
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("WelcomePySparkSQL") \
        .getOrCreate()

    # Instantiating logger for better logs and comments in console.
    logger = Log4j(spark)

    # Reading Sample file using pyspark
    DF = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/sample.csv")

    # Creating temporary table for utilizing SQL power.
    DF.createOrReplaceTempView("SAMPLE_DF")

    countDF = spark.sql("select Country, count(1) as count from SAMPLE_DF where Age<40 group by Country")
    logger.info("Data frame has been created as shown below:")

    countDF.show()
