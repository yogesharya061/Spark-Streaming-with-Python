from pyspark.sql import SparkSession
# from lib.logger import Log4j
import logging
import logging.config

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("WelcomeToPySparkSQLExample") \
        .getOrCreate()

    # logger = Log4j(spark)
    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger('WelcomeToPySparkSQLExample')

    src_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/country.csv")
    src_df.createOrReplaceTempView("SOURCE_TBL")
    logger.info("*** Finish reading source file ***")

    result = spark.sql("""select country, count(*) as count
    from SOURCE_TBL where Age<40 group by country""")
    logger.info("hi")

    result.show()
