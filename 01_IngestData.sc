import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .config("spark.yarn.access.hadoopFileSystems",os.getenv['STORAGE'])\
  .appName("Spark SQL basic example")
  .getOrCreate()

