import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.hadoop.fs.s3a.s3guard.ddb.region","us-east-1").config("spark.yarn.access.hadoopFileSystems","s3a://demo-aws-1/").getOrCreate()

val campaignDF = spark.read.option("header","true").csv("data/campaign_conversion.csv")
