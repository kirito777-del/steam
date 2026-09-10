#拼接日度全景宽表
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("IcebergHDFSDemo") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.hdfs",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hdfs.type", "hadoop") \
    .config("spark.sql.catalog.hdfs.warehouse", "hdfs://localhost:9000/iceberg_warehouse") \
    .getOrCreate()
df=spark.sql(
    """
    select a.*,b.price_final_cny
    from
    hdfs.default.dwd_steam_review a join hdfs.default.steamgame b
    on a.appid=b.appid
    where steam_purchase=True
    """
)
df.writeTo("hdfs.default.dws_overview_1d").createOrReplace()
spark.stop()