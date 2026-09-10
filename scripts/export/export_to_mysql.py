"""
ADS 层 Iceberg 表 → MySQL 同步脚本
用于 Power BI 连接 MySQL 做可视化
"""
from pyspark.sql import SparkSession
import os
# ==================== 配置区 ====================
ICEBERG_TABLE = "hdfs.default.dws_sale_gmv"   # 你的 ADS 表名
MYSQL_URL = "jdbc:mysql://localhost:3306/steam_dashboard?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf8"
MYSQL_USER = "root"
MYSQL_PASSWORD =  os.getenv('mysql_password')
MYSQL_TABLE = "ads_steam_dashboard"
WRITE_MODE = "overwrite"                             # overwrite 全量覆盖 / append 增量追加
# ================================================


def create_spark():
    spark = SparkSession.builder \
        .appName("ADS_To_MySQL") \
        .master("local[*]") \
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0,"
                "mysql:mysql-connector-java:8.0.33") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.hdfs",
                "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hdfs.type", "hadoop") \
        .config("spark.sql.catalog.hdfs.warehouse",
                "hdfs://localhost:9000/iceberg_warehouse") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def sync_to_mysql(spark):
    print(f"📖 正在读取 Iceberg 表: {ICEBERG_TABLE}")
    df = spark.sql(f"SELECT * FROM {ICEBERG_TABLE}")
    row_count = df.count()
    print(f"✅ 读取完成，共 {row_count} 行")
    df.printSchema()

    print(f"📤 正在写入 MySQL: {MYSQL_TABLE} (mode={WRITE_MODE})")
    df.write \
        .format("jdbc") \
        .option("url", MYSQL_URL) \
        .option("dbtable", MYSQL_TABLE) \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("batchsize", "5000") \
        .option("truncate", "true") \
        .mode(WRITE_MODE) \
        .save()

    print(f"🎉 同步完成！{row_count} 行数据已写入 MySQL")


if __name__ == "__main__":
    spark = create_spark()
    try:
        sync_to_mysql(spark)
    finally:
        spark.stop()