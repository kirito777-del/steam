#GMV指标由日度销量*日增评论数得出（单个用户评价次数唯一）
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

#先建表:仅首次运行建表
spark.sql("""
create table hdfs.default.dws_sale_gmv (
    appid varchar comment '游戏id',
    dt varchar comment '日期',
    gmv decimal(18,4) comment '产品日度销售额GMV'
)
""")


dws_sale_gmv=spark.sql(
    """
    insert overwrite table
        hdfs.default.dws_sale_gmv
        
    with t as (
    select a.appid,b.price_final_cny,date(created_date) dt
    from hdfs.default.dwd_steam_review a
    join hdfs.default.steamgame b on a.appid=b.appid
    where a.steam_purchase=TRUE)
    
    select appid,dt,sum(price_final_cny),count(*) add_review_num gmv from t
    group by appid,dt
    """
)

spark.stop()