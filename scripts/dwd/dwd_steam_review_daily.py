#对评论内容进行初步清洗、并进行情感归类、打内容标签
from pyspark.sql import SparkSession
import pandas as pd
import re
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
#从ods层读表,日常只查近7天的
df=spark.sql("""
select*from hdfs.default.ods_steam_review
where datediff(CURDATE(),left(created_date,10))<=7
""")

#转为pandas_DataFrame
df=df.toPandas()

def clean_text(text):
    # 清洗，移除特殊字符（使用Python re模块支持的语法）
    text = re.sub(r'[\s\W_]+', ' ', text)
    # 清洗，移除多余空格
    text = re.sub(r'\s+', ' ', text).strip()
    return text
df['clean_content']=df['review'].apply(clean_text)
#去重,去除空值
df.drop_duplicates(inplace=True)
df.dropna(inplace=True)

#调用大模型api接口,用ai对清洗后的文本自动分类、自动判断情感倾向
from openai import OpenAI
import os
system_set="""
你是一个对游戏《生化危机》的评价文本进行评价分类的助手，你将接收到一条游戏评论，请你评论进行分类
分类范围：[画面，剧情，玩法，音效，优化，价格，情怀，其他]
示例输入：
十个小时丢档三次，OK，我真的只能差评，重新打三次幸好中途有一点退出的地方，不然我玩毛线
输出：
优化

示例输入：
里昂男神重返浣熊市，支持支持还是tm的支持
输出：
情怀

注意：分类范围优先考虑上述给出的几个范围关键词，严格按照示例格式输出每条评论最相关的分类关键词
"""
def sort(text):
    client=OpenAI(base_url='https://api.deepseek.com',api_key=os.getenv('DEEPSEEK_KEY'))
    responses=client.chat.completions.create(
        model='deepseek-chat',
        temperature=0.5,
        messages=[{'role':'system','content':system_set},
                  {'role':'user','content':text}
                  ])
    response=responses.choices[0].message.content
    return response

df['category']=df['clean_content'].apply(sort)

#删除dwd层进群天数据防止冗余重复
spark.sql("""
delete from hdfs.default.dwd_steam_review
where datediff(CURDATE(),left(created_date,10))<=7
""")
#追加写入dwd层
df=spark.createDataFrame(df)
df.writeTo('hdfs.default.dwd_steam_review').append()