"""
爬取游戏真实评论（包含所有字段）- 首次执行回溯60天、后续增量刷新近7天
"""
import pandas as pd
import requests
import time
import sys
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
#待入库的游戏id列表
appid_li=[413150, 1281930, 1245620, 1086940, 1091500, 2358720, 2807960, 1551360, 3240220, 1174180]

#预先定义表结构schema
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, FloatType, LongType, DoubleType
)

# 定义完整的Schema
reviews_schema = StructType([
    # 带上appid标签、注明是哪个游戏
    StructField("appid",StringType(),True),
    # 核心信息
    StructField("recommendationid", StringType(), True),
    StructField("review", StringType(), True),
    StructField("language", StringType(), True),
    StructField("timestamp_created", LongType(), True),
    StructField("timestamp_updated", LongType(), True),
    StructField("voted_up", BooleanType(), True),

    # 互动数据
    StructField("votes_up", IntegerType(), True),
    StructField("votes_funny", IntegerType(), True),
    StructField("weighted_vote_score", DoubleType(), True),  # 浮点数
    StructField("comment_count", IntegerType(), True),

    # 作者信息
    StructField("author_steamid", StringType(), True),
    StructField("author_num_games_owned", IntegerType(), True),
    StructField("author_num_reviews", IntegerType(), True),
    StructField("author_playtime_forever", IntegerType(), True),
    StructField("author_playtime_last_two_weeks", IntegerType(), True),
    StructField("author_playtime_at_review", IntegerType(), True),
    StructField("author_deck_playtime_at_review", DoubleType(), True),  # 有小数
    StructField("author_last_played", LongType(), True),

    # 元数据与标志位
    StructField("steam_purchase", BooleanType(), True),
    StructField("received_for_free", BooleanType(), True),
    StructField("written_during_early_access", BooleanType(), True),
    StructField("primarily_steam_deck", BooleanType(), True),
    StructField("developer_response", StringType(), True),
    StructField("timestamp_dev_responded", LongType(), True),

    # 可读时间格式（新增字段）
    StructField("created_date", StringType(), True),
    StructField("updated_date", StringType(), True),
])

#主循环
for i in appid_li:
    # ---------------------- 配置区 ----------------------
    APP_ID = i                # 游戏appid
    LANGUAGE = "schinese"           # 简体中文评论
    PER_PAGE = 20                   # 每页条数（最大100）
    TARGET_COUNT = 100              # 想爬的评论总条数
    DAYS_RANGE = 60                  # 爬取最近多少天的评论（首次回溯60天、后续增量刷新近7天）
    MAX_RETRIES = 5                 # 超时重试次数
    TIMEOUT = 30                    # 单次请求超时时间
    PROXIES = {
        # "http": "http://127.0.0.1:7890",
        # "https": "http://127.0.0.1:7890"
    }
    #目标表
    target_table='hdfs.default.ods_steam_review'
    # ---------------------------------------------------------------

    # 完整的请求头
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Referer": f"https://store.steampowered.com/app/{APP_ID}/",
    }

    # 带重试的请求会话
    session = requests.Session()
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504, 403]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)

    all_reviews = []
    cursor = "*"
    page = 1
    query_summary = None
    failed_count = 0
    max_failures = 10

    # 计算截止时间（用于过滤）
    cutoff_date = None
    if DAYS_RANGE > 0:
        cutoff_date = datetime.now() - timedelta(days=DAYS_RANGE)
        print(f"⏰ 将爬取最近 {DAYS_RANGE} 天的评论（自 {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')} 起）")
    else:
        print("⏰ 不限制时间范围，爬取全部评论")

    print(f"🎮 开始爬取游戏 {APP_ID} 的评论...")
    print(f"🎯 目标: {TARGET_COUNT} 条评论")

    while len(all_reviews) < TARGET_COUNT:
        print(f"\n📄 正在爬取第 {page} 页，当前已爬 {len(all_reviews)}/{TARGET_COUNT} 条")

        # 构建参数
        params = {
            "json": 1,
            "filter": "all",  # 必须为"all"才能使用day_range
            "language": LANGUAGE,
            "cursor": cursor,
            "num_per_page": PER_PAGE,
            "purchase_type": "all",
        }

        # 如果设置了天数范围，添加day_range参数
        if DAYS_RANGE > 0:
            params["day_range"] = DAYS_RANGE
            print(f"  📅 时间筛选: 最近 {DAYS_RANGE} 天")

        # 尝试两种URL格式
        urls_to_try = [
            f"https://store.steampowered.com/appreviews/{APP_ID}",
            f"https://steamcommunity.com/appreviews/{APP_ID}"
        ]

        data = None
        for url in urls_to_try:
            try:
                print(f"  🔗 尝试连接: {url}")
                resp = session.get(
                    url,
                    params=params,
                    proxies=PROXIES if PROXIES else None,
                    timeout=TIMEOUT
                )
                resp.raise_for_status()
                data = resp.json()
                print(f"  ✅ 连接成功！")
                break
            except Exception as e:
                print(f"  ❌ 连接失败: {e}")
                continue

        if data is None:
            failed_count += 1
            if failed_count >= max_failures:
                print(f"❌ 连续失败 {max_failures} 次，程序退出")
                sys.exit(1)
            print(f"⏳ 等待 {failed_count * 5} 秒后重试...")
            time.sleep(failed_count * 5)
            continue
        else:
            failed_count = 0

        # 第一页打印统计信息
        if page == 1 and data.get("success") == 1:
            query_summary = data.get("query_summary", {})
            print(f"\n📊 游戏评价概况:")
            print(f"  评分: {query_summary.get('review_score_desc', 'N/A')}")
            print(f"  好评: {query_summary.get('total_positive', 0)}")
            print(f"  差评: {query_summary.get('total_negative', 0)}")
            print(f"  总数: {query_summary.get('total_reviews', 0)}\n")

        reviews_data = data.get("reviews", [])
        new_cursor = data.get("cursor", "*")

        if not reviews_data:
            print("⚠️ 没有更多评论了，爬取提前结束")
            break

        # 处理当前页评论
        page_count = 0
        for rev in reviews_data:
            if len(all_reviews) >= TARGET_COUNT:
                break

            # 如果设置了天数范围，额外检查时间（双重保障）
            if DAYS_RANGE > 0 and cutoff_date:
                timestamp = rev.get("timestamp_created")
                if timestamp:
                    review_date = datetime.fromtimestamp(timestamp)
                    # 如果评论时间早于截止日期，跳过
                    if review_date < cutoff_date:
                        continue

            author = rev.get("author", {})

            processed = {
                # 核心信息
                "recommendationid": rev.get("recommendationid"),
                "review": rev.get("review", ""),
                "language": rev.get("language"),
                "timestamp_created": rev.get("timestamp_created"),
                "timestamp_updated": rev.get("timestamp_updated"),
                "voted_up": rev.get("voted_up", False),
                # 互动数据
                "votes_up": rev.get("votes_up", 0),
                "votes_funny": rev.get("votes_funny", 0),
                "weighted_vote_score": rev.get("weighted_vote_score"),
                "comment_count": rev.get("comment_count", 0),
                # 作者信息
                "author_steamid": author.get("steamid"),
                "author_num_games_owned": author.get("num_games_owned"),
                "author_num_reviews": author.get("num_reviews"),
                "author_playtime_forever": author.get("playtime_forever"),
                "author_playtime_last_two_weeks": author.get("playtime_last_two_weeks"),
                "author_playtime_at_review": author.get("playtime_at_review"),
                "author_deck_playtime_at_review": author.get("deck_playtime_at_review"),
                "author_last_played": author.get("last_played"),
                # 元数据与标志位
                "steam_purchase": rev.get("steam_purchase", False),
                "received_for_free": rev.get("received_for_free", False),
                "written_during_early_access": rev.get("written_during_early_access", False),
                "primarily_steam_deck": rev.get("primarily_steam_deck", False),
                "developer_response": rev.get("developer_response"),
                "timestamp_dev_responded": rev.get("timestamp_dev_responded"),
            }
            all_reviews.append(processed)
            page_count += 1

        print(f"✅ 第 {page} 页完成，本页获取 {page_count} 条，当前共 {len(all_reviews)}/{TARGET_COUNT} 条")

        # 检查是否还有下一页
        if new_cursor == cursor:
            print("🔄 游标未更新，已无下一页，爬取结束")
            break

        cursor = new_cursor
        page += 1

        # 随机延迟，避免被识别为爬虫
        delay = 2 + (page % 3)
        print(f"⏳ 等待 {delay} 秒后继续...")
        time.sleep(delay)

    # 保存为DataFrame
    df = pd.DataFrame(all_reviews)

    if not df.empty:
        # 添加可读时间格式
        df['created_date'] = df['timestamp_created'].apply(
            lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') if x else None
        )
        df['updated_date'] = df['timestamp_updated'].apply(
            lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') if x else None
        )
        #打伤appid标签
        df['appid']=APP_ID

        # 统计信息
        print(f"\n{'='*60}")
        print(f"🎉 爬取完成！")
        print(f"📊 统计信息:")
        print(f"  总评论数: {len(all_reviews)}")
        print(f"  好评数: {df[df['voted_up'] == True].shape[0]}")
        print(f"  差评数: {df[df['voted_up'] == False].shape[0]}")

        if not df.empty and DAYS_RANGE > 0:
            # 显示时间范围
            min_date = df['created_date'].min()
            max_date = df['created_date'].max()
            print(f"  时间范围: {min_date} 至 {max_date}")

        print(f"📋 总字段数: {len(df.columns)}")
        print(f"{'='*60}")

        # 写入ods贴源层
        df=spark.createDataFrame(df,schema=reviews_schema)


        df.writeTo(target_table).createOrReplace()

        # 显示几条示例
        print(f"\n📝 评论示例（前3条）:")
        print(df[['review', 'voted_up', 'votes_up', 'created_date']].head(3).to_string(index=False, max_colwidth=50))
    else:
        print("❌ 没有爬取到任何评论")

    print(f"\n✅ {APP_ID}daily数据爬取入库成功")

#日度任务完成
spark.stop()