"""批量获取steam游戏详情、创建游戏信息维度表"""
import requests
import time
from pyspark.sql import SparkSession
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------- 配置区（和评论脚本格式对齐） ----------------------
APPID_LIST = [413150, 1281930, 1245620, 1086940, 1091500, 2358720, 2807960, 1551360, 3240220, 1174180]

CC = "cn"
LANGUAGE = "schinese"
MAX_RETRIES = 5
TIMEOUT = 15
SLEEP_SEC = 1.0
OUT_CSV = "steam_games_info.csv"

PROXIES = {
    # "http": "http://127.0.0.1:7890",
    # "https": "http://127.0.0.1:7890"
}

#目标表
target_table='hdfs.default.steamgame'
# ------------------------------------------------------------------------

#会话重试策略
session = requests.Session()
retry_strategy = Retry(
    total=MAX_RETRIES,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
}


def get_single_game(appid):
    """获取单个游戏信息"""
    url = "https://store.steampowered.com/api/appdetails"
    params = {
        "appids": appid,
        "cc": CC,
        "l": LANGUAGE,
        "filters": "basic,price_overview,release_date,developers,publishers,genres,platforms"
    }

    print(f"  [调试] 开始请求 appid={appid}")
    print(f"  [调试] URL: {url}")
    print(f"  [调试] 参数: {params}")

    try:
        resp = session.get(
            url,
            params=params,
            headers=HEADERS,
            proxies=PROXIES if PROXIES else None,
            timeout=TIMEOUT
        )

        print(f"  [调试] 响应状态码: {resp.status_code}")

        if resp.status_code != 200:
            print(f"  [调试] 状态码异常: {resp.status_code}")
            return None

        resp.raise_for_status()

        print(f"  [调试] 开始解析JSON...")
        root = resp.json()
        print(f"  [调试] JSON解析成功")

        item = root.get(str(appid))
        if not item or not item.get("success"):
            print(f"  [调试] appid={appid} 接口success返回false，跳过")
            return None

        d = item["data"]
        price = d.get("price_overview", {})
        release = d.get("release_date", {})

        row = {
            "appid": appid,
            "name": d.get("name"),
            "developers": "、".join(d.get("developers", [])),
            "publishers": "、".join(d.get("publishers", [])),
            "release_date": release.get("date"),
            "coming_soon": release.get("coming_soon"),
            "price_initial_cny": price.get("initial"),
            "price_final_cny": price.get("final"),
            "discount_percent": price.get("discount_percent"),
            "genres": "、".join(g.get("description", "") for g in d.get("genres", [])),
            "platforms": "、".join(p for p, ok in d.get("platforms", {}).items() if ok),
            "short_description": d.get("short_description", "")
        }

        print(f"  [调试] ✅ 成功获取游戏: {row.get('name')}")
        return row

    except requests.exceptions.Timeout:
        print(f"  [调试] ❌ appid={appid} 请求超时")
        return None
    except requests.exceptions.ConnectionError:
        print(f"  [调试] ❌ appid={appid} 连接错误")
        return None
    except Exception as e:
        print(f"  [调试] ❌ appid={appid} 请求异常: {type(e).__name__}: {e}")
        return None


def main():
    print("=" * 60)
    print("开始批量获取Steam游戏信息")
    print("=" * 60)

    # 阶段1: 爬取数据
    print("\n【阶段1】开始爬取游戏数据...")
    result_rows = []
    total = len(APPID_LIST)

    for idx, appid in enumerate(APPID_LIST, 1):
        print(f"\n[{idx}/{total}] 正在获取 appid = {appid}")
        print("-" * 40)

        game_info = get_single_game(appid)

        if game_info:
            result_rows.append(game_info)
            print(f"  ✅ 已添加到结果列表 (当前共 {len(result_rows)} 条)")
        else:
            print(f"  ❌ 获取失败，跳过该游戏")

        print(f"  ⏳ 等待 {SLEEP_SEC} 秒...")
        time.sleep(SLEEP_SEC)

    print("\n" + "=" * 60)
    print(f"【阶段1完成】成功获取 {len(result_rows)} / {total} 条数据")

    if not result_rows:
        print("❌ 没有获取到任何数据，程序退出")
        return

    # 阶段2: 初始化Spark
    print("\n【阶段2】初始化Spark...")
    print("  ⏳ 正在创建SparkSession...")

    try:
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

        print("  ✅ SparkSession创建成功")
        print(f"  Spark版本: {spark.version}")

    except Exception as e:
        print(f"  ❌ Spark初始化失败: {e}")
        return

    # 阶段3: 创建DataFrame
    print("\n【阶段3】创建DataFrame...")

    try:
        df = spark.createDataFrame(result_rows)
        print(f"  ✅ DataFrame创建成功")
        print(f"  数据行数: {df.count()}")
        print(f"  列数: {len(df.columns)}")
        print(f"  列名: {df.columns}")

    except Exception as e:
        print(f"  ❌ DataFrame创建失败: {e}")
        return

    # 阶段4: 显示数据预览
    print("\n【阶段4】数据预览（前5行）:")
    print("-" * 60)

    try:
        df.select("appid", "name", "publishers", "release_date", "price_final_cny").show(5, truncate=False)
        print("-" * 60)

        # 显示数据统计
        print("\n数据统计:")
        print(f"  游戏数量: {df.count()}")
        print(f"  平均价格: {df.selectExpr('avg(price_final_cny)').collect()[0][0] or 0:.2f} CNY")
        print(f"  有折扣的游戏: {df.filter(df.discount_percent > 0).count()}")

    except Exception as e:
        print(f"  ❌ 数据预览失败: {e}")

    # 阶段5: 写入Iceberg表
    print("\n【阶段5】写入HDFS Iceberg表...")
    print(f"  目标表: {target_table}")

    try:
        # 先检查HDFS是否可访问
        print("  ⏳ 检查HDFS连接...")
        hdfs_path = "hdfs://localhost:9000/iceberg_warehouse"

        try:
            # 尝试列出HDFS目录
            hdfs_files = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark.sparkContext._jsc.hadoopConfiguration()
            ).listStatus(
                spark.sparkContext._jvm.org.apache.hadoop.fs.Path(hdfs_path)
            )
            print(f"  ✅ HDFS连接成功，warehouse路径存在")
        except Exception as e:
            print(f"  ⚠️  HDFS路径可能不存在或无法访问: {e}")
            print("  将继续尝试写入...")

        # 执行写入
        print("  ⏳ 正在写入数据到Iceberg表...")

        # 方案1: 使用createOrReplace（如果表存在则替换）
        df.writeTo(target_table).createOrReplace()
        print(f"  ✅ 数据写入成功 (使用createOrReplace)")

        # 验证写入
        print("  ⏳ 验证写入结果...")
        count = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]
        print(f"  ✅ 表 {target_table} 当前共有 {count} 条数据")

        # 显示写入的数据
        print("\n写入的数据预览:")
        spark.sql(f"SELECT appid, name, publishers, release_date, price_final_cny FROM {target_table} LIMIT 5").show(truncate=False)

    except Exception as e:
        print(f"  ❌ 写入Iceberg表失败: {type(e).__name__}: {e}")
        print("  💡 可能的原因:")
        print("     1. HDFS服务未启动 (请运行: start-dfs.sh)")
        print("     2. Iceberg表已存在但schema不匹配")
        print("     3. 权限不足")
        print("     4. 网络问题")

        # 尝试备选方案：写入本地CSV
        print("\n  ⏳ 尝试备选方案：写入本地CSV...")
        try:
            csv_path = "steam_games_backup.csv"
            df.write.mode("overwrite").option("header", "true").csv(csv_path)
            print(f"  ✅ 数据已备份到本地: {csv_path}")
        except Exception as e2:
            print(f"  ❌ 本地备份也失败: {e2}")

    print("\n" + "=" * 60)
    print("程序执行完毕")
    print("=" * 60)

    # 关闭Spark会话
    spark.stop()
    print("Spark会话已关闭")


if __name__ == "__main__":
    main()