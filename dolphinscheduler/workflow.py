from pydolphinscheduler.core.workflow import Workflow
from pydolphinscheduler.tasks.shell import Shell

PROJECT_PATH = "/path/to/your/steam-lakehouse-platform"

with Workflow(
        name="steam_lakehouse_daily",
        schedule="0 0 2 * * ?",  # 每天凌晨2点执行[citation:9]
        start_time="2025-01-01",
) as workflow:
    # ODS采集
    ods_task = Shell(
        name="ods_collection",
        command=f"cd {PROJECT_PATH} && source .venv/bin/activate && python scripts/collection/ods_steam_review_catch_daily.py"
    )

    # DWD清洗
    dwd_task = Shell(
        name="dwd_cleaning",
        command=f"cd {PROJECT_PATH} && source .venv/bin/activate && python scripts/dwd/dwd_steam_review_firstly.py"
    )

    # DWS汇总
    dws_task = Shell(
        name="dws_aggregation",
        command=f"cd {PROJECT_PATH} && source .venv/bin/activate && python scripts/dws/dws_overview.py"
    )

    # MySQL同步
    sync_task = Shell(
        name="mysql_sync",
        command=f"cd {PROJECT_PATH} && source .venv/bin/activate && python scripts/export/export_to_mysql.py"
    )

    # 定义依赖关系：ODS → DWD → DWS → MySQL同步
    ods_task.set_downstream(dwd_task)
    dwd_task.set_downstream(dws_task)
    dws_task.set_downstream(sync_task)

    workflow.run()