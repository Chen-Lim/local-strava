---
name: local-strava
description: 用 SQL 分析本地 Strava 活动数据。当用户提到自己的运动/活动/锻炼数据，要看心率/配速/距离/海拔/里程汇总，按运动类型/时间筛选活动，对比训练量，分析睡眠/HRV/功率/踏频，或要画曲线/做统计时使用。包含 264+ 真实 FIT 文件已入 DuckDB，123 张 Garmin Profile 表（record/session/lap/event 等），表头遵循 snake_case 标准。
---

# local-strava Skill

## 1. 触发场景示例与反例
- **触发**：“分析我最近的跑步”、“查看心率分布”、“我这个月的总里程是多少”、“比较这几天的踏频”、“帮我画个海拔曲线”等涉及运动数据统计、对比和可视化的需求。
- **不触发**：“什么是大语言模型”、“如何配置一个 nginx 服务器”等与个人运动活动数据无关的问题。

## 2. 工具入口
在开始分析前，你可以先运行以下命令摸清当前的数据结构和规模：
- `local-strava db-info`：查看当前数据规模的概览摘要。
- `local-strava tables`：列出当前所有的非空数据表及其行数（机器可读格式）。
- `local-strava schema <table>`：打印某张特定表的列名与数据类型。

## 3. 常用查询模板
可以通过 `local-strava query "<SQL>"` 命令来执行分析查询。由于返回的数据量可能很大，建议使用 `--limit` 控制输出或通过 SQL 的聚合函数来归纳数据。如果要方便解析，也可以加上 `--json` 参数。
- **跨活动汇总**：`SELECT sport, count(*), AVG(total_distance) FROM session GROUP BY sport;`
- **按月里程统计**：`SELECT date_trunc('month', start_time) AS month, sum(total_distance) AS dist FROM session GROUP BY month ORDER BY month;`
- **单次活动心率曲线**：`SELECT timestamp, heart_rate FROM record WHERE activity_id='<ACTIVITY_ID>' ORDER BY timestamp;`
- **海拔剖面**：`SELECT distance, enhanced_altitude FROM record WHERE activity_id='<ACTIVITY_ID>' ORDER BY timestamp;`
- **配速分布**：可以基于 `enhanced_speed` 字段来进行分布统计（注意：speed 的单位是 m/s，换算为配速需倒数）。

## 4. 字段说明
表结构和字段严格遵循 Garmin FIT Profile 标准，采用 `snake_case` 命名法。
常用高频字段包括：
- `activity_id`：活动的唯一标识符。
- `timestamp`：记录的时间戳。
- `heart_rate`、`enhanced_speed`、`enhanced_altitude`、`distance` 等。
如果需要详细了解所有字段，请参阅 `docs/fit-to-duckdb.md` 第 3 节的内容。

## 5. 单位约定
所有的指标数据均采用标准单位：
- 距离：米 (m)
- 速度：米每秒 (m/s)
- 温度：摄氏度 (°C)
- 心率：每分钟跳动次数 (bpm)
- 时间：UTC `TIMESTAMP` 格式
- 半圆值 (semicircles)：如 `position_lat` 等在数据存入时已自动转换为标准的“度”。

## 6. 失败处理与调试
- 如果 `query` 命令执行报错（例如遇到未知的列名或语法错误），不要盲目猜测。
- 应当先运行 `local-strava tables` 或 `local-strava schema <table>` 来确认实际存在的表和字段名称，然后再调整 SQL 语句重试。

## 7. 重要约束（不做的事）
- **绝不要执行任何修改数据的操作！** 包括 `INSERT`、`UPDATE`、`DELETE` 或 `DROP` 等。
- DuckDB 中的数据是衍生只读副本。它们可以通过 `local-strava reingest --all` 随时重建，任何手动的修改都会在下一次数据同步时被无情覆盖。请只进行 `SELECT` 查询分析。
