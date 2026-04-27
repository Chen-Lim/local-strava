---
name: strata
description: 用于这个 `strata` 项目的本地 Skill。当用户要同步 Strava 归档、检查 inbox 批次、导出新活动、重建 FIT 入库、查看 DuckDB 表结构、写 SQL 分析自己的运动数据，或调试本仓库与 Strava 数据管线时使用。
---

# strata Skill

这个 Skill 面向仓库 `/Users/chen/zed/local-strava`，项目名已更改为 `strata`。

## 何时使用

在下面这些场景触发：

- 用户要处理 Strava 全量归档导出。
- 用户要运行或排查 `sync`、`scan`、`export-new`、`reingest`、`db-info`、`tables`、`schema`、`query`。
- 用户要用 SQL 分析自己的跑步、骑行、心率、配速、海拔、训练量等数据。
- 用户要理解这个仓库的数据流：`inbox/` -> `library/` -> `state/strava.db` / `state/activities.duckdb`。

不适用于与本仓库或本地 Strava 数据无关的通用问题。

## 先做什么

先确认当前仓库根目录，再按需要读取这些文件：

- `README.md`：总体用法和命令入口。
- `src/cli/mod.rs`：命令分发与真实 CLI 语义。
- `docs/fit-to-duckdb.md`：只有在需要解释表设计、字段单位、FIT 到 DuckDB 的映射时再读。

如果只是做数据分析，优先先看实际数据库而不是猜字段。

## 常用命令

默认在仓库根目录执行：

```bash
cargo run -- sync [batch_name]
cargo run -- scan
cargo run -- export-new [YYYY-MM-DD]
cargo run -- reingest <activity_id>
cargo run -- reingest --all
cargo run -- db-info
cargo run -- tables
cargo run -- schema <table>
cargo run -- query "<SQL>" --json
```

## 推荐工作流

### 1. 同步或排查导入

1. 先用 `scan` 看 `inbox/` 下有哪些合法批次。
2. 需要导入时运行 `sync`，必要时可指定 `batch_name`。
3. 想导出最近一次新增活动时运行 `export-new`。
4. 某个 FIT 入库异常时优先用 `reingest <activity_id>`，只有明确需要时才用 `reingest --all`。

### 2. 做数据分析

1. 先运行 `db-info` 或 `tables` 看数据库是否已建立、哪些表非空。
2. 运行 `schema <table>` 确认真实列名和类型。
3. 再写 `query` SQL；优先聚合、过滤和限制返回行数。
4. 需要机器可读结果时加 `--json`。

常见查询模式：

```sql
SELECT sport, count(*) AS n, AVG(total_distance) AS avg_distance_m
FROM session
GROUP BY 1
ORDER BY n DESC;

SELECT timestamp, heart_rate, enhanced_speed
FROM record
WHERE activity_id = '<ACTIVITY_ID>'
ORDER BY timestamp;

SELECT date_trunc('month', start_time) AS month, sum(total_distance) AS distance_m
FROM session
GROUP BY 1
ORDER BY 1;
```

## 约束

- 分析 DuckDB 时只做只读查询，避免手动执行 `INSERT`、`UPDATE`、`DELETE`、`DROP`。
- 不要臆测表或列名，先用 `tables` / `schema` 验证。
- 单位默认按项目现有约定理解：距离米、速度 m/s、时间 UTC、心率 bpm。
- 当 README 和实际代码不一致时，以 `src/cli/mod.rs` 的当前实现为准，并在回复中指出差异。

## 回答风格

- 先给结论，再给支撑它的命令或 SQL。
- 用户问“最近活动”“本月里程”“心率趋势”这类问题时，直接落到可执行查询，不要只讲概念。
- 如果结果依赖本地数据状态，明确说明结论基于当前仓库里的 `state/activities.duckdb` 与 `state/strava.db`。
