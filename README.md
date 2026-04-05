# Strava Activity Sync

一个用于从 Strava 批量导出归档中同步并管理运动活动的 Rust 命令行工具。
A Rust CLI tool for syncing and managing activity records from Strava's bulk export archives.

---

## 快速指南 / How to Use

### 1. 如何获取你的 Strava 数据 (归档导出)
### How to get your Strava Data (Bulk Export)

你首先需要从 Strava 官网申请全量数据备份：
First, you need to request a full data backup from the Strava website:

1.  登录 [Strava.com](https://www.strava.com/)。
    Log in to [Strava.com](https://www.strava.com/).
2.  进入 **设置 (Settings)** -> **我的账户 (My Account)**。
    Go to **Settings** -> **My Account**.
3.  滚动至页面底部，找到 **下载或删除您的账户 (Download or Delete Your Account)**，点击 **开始 (Get Started)**。
    Scroll to the bottom and find **Download or Delete Your Account**, then click **Get Started**.
4.  在第 2 步中，点击 **申请您的归档 (Request Your Archive)**。
    In Step 2, click **Request Your Archive**.
5.  Strava 会向你发送一封包含下载链接的邮件。下载并解压该压缩包（通常名为 `export_xxxx.zip`）。
    Strava will email you a download link. Download and unzip the archive (usually named `export_xxxx.zip`).

### 2. 将数据放在哪里
### Where to put your data

将下载并解压后的 Strava 导出文件夹放入项目的 `inbox/` 目录中。
Place the downloaded and extracted Strava export folder into the `inbox/` directory of this project.

项目目录结构示例 / Example Directory Structure:
```text
Strava/
├── inbox/
│   └── export_12345/       <-- 在这里放置你的 Strava 导出文件夹 / Put your export folder here
│       ├── activities/     <-- 包含原始活动文件 / Contains raw activity files
│       └── activities.csv  <-- 活动索引 CSV / Activity index CSV
├── library/                <-- 处理后的文件将分类存放在这里 / Processed files will be sorted here
├── state/                  <-- 同步状态和索引 / Sync state and indexing
└── strava-sync             <-- 编译后的可执行文件 / Compiled executable
```

### 3. 运行同步
### Run the Sync

确保你已经安装了 Rust 运行环境，然后在项目根目录下运行：
Ensure you have Rust installed, then run the following command in the project root:

```bash
cargo run -- sync
```

该工具会自动识别 `inbox/` 中的新活动，将其解压、重命名并根据文件格式（FIT/TCX/GPX）存入 `library/` 文件夹。
The tool will automatically detect new activities in `inbox/`, decompress, rename, and sort them into the `library/` folder based on file format (FIT/TCX/GPX).

---

## 主要特性 / Key Features

- **增量同步 (Incremental Sync):** 自动记录已处理的活动，避免重复处理。
- **并行处理 (Parallel Processing):** 利用多线程（Rayon）高效处理大量活动文件。
- **结构化管理 (Structured Library):** 自动将活动重命名为更具语义的名称，并分类存放。
- **一致性检查 (Consistency Check):** 维护本地索引以确保数据完整性。

---

## 许可证 / License

本项目遵循 MIT 许可证。
This project is licensed under the MIT License.
