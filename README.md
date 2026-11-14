# Nginx 访问日志持续入仓（FastAPI + SQLite）

把 Nginx 的 JSON 行访问日志从共享只读目录**持续、精确一次（exactly-once）**
地采集进 SQLite，并提供按路径、状态码分页检索故障请求的 HTTP API；
另提供一个**只读单文件核验 API**，随时核对仓库里某个物理文件是否仍与
共享目录中的原件逐行一致。

## 为什么不会漏行 / 重记 / 错用偏移

| 风险 | 设计对策 |
| --- | --- |
| 轮转后把旧偏移用到新文件 | 文件身份用 `(st_dev, st_ino)`，**不是文件名**。rename 后旧 inode 与新 inode 各自独立追踪 |
| 旧文件轮转后仍被短暂写入 | 旧 inode 继续按已提交偏移采集到 EOF；归档永不删除、不截断 |
| 读到写了一半的行 | 只入仓以 `\n` 结尾的完整行；未换行尾部保持原偏移，下轮重读 |
| 进程在写入中途/前后退出 | 行数据与“下一字节偏移”在**同一个 SQLite 事务**提交，重启从最后提交点续采 |
| 查询读到半成品 | WAL + 事务隔离，查询只能看到已完整提交的事务 |
| 内容相同的两行 | 唯一性只约束物理位置 `(dev,inode,source_code,byte_offset)`，不含内容，重复内容各自成行 |
| JSON 坏行 | 隔离进 `malformed_lines` 表，不阻断后续行 |

## 与原件核验（交给排障同事前先自证）

`GET /verify/{dev}/{inode}` 按**物理文件身份**选择已追踪文件（`/ingest/progress`
里就有每个文件的 `dev` 与 `inode`），把保留原件与已入仓记录逐行比对。

**只核对同一物理文件、同一批已提交记录：**

- 以**核验开始时** `ingest_state.offset` 为界，只读原件 `[0, offset)` 的字节，
  只纳入 `byte_offset < offset` 的行；该位置与对应行在同一事务提交，是一份
  不可再变的完整批次。
- 边界之后的字节一律忽略：未换行尾部、已追加但尚未入仓的新行、轮转产生的
  新文件，都**不会**被当成缺行。
- 原件按 inode 在共享目录中查找，不信任 `ingest_state.path`——rename 轮转后
  旧路径可能已指向新文件；打开时再用 `fstat` 确认就是目标 inode。

**三类结果：**

| `status` | `passed` | 含义 |
| --- | --- | --- |
| `verified` | `true` | 已提交批次逐行一致（正常行 + 坏行都核对） |
| `mismatch` | `false` | 检出缺行 / 重复字节位置 / 原文不符 / 多余记录 |
| `unverified` | `false` | 原件缺失、被截断，或核验期间无法稳定读取；**绝不报通过** |

报告给出 `counts`（全量计数）和可分页/截断的明细：
`missing_lines`（仓库无此位置记录的完整行）、`duplicate_positions`
（同一字节位置多条记录）、`content_mismatches`（原文不同）、
`unexpected_records`（仓库有记录但已提交区间内无对应行）。
`boundary.bytes_after_boundary_ignored` 显示边界之后有多少字节未参与核对。

```bash
# 1) 找到要核验的文件身份
curl -s localhost:8000/ingest/progress | python3 -m json.tool
# 2) 核验（dev/inode 换成上一步的值）
curl -s localhost:8000/verify/69/12345 | python3 -m json.tool
# 明细太多时可限制条数（计数仍为全量）
curl -s "localhost:8000/verify/69/12345?limit=20"
```

也可在 http://localhost:8000/docs 的 `verify` 分组里直接填 `dev`/`inode` 执行。

## 目录结构

```
.
├── docker-compose.yml      # nginx + collector + traffic 一键启动
├── collector/              # FastAPI + SQLite 采集/查询服务
│   ├── app/storage.py      # 表结构、事务、查询
│   ├── app/ingest.py       # 目录扫描、inode 识别、完整行切分、续采
│   ├── app/verifier.py     # 只读单文件核验（边界、稳定快照、逐行比对）
│   └── app/main.py         # API（/docs 可用）
├── nginx/                  # JSON 行日志格式 + 可控轮转脚本
├── traffic/                # 持续请求生成器（含 4xx/5xx）
├── scripts/                # 轮转、核验演示、汇总脚本
└── tests/                  # 单元/集成断言 + 端到端模拟
```

## 快速开始

```bash
docker compose build
docker compose up -d
# 实时看日志
docker compose logs -f collector
```

| 服务 | 地址 | 说明 |
| --- | --- | --- |
| API 文档 Swagger | http://localhost:8000/docs | 直接在页面上试所有接口 |
| 示例站点 | http://localhost:8080/ | Nginx，访问即产生 JSON 日志 |
| OpenAPI JSON | http://localhost:8000/openapi.json | |

### 可控轮转与一键核验演示

```bash
# 情况 A：默认演示环境（手动轮转）——自动起服务、产生流量、轮转、
# 向旧 inode 补写、再对每个已追踪文件调用 /verify，打印汇总。
./scripts/verify-demo.sh

# 情况 B：只核验当前已入仓文件，不额外轮转
DEMO_ROTATE=0 ./scripts/verify-demo.sh
```

演示脚本做的是：启动 compose → 等首批流量入库 → rename 轮转并 `USR1`
重开 → 在**旧 inode** 写一行（延迟写入，由归档来源采集）→ 在新
`access.log` 写正常行、坏行和一条**未换行尾部** → 强制采集一轮 →
逐文件核验。你会看到归档来源与当前文件都 `PASS`，未换行尾部出现在
`boundary.bytes_after_boundary_ignored` 而不是 `missing_lines`。

```bash
# 也可以手动轮转（rename 成唯一归档名 + nginx 重开同名新日志）
docker compose exec nginx /usr/local/bin/rotate-nginx-log.sh

# 自定义归档后缀
docker compose exec nginx ROTATE_NAME=incident-1 \
    /usr/local/bin/rotate-nginx-log.sh
```

### 自动周期轮转（演示轮转与采集交错）

```bash
# 每 15 秒自动轮转一次
AUTO_ROTATE_INTERVAL=15 docker compose up -d
# 恢复纯手动
docker compose up -d --force-recreate nginx
```

轮转后共享目录里同时存在 `access.log` 与所有
`access.log.<时间戳>.log` 归档，采集器分别追踪、全部保留。

## 常用查询

```bash
# 采集进度（每个文件的 offset / size、是否采完）
curl -s localhost:8000/ingest/progress | python3 -m json.tool

# 所有 5xx 故障请求，分页
curl -s "localhost:8000/events?status_min=500&status_max=599&limit=50"

# 某路径的 404
curl -s "localhost:8000/events?path=/missing/page&status=404"

# 只看某个归档来源
curl -s "localhost:8000/events?source_code=access.log.20261113T100000Z.log"

# 翻页
curl -s "localhost:8000/events?limit=100&offset=200"

# 立即触发一次采集（默认每 0.5s 自动轮询）
curl -s -X POST localhost:8000/ingest/poll-now

# 无法解析的坏行
curl -s localhost:8000/malformed
```

## API 一览

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/health` | 存活、计数、采集线程状态 |
| GET | `/ingest/progress` | 各来源文件 offset/size/是否完成；含 `dev`/`inode` |
| POST | `/ingest/poll-now` | 立即执行一轮采集 |
| GET | `/events` | 分页取数，支持 `path`、`status`、`status_min/max`、`source_code`、`limit`、`offset` |
| GET | `/events/{id}` | 单条事件（含原始 JSON 行 `raw_line`） |
| GET | `/malformed` | 坏行隔离区 |
| GET | `/verify/{dev}/{inode}` | **只读核验**一个已追踪物理文件与原件是否逐行一致 |

## 数据模型

```
log_events(id, ts, path, status, method, source_code,
           dev, inode, byte_offset, raw_line, ingested_at)
ingest_state(dev, inode, source_code, path, offset, last_seen)  -- 续采位置
malformed_lines(..., raw_line, error)                            -- 坏行隔离
```

- `source_code`：来源文件代号（当前文件名/归档名）。
- `byte_offset`：该行在其来源文件中的起始字节位置。
- 唯一索引 `(dev, inode, source_code, byte_offset)` 是防重的最后防线；
  内容不在键中，所以两行完全相同也分别保留。
- SQLite 运行在 `WAL + synchronous=FULL`：提交即落盘，读不阻塞写。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `LOG_DIR` | `/logs` | 共享日志目录（compose 中以 `:ro` 只读挂载） |
| `DB_PATH` | `/data/warehouse.db` | SQLite 数据库（独立可写卷） |
| `POLL_INTERVAL` | `0.5` | 轮询间隔秒 |
| `LOG_LEVEL` | `INFO` | 日志级别 |
| `AUTO_ROTATE_INTERVAL` | 空 | 仅 nginx 容器；非空时按秒自动轮转 |

## 本地测试（无需 Docker）

```bash
python3 -m venv --without-pip .venv && . .venv/bin/activate
# 若 venv 内无 pip：python get-pip.py
pip install -r collector/requirements.txt httpx pyyaml
python -I tests/test_collector.py   # 46 项：部分行/轮转/崩溃恢复/重复行/API
python -I tests/test_verifier.py    # 47 项：核验缺行/重复/不符/边界/轮转/未核实
python -I tests/e2e_sim.py          # 完整服务启停 + 轮转 + 重启续采 E2E
python -I tests/e2e_verify.py       # HTTP 层核验 E2E（需 fastapi/uvicorn）
```

覆盖的关键场景：未换行尾部等待补齐、rename 轮转双 inode、旧 inode 延迟写入、
20 轮随机“进程退出”后续采不漏不重、相同内容多行保留、坏行隔离、分页与过滤。

核验额外覆盖：只比已提交批次（未换行尾部与未采集新行不算缺口）、轮转后
按 inode 找到归档而非新文件、缺行/重复位置/原文不符逐项可定位、原件缺失或
被截断或读取不稳定时返回 `unverified`、采集与轮转交错时无假缺口。

## 故障排查

```bash
docker compose ps
docker compose logs --tail=100 collector
docker compose exec nginx ls -li /var/log/nginx/json   # 看 inode 与归档
docker compose exec collector sh -c 'ls -li /logs'      # 只读视角
# 入库数与文件总字节核对
curl -s localhost:8000/ingest/progress
```
