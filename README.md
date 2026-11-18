# 事故诊断包断点续传服务

值班工程师接收事故诊断包的上传网页：网络断开可续传，交接时只能下载校验通过的完整文件。

## 功能

- 浏览器创建任务时提交 **文件大小 + SHA-256**，按 **1 MiB** 分块上传
- 重开页面可查看各任务 **已收/缺失分块** 并补齐（断点续传）
- 相同分块同内容重传 **幂等去重**；异内容 **拒绝并保留原块**（409）
- 提交完成时服务端核对 **分块齐全、总大小、整体 SHA-256**，通过才发布下载
- 缺块可补传；摘要不符 **拒绝发布** 并提示重新创建任务
- 多进程并发上传/提交、磁盘写失败、进程重启均不会发布残缺文件或产生两份成品
- 已发布的包重启后仍可下载
- **成品下载限速**：网页设置全服务每秒字节预算与突发额度，所有服务进程的并发下载共用同一预算；下载中修改即时生效，设置重启后保留；下载仍是完整原包的流式传输

## 快速开始

### Docker Compose

```bash
docker compose up --build
# 打开 http://localhost:8000
```

容器内以 `--workers 2` 运行两个服务进程，共享挂载卷中的 SQLite 与数据目录。

### 本地开发

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
DATA_DIR=data .venv/bin/uvicorn app.main:app --port 8000
# 打开 http://localhost:8000
```

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/tasks` | 创建任务 `{filename, size, sha256}`，返回 `task_id` 与分块参数 |
| GET | `/api/tasks` | 任务列表（状态、已收分块数） |
| GET | `/api/tasks/{id}` | 任务详情：`received_chunks` / `missing_chunks` / `state` |
| PUT | `/api/tasks/{id}/chunks/{i}` | 上传第 i 块（1 MiB，最后一块按实际大小）。同内容重传返回 `deduplicated:true`；异内容 409 |
| POST | `/api/tasks/{id}/finalize` | 核对分块+大小+摘要，通过则发布；缺块返回 409+`missing_chunks`；摘要不符标记 `failed` |
| GET | `/api/tasks/{id}/download` | 下载已发布的包（未发布返回 409）；受全服务限速约束，支持单段 `Range` 与 `HEAD` |
| GET | `/api/settings` | 读取下载限速 `{rate_bps, burst_bytes, unlimited}` |
| PUT | `/api/settings` | 设置下载限速 `{rate_bps, burst_bytes}`；`rate_bps=0` 表示不限速 |

任务状态机：`uploading` → `published`（终态）/ `failed`（终态，摘要不符）。

## 下载限速

- **全服务一个预算**：令牌桶存放在共享 SQLite 的 `download_rate` 表中，容器内两个 uvicorn 进程（以及任意数量的并发下载）都从这一个桶取字节，不存在“每进程一份”的放大。
- **流式完整原包**：下载仍按 64 KiB 分块顺序吐出成品文件，`Content-Length`、`Content-Disposition` 与整包字节不变；每个连接有自己的文件偏移，多个客户端同时下载同一包时各自收到的字节连续且准确。
- **等待而非报错**：配额不足时发送方按需睡眠，随后继续发送。限速只表现为变慢，不会产生 4xx/5xx 或“文件损坏”。
- **下载中改设置即时生效**：每个分块发送前都读取当前设置，修改后从下一小段起按新预算发送；无需重连或重启。
- **重启保留**：设置写入 `synchronous=FULL` 的 SQLite 连接，服务重启后仍生效。
- **慢客户端不占坑**：响应独立监听 `http.disconnect`，客户端断开即取消该路发送并释放文件句柄；已在途的最多一个 64 KiB 分块预算不会继续被消耗，也不会拖住其他下载。
- `rate_bps=0` 为不限速（默认），此时限速路径不做任何写操作，下载速度与旧版一致。

## 一致性设计

- **分块幂等**：`(task_id, chunk_index)` 主键。先写临时文件 → INSERT 抢主键 → 赢者原子改名落盘；撞主键则比对内容哈希，相同去重、不同 409 保留原块。
- **发布原子性**：成品先在临时目录完整拼装并 fsync，**校验通过后**才 `os.replace` 原子改名到 published 路径；任何时刻 published 路径要么不存在、要么是完整成品。
- **多进程互斥**：finalize 用 per-task `flock` 串行化；SQLite WAL + busy_timeout 处理并发写。两个进程同时提交，只会有一个完成校验-改名-置态，另一个读到已发布直接返回。
- **崩溃恢复**：状态全在 SQLite（synchronous=FULL）与磁盘。重启后已收分块保留；DB 有记录但文件丢失的块（INSERT 后、改名前崩溃）在 finalize 时被发现并允许补传。
- **磁盘写失败**：改名失败即回滚 DB 记录并返回 500，任务保持 `uploading`，不会产生半成品。

## 测试

```bash
.venv/bin/python -m pytest tests/ -v
```

- `tests/test_app.py`（14 项）：完整流程、续传、幂等去重、异内容拒绝、缺块补传、摘要拒绝、磁盘失败注入（chunk 上传 / finalize 两条路径）、重启恢复、空文件、参数校验。
- `tests/test_common.py`（9 项）：令牌桶纯逻辑（假时钟）：默认不限速、突发用尽后等待再补充、不超发、跨实例共享一个桶、设置即时生效与非法值拒绝、重启后设置保留、取消下载后返还未发送的额度。
- `tests/test_download.py`（13 项）：限速下载仍字节准确、串行与并发下载共享预算、下载中改设置即时生效、断开客户端释放预算、`Range`/`HEAD`、未发布仍 409/404、限速期间上传发布不受影响。
- `tests/test_live.py`（3 项）：两个 uvicorn 进程并发上传/并发提交同一任务、kill -9 后重启续传、已发布包重启后可下载、跨进程同块并发去重。
- `tests/test_live_download.py`（4 项）：两个真实 uvicorn 进程共享一个预算、在一进程上改设置影响另一进程的在途下载、全部重启后预算仍生效、真实断连后预算不被僵尸连接占用。

## 数据布局（`DATA_DIR`）

```
uploader.db        SQLite（WAL）：任务、分块与全服务下载限速设置/令牌桶
chunks/<task>/     已收分块
published/<task>   校验通过后的成品（原子改名出现）
tmp/               临时文件（启动时清理超过 1 小时的残留）
locks/             per-task flock 文件
```
