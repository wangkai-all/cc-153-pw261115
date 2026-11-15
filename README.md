# 本地对象采集演示（分页浏览 · 版本化下载 · 断点续传 · 清单）

从零实现的本地演示：一个**模拟版本化对象存储**（带分页游标、版本历史、删除标记、断流故障注入）
＋一个**采集服务与网页入口**（按前缀浏览、选版本、发现、下载、续传、入库、清单导出）。

全部使用 Python 3 标准库，不需要云账号、不需要安装依赖。

```
python3 -m app.run            # 或 ./scripts/run-demo.sh
# 网页  http://127.0.0.1:8000    模拟存储 http://127.0.0.1:9001
```

首次启动会写入一批确定性的演示数据（`var/store`、`var/ingest`）。用页面右上角「重置演示」可清空采集记录并重建存储。

---

## 页面能做什么

**① 浏览对象存储**：输入前缀（如 `datasets/`、`incoming/`），服务端按字典序分页返回对象。
每行显示对象键、大小、**当前版本号**、**内容指纹（SHA-256）**、是否删除标记。
展开「查看 N 个版本」可看到该键的完整版本历史（含删除标记），勾选具体版本加入选择。
「跟随最新」表示下载期间若出现更新版本就改抓最新；不勾选则固定该版本号。

**② 模拟存储侧变化**：在分页或下载途中制造真实变化——
- 覆盖对象（写入同名键的新内容，指纹随之改变）
- 生成删除标记 / 删除后重建
- 断流（服务端在指定字节后关闭连接）
- 让分页游标失效（存储换代游标纪元）

**③ 采集任务**：显示代次、页数、键/版本数、完成与跳过数、总进度。
四个视图：
- **版本记录**：每个版本一条，列出「发现（来源/时间/代次）→ 下载（字节/尝试次数）→ 入库（状态/指纹）」。
- **传输**：每个传输任务一行，含父任务与 `superseded_by` 关系、进度、尝试次数、说明。
- **事件**：带时间戳的完整事件流（`scan.page`、`scan.cursor_expired`、`transfer.retargeted`、
  `transfer.superseded`、`import.skip`、`transfer.complete` …）。
- **清单**：当前清单快照；顶部按钮可下载 JSON / CSV 最终清单。

---

## 核心规则（以及为什么这样实现）

### 1. 是否采过：版本号 + 内容指纹，不看文件名和时间

一条「源对象」的身份是 `(key, version_id, etag, size)`。本演示的 ETag 就是内容的 SHA-256，
所以 ETag 相同即字节相同。
- 判断跳过：`complete` 记录必须**版本、ETag、大小全部相等**，且落地文件仍存在；
  只有文件名和更新时间相同不算。
- 跨任务：若另一个任务已完整落地同一 `(key, version, etag, size)`，本任务写一条 `origin=reused`
  记录并复用文件，**不重复下载**。
- 发现历史：列表只给每个键的当前版本；随后逐键读取版本历史，
  因此「删除后重建」的键会同时出现旧版本、删除标记和新版本，各自可独立选择。

### 2. 分页游标失效 → 允许重新扫描

游标是服务端签发的不透明 token，内含游标纪元 `epoch`。
存储侧「使游标失效」会让纪元 +1，旧游标请求返回 `410 CURSOR_EXPIRED`。
采集端收到 410 后：
- 丢弃当前游标，**从第一页重新扫描**，并把 job 的 `generation` +1（事件里可见）；
- 已发现的 `(key, version)` 按唯一键去重，重扫不会重复记录；
- 不会因为游标失效而漏掉对象；页面会提示「已从头重新扫描，请重新确认选择」。

### 3. 断点续传：先确认服务端版本没变，再追加

下载直接写入 `var/ingest/partial/*.part`，每 256 KiB 落一次进度（字节数持久化到 SQLite），
所以进程中断、连接中断、取消都不会丢进度。

续传前做两层校验：
1. 请求携带 `If-Match: <etag>` 与 `Range: bytes=<offset>-`；服务端若不认这个指纹返回 412，
   若不支持 Range 返回 200 → 采集端都会**放弃追加**，改走「另建任务」流程。
2. 落地前对整个文件流式重算 SHA-256，与发现时的 ETag 比对；不一致标记 `failed_integrity`，不发布。

### 4. 服务端变了 → 另建任务，绝不拼接

续传前先 `HEAD` 确认服务端仍是同一个 `(version, etag, size)`。出现以下任一情况：
- `version_id` 变了（键被覆盖）；
- 同版本号但 `etag`/`size` 变了；
- 下载中途 `If-Match` 得到 412；
- 字节全部到齐、发布前最后一次 `HEAD` 发现源已变化或已删除；

则：
- 旧传输标为 `superseded_version_changed`，写入 `superseded_by=<新任务 id>`；
- 旧的半截文件移入 `var/ingest/quarantine/<旧任务 id>/`（不删除，便于取证）；
  同时清空它的 staging 指针，杜绝被后续续传命中；
- 为新版本创建**全新传输任务与全新文件**，从 0 字节开始；
- 事件流记录 `transfer.retargeted` / `transfer.superseded` / `transfer.changed_before_publish`。

测试 `test_overwrite_during_partial_creates_new_transfer` 会断言：
新文件的字节里不包含旧版本内容（`VERSION-ONE` 出现 0 次），且旧任务与新任务的文件路径不同。

### 5. 删除标记（tombstone）

键被删除后，存储保留一条删除标记（S3 versioning 语义），列表仍会返回该键。
采集端把删除标记作为**发现记录**写入清单，状态 `tombstone`，**从不下载**（无内容可下）。
选择被删除的键时，清单里能同时看到：旧版本 → 删除标记 → 重建版本。

### 6. 清单

每个任务可随时导出 JSON / CSV。字段包括：
`object_key, version_id, size, etag_sha256, content_type, is_current, is_delete_marker,
discovery_source, discovered_at, last_seen_at, generation, import_status, bytes_downloaded,
content_sha256, file, reason, transfer_id, transfer_attempts, transfer_chain`。
`transfer_chain` 把同一对象版本的多次传输按 `id:state(bytes, attempts)` 串起来，
因此「一次中断、一次续传、一次因源变化而作废」在 CSV 里也是完整可追溯的。

---

## 演示脚本（建议顺序）

1. 前缀填 `datasets/`，每页 3，点浏览 → 翻两页。
2. **翻页途中**点「使游标失效」→ 页面提示已从头重扫；继续翻页不丢对象。
3. 展开 `datasets/2025/events-01.csv`，勾选两个历史版本，点「发现并导入」。
4. 在「模拟存储侧变化」里覆盖 `incoming/batch-a/payload.bin`，然后在任务里对同一键重新扫描 → 看 `import.retarget`。
5. 断流演示：
   - 断流对象键 `incoming/batch-a/payload.bin`，断在 200000 字节，次数 1；
   - 新建任务并勾选该键、自动导入 → 传输停在 `interrupted`，进度保留；
   - 点「续传 / 继续」→ 事件显示 `resuming at 200000B`，完成后 `transfer.complete`。
6. 源变化演示：
   - 断流 `scratch/...` 大对象，停在中断；
   - 用「覆盖对象」写同名新内容；
   - 点「续传 / 继续」→ 旧任务变 `superseded_version_changed`，并生成新任务下载新内容；
   - 在「传输」页看 `superseded_by`，在 `var/ingest/quarantine/` 里能看到被隔离的旧半截文件。
7. 删除标记：对 `archive/removed-object.txt` 重新扫描 → 版本记录显示 `tombstone`，无下载。
8. 导出 CSV，检查 `transfer_chain` 和 `content_sha256`。

---

## API 摘要

采集服务（:8000）：
| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/store/list?prefix=&cursor=&limit=` | 浏览分页（代理存储），410 表示游标失效 |
| GET | `/api/store/versions?key=` | 单键版本历史 |
| POST | `/api/jobs` | 创建任务（`prefix, page_size, page_delay_ms, include_history, auto_import, selections`） |
| GET | `/api/jobs` · `/api/jobs/{id}` | 任务列表 / 详情（含 discoveries、transfers） |
| POST | `/api/jobs/{id}/import` | 导入所选（可带新的 `selections`） |
| POST | `/api/jobs/{id}/resume` | 续传/继续（会重新校验源版本） |
| POST | `/api/jobs/{id}/rescan` | 重新扫描 |
| POST | `/api/jobs/{id}/cancel` | 取消（保留已下载进度） |
| POST | `/api/jobs/{id}/auto-import` | 开关自动导入当前版本 |
| GET | `/api/jobs/{id}/manifest?format=json\|csv` | 下载清单 |
| GET | `/api/jobs/{id}/events?after=` | 事件流（支持增量） |
| POST | `/api/demo/put` `/api/demo/delete` `/api/demo/fault` `/api/demo/invalidate-cursors` | 模拟操作 |
| POST | `/api/demo/reset` · `/api/demo/seed` | 重置全部 / 重建演示数据 |

存储服务（:9001，仅本机使用）：
`GET /api/list`、`GET /api/versions`、`GET /api/head`、`GET /api/download`
（支持 `Range` 与 `If-Match`）、`POST /api/admin/{put,delete,fault,invalidate-cursors,seed}`。

---

## 测试

```
python3 -m unittest tests.test_demo -v
```

14 个端到端用例（都在临时目录起真实 HTTP 服务，不依赖外部环境），覆盖：
分页与版本历史、删除/重建产生不同版本、tombstone、
落地文件 SHA-256 校验、同版本不重复下载、跨任务复用、
断流后续传同一条传输且 offset 正确、覆盖后续传另建任务且不拼接、
游标失效重扫、发现后删除的记录、CSV 清单字段、路径穿越拒绝。

## 目录

```
app/
  store.py       模拟版本化对象存储（SQLite + 内容寻址 blob + 分页游标 + 故障注入）
  storeclient.py 存储 HTTP 客户端（Range / If-Match / 逐块回调）
  ingest.py      扫描、发现、计划、续传、隔离、清单
  ingestdb.py    SQLite 持久化（jobs / discoveries / transfers / events）
  webapp.py      HTTP API 与静态页面服务
  seed.py        确定性演示数据
  static/        页面（index.html, app.js, styles.css）
tests/test_demo.py
```

## 已知边界

- 存储与采集服务都在单机、单进程内；采集任务是进程内后台线程，进程重启后不会自动恢复线程，
  但**已写入的进度和落地文件都在 SQLite/磁盘上**，重启后点「续传」会从持久化字节数继续。
- 演示存储的 ETag 是内容 SHA-256。真实 S3 的 multipart ETag 不是内容哈希；
  代码里用 `_etag_is_checksum` 区分：非 SHA-256 指纹时 `If-Match` 仍生效，但落地校验改用流式哈希本身。
- 浏览页的「上一页」依赖此前记录的游标；游标失效后会强制回到第一页（这是刻意的保守策略）。
