# 星载回传分区交接协调器 (Partition Handoff Coordinator)

地面站在切换某回传分区的接收实例时，本服务保证核心不变量：

> **任一分区在任一代次至多归属一台接收实例。**

目标改变但仍被旧实例持有的分区先进入**撤销 (revoking)**，绝不直接发给新实例；
旧实例在**当前代次**确认所列撤销分区后，服务在**同一个持久化提交**中删除旧所有权、
推进可交接集合并公布新代次。进程在"释放后、发布前"中断重启，也不会双重归属。

实现仅依赖 Python 3.11 标准库（HTTP 用 `http.server`，持久化用 SQLite WAL）。

## 模型

* 每个分区有一个持久化条目：`generation / owner / target / revoking`。
* `owner` 是当前唯一可消费方；`target` 是成员快照决定的下一目标。
* `owner != target` 时 `revoking=true`，分区仍归旧 owner，直到旧 owner 以当前
  代次确认撤销；确认成功后同一事务内 `owner←target, generation+1, revoking=false`，
  并推进可交接集合 `handoff`（分区 → 已发布代次）。
* 目标分配是 `(成员标识, 分区号)` 的纯函数（稳定哈希），成员集合相同即收敛到同一目标，
  与提交顺序无关。
* 每个请求带稳定 `request_id`；相同请求重放首次结果（`replayed: true`），
  复用同一 id 但快照/确认内容不同返回 `409 request_id_conflict`。
* 确认前先持久化一条 durable intent：进程在 intent 之后、发布之前崩溃时，所有权仍
  完整属于旧实例；重启时 `recover_pending()` 以**最新已持久化 target** 恢复提交。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/healthz` | 健康检查，返回 `{"status":"ok"}` |
| `GET` | `/` | 当前分配视图（仅旧完整分配或符合已确认释放的中间分配） |
| `POST` | `/snapshots` | 提交完整成员快照 |
| `POST` | `/confirmations` | 旧实例确认撤销分区 |

### 提交快照

```json
POST /snapshots
{
  "request_id": "snap-0001",
  "members": [{"member_id": "station-a"}, {"member_id": "station-b"}]
}
```

响应包含 `assignments`（当前归属）、`targets`（下一目标）、`revoked`（全部撤销中
分区）与 `newly_revoked`（本次新进入撤销的分区）。

### 确认撤销

```json
POST /confirmations
{
  "request_id": "ack-0001",
  "member_id": "station-a",
  "generation": 3,
  "partitions": [2, 7]
}
```

整批校验，任一分区不合法则全部拒绝、不推进代次：

* 代次不是当前代次 → `409 stale_confirmation`
* `member_id` 不是这些分区的 owner → `403 unauthorized_confirmation`
* 含未处于撤销的分区（多余分区）→ `409 unexpected_partition`
* 分区不存在 → `404 unknown_partition`

成功响应：

```json
{
  "released": [{"partition": 2, "generation": 4, "owner": "station-b"}],
  "assignments": {...},
  "revoked": [...],
  "handoff": {"2": 4, "...": 3}
}
```

## 运行（Docker Compose）

宿主端口可配置（默认绑定 `127.0.0.1:8080`）：

```bash
docker compose up --build
APP_PORT=9090 APP_HOST=0.0.0.0 docker compose up --build
```

数据持久化在 `handoff-data` 卷的 SQLite 数据库中。

## 一次性 verify 容器

围绕**成员替换、失效/过期/越权/多余确认、请求重放冲突、快照再收敛、并发、
释放后发布前中断恢复**运行规则测试，然后镜像构建、等待健康端点并执行 API 冒烟，
最终以容器状态码退出：

```bash
docker compose --profile verify run --build --rm verify
```

## 本地（无 Docker）

```bash
python -m unittest discover -s tests -v   # 27 个规则测试（含子进程崩溃恢复）
python scripts/smoke.py                    # 启停本地服务 + 崩溃重启端到端冒烟
PORT=8080 python -m app                    # 直接运行服务
```

可配置环境变量：`HOST`、`PORT`、`PARTITION_COUNT`（默认 32）、`HANDOFF_DB`
（默认 `/data/handoff.db`）、`LOG_LEVEL`。

## 目录

```
app/store.py       持久化状态机（SQLite、幂等、durable intent、崩溃恢复）
app/httpapi.py     HTTP 端点
tests/test_rules.py 规则/不变量/并发测试
tests/crash_driver.py 崩溃恢复子进程驱动
scripts/smoke.py   端到端 API + 中断恢复冒烟
scripts/verify.sh  单次容器入口
Dockerfile, docker-compose.yml
```
