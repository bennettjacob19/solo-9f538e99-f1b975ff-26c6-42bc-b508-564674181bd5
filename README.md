# 固件构建流水线 API

FastAPI + SQLite 实现的 DAG 构建流水线：接收至多 12 步的 JSON DAG，在挂载工作目录中
执行真实 shell 命令，全局 2 个名额由中央调度器**跨构建统一调度**，提交时可标记
普通构建（`normal`）或紧急补丁（`urgent`），带依赖调度、紧急优先与防饥饿轮流、
取消、超时、进程组清理和重启恢复。

## 跨构建调度语义（v2）

- **两类构建**：`POST /builds` 请求体可选 `"priority": "normal" | "urgent"`，
  缺省为 `normal`；响应与列表/详情接口均回显 `priority`。
- **两个全局名额**：覆盖命令执行与残留进程清理的整个步骤生命周期，名额在进程组
  清空、步骤终态落库后才真正释放；**不抢占**任何已运行步骤。
- **每次名额真正释放后只选一次**，且只从"依赖已全部 `succeeded` 的待运行步骤"中选取：
  1. 已取消构建的排队步骤一律不得启动（直接剔除）；失败步骤的后代继续 `skipped`；
  2. **紧急优先**：有待启动的紧急步骤时优先分配；
  3. **防饥饿**：连续两次名额分给紧急后，只要有普通步骤就绪，下一次必须给普通；
     普通被服务或无普通就绪时连计清零/不累计；
  4. **同优先级跨构建轮流**：不同构建按提交顺序（构建 id）循环轮转取得名额，
     单个构建不能连续拿走名额、也无法提前占满等待队列；同构建内多步骤按入池先后。
- 每个构建只负责推进自己的 DAG 并把就绪步骤放进全局待选池，不再自行抢占名额。

## 功能与语义对照

| 需求 | 实现 |
| --- | --- |
| 至多 12 步 JSON DAG，含命令/依赖/超时 | `POST /builds`，Pydantic 校验 + 重复 id / 悬空依赖 / 自依赖 / 有环检测（400/422） |
| 提交时标记普通/紧急 | 请求体 `priority`（`normal`/`urgent`，默认 `normal`），落库并在响应回显 |
| 挂载工作目录执行真实命令 | 每构建独立目录 `$PIPELINE_DATA_DIR/workdir/<build_id>/`，compose 挂载命名卷 `/data` |
| 全局两个名额，覆盖执行与清理 | 中央调度器统一计数，名额持有到进程组清空、终态落库后才释放 |
| 名额释放后只从依赖成功的待运行步骤选取 | 构建任务仅在依赖全 `succeeded` 时入全局待选池，调度器逐名额选取 |
| 紧急优先 + 连续两次紧急后让普通 | `urgent_streak` 连计，达 2 且普通就绪则强制普通层 |
| 同优先级跨构建轮流、单构建不占满队列 | 每层轮转游标在"有未完成步骤的活跃构建"间按构建 id 循环，选中即推进 |
| 不抢占已运行步骤 | 调度只决定下一个启动谁，运行中步骤跑到终态才释放名额 |
| 已取消构建的排队步骤不得启动 | 选取前与启动前双重取消检查，入池条目随取消即时剔除 |
| 失败使后代跳过，独立分支继续 | 上游出现失败/取消/中断即置后代 `skipped`；无依赖关系的分支不受影响 |
| 整次取消停止所有未完成步骤 | `POST /builds/{id}/cancel`，per-build 事件即时通知；未启动步骤不再启动 |
| 超时/取消终止对应进程组 | 每步 `start_new_session=True` 独立进程组，`killpg(PGID, SIGTERM→SIGKILL)` |
| 主命令退出后慢子进程清理并判失败 | 退出后扫描 `/proc` 检查组内残留，有残留即清理且步骤判 `failed` |
| 进程组清空前不释放名额/不确认终态 | 清理完成、终态落库后回调释放名额；期间状态可见为 `cleaning_up` |
| 慢清理不阻塞另一名额 | 清理是异步等待，只占自己的名额，另一名额正常调度其他步骤/构建 |
| 重启先清理旧残留再标中断 | startup 恢复：PID+starttime 双重身份确认后终止旧组，再统一置 `interrupted` |
| 未跑步骤不再启动、不误杀后来构建 | 恢复不恢复调度；身份不符（PID 复用）的进程绝不触碰 |
| 重复取消不改终态 | 终态构建上的取消请求幂等忽略，返回说明，不写库 |
| 逐步状态与日志 | `GET /builds/{id}` 全量状态；`GET /builds/{id}/steps/{sid}/logs`（支持 `?tail=N`） |

步骤状态机：`pending → running → cleaning_up → succeeded|failed|cancelled|interrupted`，
失败后代为 `skipped`。构建终态：全部成功 `succeeded`；有失败 `failed`；
取消导致的终止 `cancelled`；重启中断 `interrupted`。

## 快速启动（Docker Compose）

```bash
docker compose up --build
# API:      http://localhost:8000
# Swagger:  http://localhost:8000/docs
# 健康检查: http://localhost:8000/health
```

容器以 `tini` 作为 PID 1 回收孤儿/僵尸进程；数据全部落在命名卷 `pipeline-data`
（SQLite、工作目录、步骤日志），重启不丢、恢复逻辑依赖它。

## 本地直接运行（无 Docker 时）

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
PIPELINE_DATA_DIR=./data .venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 示例构建

`examples/` 下提供：

- `success_build.json` — prepare → compile → unit-test → package 全成功链路（普通）
- `failure_build.json` — 编译失败、后代跳过、独立分支继续
- `slow_child_build.json` — 主命令退出 0 但派生 `sleep 300` 慢子进程，验证清理与判失败
- `timeout_build.json` — 2s 超时终止睡眠 60s 的进程组
- `urgent_patch_build.json` — `priority=urgent` 紧急补丁，名额释放后优先于排队普通构建

一键演示（API 已启动时）：

```bash
.venv/bin/python examples/run_demo.py            # 默认 http://127.0.0.1:8000
```

手动提交（含紧急补丁）：

```bash
# 普通构建（priority 可省略，默认 normal）
curl -s -X POST localhost:8000/builds \
  -H 'Content-Type: application/json' \
  --data @examples/success_build.json
# 紧急补丁
curl -s -X POST localhost:8000/builds \
  -H 'Content-Type: application/json' \
  --data @examples/urgent_patch_build.json
curl -s localhost:8000/builds/1
curl -s localhost:8000/builds/1/steps/compile/logs
curl -s -X POST localhost:8000/builds/1/cancel
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/builds` | 提交 DAG 构建，body 可选 `priority=normal\|urgent`（201 返回含逐步状态） |
| GET | `/builds?limit=N` | 构建列表（新→旧），含 `priority` |
| GET | `/builds/{id}` | 构建与全部步骤状态 |
| POST | `/builds/{id}/cancel` | 整次取消（幂等） |
| GET | `/builds/{id}/steps/{step_id}/logs?tail=N` | 步骤日志 |
| GET | `/health` | 健康检查与最近一次启动恢复报告 |
| GET | `/docs` | Swagger UI，可直接提交两类构建并查询状态 |

## 进程组与重启恢复的关键设计

1. 每个步骤由 `/bin/sh -c <command>` 在新会话中启动，主命令为进程组组长；
   命令派生出的所有子进程默认同组，可被 `killpg` 一次性覆盖。
2. 步骤终态确认前必须满足：`/proc` 扫描该 PGID 已无任何非僵尸成员。
   清理序列为 SIGTERM 宽限（默认 5s）→ SIGKILL → 硬等排空（默认 60s 有界）。
3. 主命令退出码为 0 但组内仍有成员时，清理后步骤判 `failed`，杜绝"误发包"。
4. SQLite 中记录每步的 `pid / pgid / proc_starttime`。重启恢复时先校验
   `/proc/<pid>/stat` 的 starttime 与 pgid 均匹配才终止；PID 已被复用的
   后来构建进程不会被误杀。组长已死但组内仍有成员时，因 PGID 不可能被复用，
   残留成员仍会被清理。
5. 恢复在应用开始接受请求前完成；旧构建统一置 `interrupted`，调度不恢复。

## 测试

```bash
.venv/bin/python -m pytest tests/ -v
```

- `tests/test_validation.py`：DAG 校验、12 步上限、超时正数、进程身份识别。
- `tests/test_scheduling_policy.py`：中央调度选取策略的纯单元测试——紧急优先、
  连续两次紧急后强制普通、同优先级跨构建轮流、已取消构建不启动、同构建内顺序。
- `tests/test_integration.py`：以真实 uvicorn 子进程端到端验证成功、失败跳过、
  慢子进程清理判失败、超时、并行上限 2、整次取消、重复取消幂等、慢清理不占
  另一名额、重启恢复（含不误杀后来构建）、**紧急插队、紧急连计让位普通、
  跨构建轮流单构建不占满队列、取消后排队步骤不启动且名额不泄漏**、错误请求与文档接口。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PIPELINE_DATA_DIR` | `./data` | SQLite/工作目录/日志根目录 |
| `PIPELINE_MAX_STEPS` | `12` | 单构建步骤上限 |
| `PIPELINE_MAX_PARALLEL` | `2` | 全局并行名额 |
| `PIPELINE_TIMEOUT_CAP_SECONDS` | `86400` | 单步超时上限 |
| `PIPELINE_TERM_GRACE_SECONDS` | `5` | SIGTERM 宽限 |
| `PIPELINE_KILL_GRACE_SECONDS` | `60` | SIGKILL 后硬等上限 |
