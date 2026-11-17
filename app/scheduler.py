"""跨构建流水线调度器。

并发模型（v2：跨构建统一调度）：
- 两个全局名额由中央调度器 ``_dispatch_loop`` 统一管理，名额在整个步骤生命周期内
  持有，包含进程组清理阶段 —— 组未清空前不释放名额、不写步骤终态。
- 每个构建一个 asyncio 任务推进自己的 DAG：依赖全部 succeeded 的待运行步骤进入
  全局"待选池" ``_ready``；构建任务不再自行抢名额，因此单个构建不可能提前占满
  等待队列。
- 每次名额真正释放（步骤任务结束回调）后唤醒调度器，只从待选池中选取一次：
  1. 已取消构建的排队步骤不得启动（直接剔除）；
  2. 紧急优先；连续两次分给紧急后，若有普通步骤就绪，下一次必须给普通；
  3. 同优先级内不同构建按提交顺序轮流取得名额，同一构建不能连续拿走名额；
  4. 同构建内多个就绪步骤按进入待选池的先后（seq）排序。
- 不抢占已运行步骤：调度只决定"下一个启动谁"，运行中的步骤跑到终态才释放名额。
- 慢清理只占住自己的名额，不阻塞另一名额上的独立步骤/构建。
"""
from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from . import config, proc
from .db import Database
from .models import (
    BUILD_TERMINAL,
    STEP_TERMINAL,
    BuildCreate,
    validate_dag,
)

_STEP_FAIL_STATES = {"failed", "skipped", "cancelled", "interrupted"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class _ReadyEntry:
    """待选池条目：依赖已全部成功、等待全局名额的步骤。"""

    build_id: int
    uid: str
    step_pk: int
    priority: str
    seq: int  # 进入待选池的全局先后序号
    row: Any = field(repr=False)


class Scheduler:
    def __init__(self, db: Database) -> None:
        self.db = db
        self._build_events: dict[int, asyncio.Event] = {}
        self._build_tasks: dict[int, asyncio.Task[None]] = {}

        # ---- 中央调度状态（全部只在事件循环线程内读写，无需加锁）----
        self._ready: dict[tuple[int, str], _ReadyEntry] = {}
        self._inflight: dict[asyncio.Task[str], _ReadyEntry] = {}
        self._free_slots = config.MAX_PARALLEL
        self._wake_event = asyncio.Event()
        self._dispatcher_task: Optional[asyncio.Task[None]] = None
        self._stopping = False
        # 公平性账本
        self._seq_counter = 0
        self._urgent_streak = 0  # 连续分给紧急的次数（上限 2）
        # 每个优先级层一个轮转游标：同优先级内不同构建按 id（提交顺序）循环轮流，
        # 单个构建不能连续拿走名额；游标只记录"上一个被服务的构建"。
        self._last_build_by_priority: dict[str, Optional[int]] = {
            "normal": None,
            "urgent": None,
        }
        # 仍有未完成步骤的构建 id（按提交顺序）。轮转在"有活的构建"之间进行，
        # 而不是只看当前已入池条目：否则先跑完一轮主循环的构建会在其他构建
        # 入池前连拿多个名额，破坏跨构建轮流。
        self._active_builds: set[int] = set()

    # ------------------------------------------------------------------ API

    async def start(self) -> None:
        """恢复完成后调用：启动中央调度循环。"""
        if self._dispatcher_task is None:
            self._dispatcher_task = asyncio.create_task(
                self._dispatch_loop(), name="central-dispatcher"
            )

    async def shutdown(self) -> None:
        """进程关闭：停止调度并收尾构建任务；在途进程交由下次启动的恢复逻辑处理。"""
        self._stopping = True
        self._wake_event.set()
        tasks: list[asyncio.Task[Any]] = []
        if self._dispatcher_task is not None:
            self._dispatcher_task.cancel()
            tasks.append(self._dispatcher_task)
        tasks.extend(self._build_tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def submit(self, payload: BuildCreate) -> int:
        validate_dag(payload.steps)  # ValueError 由路由层转 400
        build_id = await self.db.create_build(
            payload.name, payload.priority, _now()
        )
        log_dir = config.LOG_ROOT / str(build_id)
        log_dir.mkdir(parents=True, exist_ok=True)
        for step in payload.steps:
            log_path = log_dir / f"{step.id}.log"
            log_path.touch()
            await self.db.create_step(
                build_id,
                step.id,
                step.command,
                step.depends_on,
                step.timeout_seconds,
                str(log_path),
            )
        # 在创建构建任务前登记为活跃构建：保证中央调度器在任何时刻看到的
        # 活跃集合都包含已提交构建，跨构建轮转会为尚未入池的构建保留次序
        self._active_builds.add(build_id)
        task = asyncio.create_task(self._run_build(build_id), name=f"build-{build_id}")
        self._build_tasks[build_id] = task
        task.add_done_callback(self._log_task_exception)
        self._wakeup()
        return build_id

    async def cancel(self, build_id: int) -> tuple[Optional[dict[str, Any]], str]:
        """返回 (构建行或 None, 说明)。重复取消幂等，不改变已定终态。"""
        row = await self.db.get_build(build_id)
        if row is None:
            return None, "构建不存在"
        if row["status"] in BUILD_TERMINAL:
            return dict(row), f"构建已处于终态 {row['status']}，取消请求忽略"
        await self.db.request_cancel(build_id)
        event = self._build_events.get(build_id)
        if event is not None:
            event.set()
        # 立即唤醒：调度器需剔除该构建的排队步骤，名额让给其他构建
        self._wakeup()
        return dict(row), "取消请求已受理，运行中的步骤将终止整个进程组"

    # ------------------------------------------------------------- 重启恢复

    async def recover(self) -> dict[str, int]:
        """API 启动时执行：先清理旧构建残留进程，再统一标记 interrupted。

        旧构建之外的进程（PID 已被复用、starttime 对不上）绝不动。
        恢复期间不恢复调度，未跑步骤一律不再启动。
        """
        stale = await self.db.list_nonterminal_builds()
        if not stale:
            return {"builds": 0, "killed_groups": 0}

        rows = await self.db.list_steps_for_builds(tuple(b["id"] for b in stale))
        grouped: dict[int, list[Any]] = {}
        for row in rows:
            grouped.setdefault(row["build_id"], []).append(row)

        killed_groups = 0
        for build in stale:
            build_id = build["id"]
            for step in grouped.get(build_id, []):
                if step["status"] not in ("running", "cleaning_up"):
                    continue
                pgid = step["pgid"]
                pid = step["pid"]
                starttime = step["proc_starttime"]
                if not pgid:
                    continue
                note: Optional[str] = None
                identity_ok = bool(
                    pid
                    and proc.is_same_process(pid, starttime)
                    and _safe_getpgid(pid) == pgid
                )
                members = await asyncio.to_thread(proc.list_pgid_members, pgid)
                if identity_ok:
                    note = "API 重启恢复：进程身份确认，终止旧进程组"
                elif members:
                    # 组长已死但组内仍有成员：组未空，pgid 不可能被复用，成员即残留
                    note = "API 重启恢复：组长已退出但组内仍有残留成员，强制清理"
                if note is None:
                    continue
                log_lines = [f"[runner] {note} build={build_id} step={step['step_uid']}"]
                cleared = await proc.terminate_process_group(
                    None, pgid, "重启清理", log_lines
                )
                killed_groups += 1
                self._append_log(step["log_path"], log_lines)
                if not cleared:
                    self._append_log(
                        step["log_path"], ["[runner] 警告: 重启清理超过硬等上限仍有残留"]
                    )

            for step in grouped.get(build_id, []):
                if step["status"] not in ("succeeded", "failed", "skipped", "cancelled"):
                    await self.db.set_step_status(
                        step["id"],
                        "interrupted",
                        _now(),
                        detail="API 进程重启，步骤被标记中断，不再启动",
                    )
                    self._append_log(
                        step["log_path"], ["[runner] API 重启：该步骤标记为 interrupted"]
                    )
            await self.db.set_build_status(
                build_id,
                "interrupted",
                _now(),
                detail="API 进程重启，旧构建中断，未启动步骤不再执行",
                started=build["started_at"] is None,
                finished=True,
            )
        return {"builds": len(stale), "killed_groups": killed_groups}

    # ------------------------------------------------------------- 中央调度

    def _wakeup(self) -> None:
        self._wake_event.set()

    def _enqueue(self, build_id: int, uid: str, step_pk: int, priority: str, row: Any) -> None:
        """依赖已成功的待运行步骤进入全局待选池（幂等）。"""
        key = (build_id, uid)
        if key in self._ready:
            return
        self._seq_counter += 1
        self._ready[key] = _ReadyEntry(
            build_id=build_id,
            uid=uid,
            step_pk=step_pk,
            priority=priority,
            seq=self._seq_counter,
            row=row,
        )
        self._wakeup()

    def _remove_build_entries(self, build_id: int) -> None:
        """构建结束/取消时，剔除其仍在排队、尚未启动的步骤。"""
        for key in [k for k in self._ready if k[0] == build_id]:
            self._ready.pop(key, None)

    def _is_cancelled(self, build_id: int) -> bool:
        event = self._build_events.get(build_id)
        return event is not None and event.is_set()

    def _choose(self) -> Optional[_ReadyEntry]:
        """策略 + 公平性记账：选出下一个获得名额的步骤并推进轮转游标。

        规则见模块 docstring。候选不足时返回 None（不改动账本）。
        注意：本方法有副作用——选中即更新该优先级层的轮转游标与紧急连计，
        因此同一唤醒周期内连续选取多个名额时，后续选取会看到更新后的账本；
        调用方负责把选中条目从待选池移除并真正分配名额。
        """
        candidates = [
            e for e in self._ready.values() if not self._is_cancelled(e.build_id)
        ]
        if not candidates:
            return None

        normal_ready = [e for e in candidates if e.priority == "normal"]
        urgent_ready = [e for e in candidates if e.priority == "urgent"]

        # 连续两次紧急后，普通就绪则下一次强制普通（防普通饥饿）
        force_normal = self._urgent_streak >= 2 and bool(normal_ready)
        tier = "normal" if force_normal else ("urgent" if urgent_ready else "normal")
        pool = normal_ready if tier == "normal" else urgent_ready

        # 同优先级跨构建轮流：在该层轮转游标之后取下一个"有活"的构建
        # （构建 id 即提交顺序），绕回头部。参与轮转的是仍有未完成步骤的
        # 活跃构建集合，而不仅是当前已入池条目，避免先入池的构建在其他构建
        # 入池前连拿多个名额；最终仍必须落到本层确有就绪步骤的构建。
        cursor = self._last_build_by_priority[tier]
        active_sorted = sorted(self._active_builds)
        ahead = [bid for bid in active_sorted if cursor is None or bid > cursor]
        rotation = ahead + [bid for bid in active_sorted if not (cursor is None or bid > cursor)]
        ready_builds = {e.build_id for e in pool}
        for chosen_build in rotation:
            if chosen_build in ready_builds:
                break
        else:  # 理论上不会发生：pool 非空则其构建必在活跃集合内
            return None
        same_build = [e for e in pool if e.build_id == chosen_build]
        entry = min(same_build, key=lambda e: e.seq)
        self._last_build_by_priority[tier] = chosen_build
        if tier == "urgent":
            self._urgent_streak += 1
        else:
            self._urgent_streak = 0
        return entry

    async def _dispatch_loop(self) -> None:
        """名额释放/新步骤入池/取消时唤醒；每轮把空闲名额逐个分出去。"""
        while not self._stopping:
            await self._wake_event.wait()
            self._wake_event.clear()
            if self._stopping:
                return
            # 一个唤醒周期内可能释放多个名额：循环选取直到名额用完或无候选
            while self._free_slots > 0:
                entry = self._choose()
                if entry is None:
                    break
                # 二次确认：选取与启动之间构建可能刚被取消。
                # 显式跳过并 continue（条目已被 _choose 记账但不会占用名额），
                # 保证循环必然向"无候选"收敛而不会空转。
                if self._is_cancelled(entry.build_id):
                    self._ready.pop((entry.build_id, entry.uid), None)
                    continue
                self._ready.pop((entry.build_id, entry.uid), None)
                self._free_slots -= 1
                task = asyncio.create_task(
                    self._run_step_guarded(entry),
                    name=f"build-{entry.build_id}-step-{entry.uid}",
                )
                self._inflight[task] = entry
                task.add_done_callback(self._on_step_done)

    def _on_step_done(self, task: asyncio.Task[str]) -> None:
        """步骤（含进程组清理与终态落库）结束：名额真正释放，触发下一轮选取。"""
        entry = self._inflight.pop(task, None)
        if entry is not None:
            self._free_slots += 1
        # 任务异常不应吞掉；_run_step_guarded 已兜底，这里仅留痕
        if not task.cancelled() and task.exception() is not None:
            self._log_task_exception(task)
        self._wakeup()

    async def _run_step_guarded(self, entry: _ReadyEntry) -> str:
        """步骤任务外壳：启动瞬间再查一次取消，杜绝取消后仍拉起新进程。"""
        if self._is_cancelled(entry.build_id):
            await self.db.set_step_status(
                entry.step_pk,
                "cancelled",
                _now(),
                detail="构建已取消，步骤未启动",
            )
            return "cancelled"
        try:
            return await self._run_step(entry.build_id, entry.row)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # 兜底：任何意外都落成 failed，不能让名额泄漏
            self._append_log(
                entry.row["log_path"], [f"[runner] 步骤任务异常: {exc!r}"]
            )
            await self.db.set_step_status(
                entry.step_pk, "failed", _now(), detail=f"步骤任务异常: {exc}"
            )
            return "failed"

    # ------------------------------------------------------------- 构建主循环

    async def _run_build(self, build_id: int) -> None:
        event = asyncio.Event()
        self._build_events[build_id] = event
        # 本构建已进入待选池的步骤，保证幂等入池
        enqueued: set[str] = set()
        try:
            build_row = await self.db.get_build(build_id)
            priority = build_row["priority"] if build_row else "normal"
            await self.db.set_build_status(
                build_id, "running", _now(), started=True
            )
            statuses: dict[str, str] = {}

            while True:
                # 每轮以落库状态为准对账（步骤由中央调度器托管执行）
                rows = await self.db.get_steps(build_id)
                by_id = {row["step_uid"]: row for row in rows}
                statuses = {row["step_uid"]: row["status"] for row in rows}

                # 取消优先：未启动步骤不再启动，运行中的交给步骤任务自行终止
                if event.is_set():
                    self._remove_build_entries(build_id)
                    for uid, st in statuses.items():
                        if st == "pending":
                            statuses[uid] = "cancelled"
                            await self.db.set_step_status(
                                by_id[uid]["id"],
                                "cancelled",
                                _now(),
                                detail="构建已取消，步骤未启动",
                            )
                    break

                progressed = False
                for uid, row in by_id.items():
                    if statuses[uid] != "pending" or uid in enqueued:
                        continue
                    deps = json.loads(row["depends_on"])
                    dep_st = [statuses[d] for d in deps]
                    if any(s in _STEP_FAIL_STATES for s in dep_st):
                        statuses[uid] = "skipped"
                        await self.db.set_step_status(
                            row["id"],
                            "skipped",
                            _now(),
                            detail=f"上游步骤未成功，后代跳过: {deps}",
                        )
                        progressed = True
                        continue
                    # 依赖未全部结束则等下一轮；全部 succeeded 才入待选池
                    if any(s != "succeeded" for s in dep_st):
                        continue
                    self._enqueue(
                        build_id, uid, row["id"], priority, row
                    )
                    enqueued.add(uid)
                    progressed = True

                if all(
                    st in STEP_TERMINAL for st in statuses.values()
                ):
                    break
                # 没有新推进时睡眠等待；有推进也短暂让出，避免忙等
                await asyncio.sleep(config.SCHEDULER_TICK if not progressed else 0)

            # 取消路径下可能仍有在途步骤（dispatcher 托管，含清理）：
            # 等它们全部落终态后再确认构建终态
            while True:
                final_rows = await self.db.get_steps(build_id)
                if all(r["status"] in STEP_TERMINAL for r in final_rows):
                    break
                await asyncio.sleep(config.SCHEDULER_TICK)

            final_statuses = [r["status"] for r in final_rows]
            if "failed" in final_statuses or "interrupted" in final_statuses:
                final = "failed"
                detail = "存在失败步骤"
            elif "cancelled" in final_statuses:
                final = "cancelled"
                detail = "构建被取消"
            else:
                final = "succeeded"
                detail = "全部步骤成功"
            await self.db.set_build_status(
                build_id, final, _now(), detail=detail, finished=True
            )
        finally:
            self._remove_build_entries(build_id)
            self._active_builds.discard(build_id)
            self._build_events.pop(build_id, None)
            self._build_tasks.pop(build_id, None)

    # ------------------------------------------------------------- 单步执行

    async def _run_step(self, build_id: int, row: Any) -> str:
        step_pk = row["id"]
        step_uid = row["step_uid"]
        command = row["command"]
        timeout = float(row["timeout_seconds"])
        workdir = config.WORKDIR_ROOT / str(build_id)
        workdir.mkdir(parents=True, exist_ok=True)
        log_path = Path(row["log_path"])
        log_lines = [
            f"[runner] build={build_id} step={step_uid} 开始",
            f"[runner] workdir={workdir}",
            f"[runner] command={command}",
            f"[runner] timeout={timeout}s",
        ]
        self._append_log(log_path, log_lines)

        env = {
            **os.environ,
            "PIPELINE_BUILD_ID": str(build_id),
            "PIPELINE_STEP_ID": step_uid,
        }
        log_f = open(log_path, "ab", buffering=0)
        try:
            try:
                process, pid, pgid, starttime = await proc.spawn(
                    command, workdir, env, log_f, log_f
                )
            except Exception as exc:  # 命令本身都无法启动
                await self.db.set_step_status(
                    step_pk, "failed", _now(), detail=f"进程启动失败: {exc}"
                )
                self._append_log(log_path, [f"[runner] 进程启动失败: {exc}"])
                return "failed"

            await self.db.set_step_process(step_pk, pid, pgid, starttime, _now())
            cancel_event = self._build_events[build_id]
            outcome, detail = await self._watch(process, timeout, cancel_event)

            # 进入清理阶段即对外可见：终态尚未确认、名额仍占用
            await self.db.set_step_status(
                step_pk, "cleaning_up", _now(), detail=detail
            )
            leftovers_before = await asyncio.to_thread(
                proc.list_pgid_members, pgid
            )
            had_leftovers = bool(leftovers_before)
            cleanup_log: list[str] = []
            if outcome == "timeout":
                reason = "步骤超时"
            elif outcome == "cancelled":
                reason = "构建取消"
            else:
                reason = "主命令退出后检查残留子进程"
            cleared = await proc.terminate_process_group(
                process, pgid, reason, cleanup_log
            )
            self._append_log(log_path, cleanup_log)
            exit_code = process.returncode

            if not cleared:
                status, final_detail = (
                    "failed",
                    f"{detail or ''}; 进程组在硬等上限内未清空".strip("; "),
                )
            elif outcome == "timeout":
                status, final_detail = "failed", detail
            elif outcome == "cancelled":
                status, final_detail = "cancelled", detail
            elif had_leftovers:
                status = "failed"
                final_detail = (
                    f"主命令已退出(exit={exit_code})，但检测到遗留子进程，"
                    "进程组清理后按失败处理"
                )
            elif exit_code == 0:
                status, final_detail = "succeeded", None
            else:
                status, final_detail = "failed", f"命令以非零状态退出: {exit_code}"

            await self.db.set_step_status(
                step_pk,
                status,
                _now(),
                exit_code=exit_code if isinstance(exit_code, int) else None,
                detail=final_detail,
            )
            self._append_log(
                log_path,
                [
                    f"[runner] 步骤终态={status} exit={exit_code} "
                    f"遗留子进程={had_leftovers} detail={final_detail or '-'}"
                ],
            )
            return status
        finally:
            log_f.close()

    async def _watch(
        self, process: Any, timeout: float, cancel_event: asyncio.Event
    ) -> tuple[str, Optional[str]]:
        """三者竞速：进程退出 / 取消 / 超时。取消与超时只负责发信号，
        实际的进程组终止统一由调用方在清理阶段完成。"""
        wait_task = asyncio.create_task(process.wait())
        cancel_task = asyncio.create_task(cancel_event.wait())
        timeout_task = asyncio.create_task(asyncio.sleep(timeout))
        try:
            await asyncio.wait(
                {wait_task, cancel_task, timeout_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if wait_task.done():
                return "exited", None
            if cancel_task.done():
                return "cancelled", "构建已取消，终止整个进程组"
            return "timeout", f"步骤超过 {timeout}s 超时，终止整个进程组"
        finally:
            for task in (cancel_task, timeout_task):
                task.cancel()
            await asyncio.gather(cancel_task, timeout_task, return_exceptions=True)

    # ------------------------------------------------------------- 工具方法

    @staticmethod
    def _append_log(log_path: str, lines: list[str]) -> None:
        try:
            with open(log_path, "a", encoding="utf-8") as fh:
                fh.write("\n".join(lines) + "\n")
        except OSError:
            pass

    @staticmethod
    def _log_task_exception(task: asyncio.Task[Any]) -> None:
        if task.cancelled() or task.exception() is None:
            return
        # 构建任务内部已兜住大部分异常；这里仅留痕，避免静默吞掉
        import logging

        logging.getLogger("pipeline").exception(
            "构建任务异常: %s", task.exception()
        )


def _safe_getpgid(pid: int) -> Optional[int]:
    try:
        return os.getpgid(pid)
    except ProcessLookupError:
        return None
