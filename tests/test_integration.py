"""端到端集成测试：以真实 uvicorn 子进程启动 API，覆盖全部关键链路。

运行:
  .venv/bin/pytest tests/test_integration.py -v
"""
from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
TERMINAL = {"succeeded", "failed", "cancelled", "interrupted"}


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ServerHarness:
    __test__ = False  # 非测试类，避免 pytest 误收集

    def __init__(self, data_dir: Path, port: int):
        self.data_dir = data_dir
        self.port = port
        self.proc: subprocess.Popen | None = None
        self.log_file: Path | None = None
        self.client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=30)

    def start(self) -> None:
        env = {
            **os.environ,
            "PIPELINE_DATA_DIR": str(self.data_dir),
            "PIPELINE_TERM_GRACE_SECONDS": "0.5",
            "PIPELINE_KILL_GRACE_SECONDS": "10",
            "PIPELINE_PROC_POLL_INTERVAL": "0.05",
            "PIPELINE_SCHEDULER_TICK": "0.05",
        }
        # 输出落文件而不是 PIPE：管道缓冲写满会冻结 uvicorn 事件循环
        self.log_file = self.data_dir / "server.log"
        log_fh = open(self.log_file, "ab")
        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "app.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--workers",
                "1",
            ],
            cwd=ROOT,
            env=env,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                r = httpx.get(f"http://127.0.0.1:{self.port}/health", timeout=1)
                if r.status_code == 200:
                    return
            except httpx.TransportError:
                pass
            if self.proc.poll() is not None:
                out = self.log_file.read_text() if self.log_file else ""
                raise RuntimeError(f"服务器提前退出:\n{out}")
            time.sleep(0.1)
        raise RuntimeError(f"服务器启动超时，日志:\n{self.log_file.read_text()}")

    def stop(self, sig: int = signal.SIGTERM) -> None:
        if self.proc is None:
            return
        try:
            self.proc.send_signal(sig)
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        finally:
            self.client.close()
            self.proc = None


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    srv = ServerHarness(tmp_path_factory.mktemp("data"), _free_port())
    srv.start()
    yield srv
    srv.stop()


def step(uid, command, deps=None, timeout=30.0):
    return {"id": uid, "command": command, "depends_on": deps or [],
            "timeout_seconds": timeout}


def submit_pri(client: httpx.Client, name: str, steps: list[dict], priority: str) -> int:
    r = client.post("/builds", json={"name": name, "priority": priority, "steps": steps})
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["priority"] == priority
    return body["id"]


def submit(client: httpx.Client, name: str, steps: list[dict]) -> int:
    r = client.post("/builds", json={"name": name, "steps": steps})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def wait_build(client: httpx.Client, build_id: int, timeout: float = 40) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        detail = client.get(f"/builds/{build_id}").json()
        if detail["status"] in TERMINAL:
            return detail
        time.sleep(0.1)
    raise AssertionError(f"构建 {build_id} 未在 {timeout}s 内结束")


def step_map(detail: dict) -> dict:
    return {s["id"]: s for s in detail["steps"]}


def assert_pid_dead(pid: int) -> None:
    for _ in range(30):
        if not Path(f"/proc/{pid}").exists():
            return
        time.sleep(0.1)
    raise AssertionError(f"进程 {pid} 在清理后仍然存活")


def command_output(log_payload: dict) -> str:
    """剔除 [runner] 元数据行，只保留命令自身的 stdout/stderr。"""
    return "\n".join(
        line for line in log_payload["content"].splitlines()
        if not line.startswith("[runner]")
    )


# --------------------------------------------------------------- 1. 成功构建

def test_success_build_runs_all_steps(server):
    bid = submit(server.client, "success", [
        step("prepare", "mkdir -p out && echo v1 > out/fw.txt"),
        step("compile", "test -f out/fw.txt && echo compiled", ["prepare"]),
        step("package", "echo packed", ["compile"]),
    ])
    detail = wait_build(server.client, bid)
    assert detail["status"] == "succeeded"
    assert all(s["status"] == "succeeded" for s in detail["steps"])
    artifact = server.data_dir / "workdir" / str(bid) / "out" / "fw.txt"
    assert artifact.read_text().strip() == "v1"
    log = server.client.get(f"/builds/{bid}/steps/compile/logs").json()
    assert "compiled" in log["content"]
    assert log["status"] == "succeeded"


# ----------------------------------------------------- 2. 失败跳过 + 独立分支

def test_failure_skips_descendants_but_independent_branch_continues(server):
    bid = submit(server.client, "failure", [
        step("good", "echo ok"),
        step("bad", "echo boom; exit 7"),
        step("child", "echo should-not-run", ["bad"]),
        step("grand", "echo should-not-run", ["child"]),
    ])
    detail = wait_build(server.client, bid)
    assert detail["status"] == "failed"
    sm = step_map(detail)
    assert sm["good"]["status"] == "succeeded"
    assert sm["bad"]["status"] == "failed" and sm["bad"]["exit_code"] == 7
    assert sm["child"]["status"] == "skipped"
    assert sm["grand"]["status"] == "skipped"
    log = server.client.get(f"/builds/{bid}/steps/child/logs").json()
    assert "should-not-run" not in command_output(log)  # 跳过的步骤确实没执行


# ------------------------------------------------- 3. 慢子进程：主退出后判失败

def test_lingering_child_is_killed_and_build_fails(server):
    bid = submit(server.client, "slow-child", [
        step(
            "spawn",
            # 不用子 shell 包装，$! 才能拿到真实后台子进程 PID
            "sleep 300 & echo $! > slow.pid && echo main-done && exit 0",
            timeout=10,
        ),
    ])
    detail = wait_build(server.client, bid)
    assert detail["status"] == "failed"
    sm = step_map(detail)
    assert sm["spawn"]["status"] == "failed"
    assert "遗留子进程" in (sm["spawn"]["detail"] or "")
    workdir = server.data_dir / "workdir" / str(bid)
    child_pid = int((workdir / "slow.pid").read_text().strip())
    assert_pid_dead(child_pid)  # 进程组清空前不确认终态：此时必须已死
    log = server.client.get(f"/builds/{bid}/steps/spawn/logs").json()
    assert "进程组" in log["content"] and "清空" in log["content"]


# ------------------------------------------------------------------- 4. 超时

def test_timeout_kills_group_and_marks_failed(server):
    bid = submit(server.client, "timeout", [
        step("slow", "echo start; sleep 30; echo end", timeout=1),
    ])
    detail = wait_build(server.client, bid, timeout=20)
    assert detail["status"] == "failed"
    assert step_map(detail)["slow"]["status"] == "failed"
    assert "超时" in (step_map(detail)["slow"]["detail"] or "")
    log = server.client.get(f"/builds/{bid}/steps/slow/logs").json()
    out = command_output(log)
    assert "start" in out
    assert "end" not in out  # 超时点在 sleep，后续输出不会产生


# ------------------------------------------------------ 5. 全局最多两步并行

def test_global_parallelism_capped_at_two(server):
    # 四个各 1.2s 的独立步骤，2 名额 -> 至少两批，总耗时应 >= 2.0s 且 < 4.5s
    steps = [step(f"s{i}", "echo running-$PIPELINE_STEP_ID; sleep 1.2") for i in range(4)]
    t0 = time.time()
    bid = submit(server.client, "parallel-cap", steps)
    detail = wait_build(server.client, bid, timeout=30)
    elapsed = time.time() - t0
    assert detail["status"] == "succeeded"
    assert elapsed >= 2.0, f"并行度疑似超过 2，总耗时仅 {elapsed:.2f}s"
    assert elapsed < 4.5


# ------------------------------------------------------------- 6. 整次取消

def test_cancel_stops_all_running_steps_and_skips_pending(server):
    bid = submit(server.client, "cancel", [
        step("a", "echo a-start; sleep 20; echo a-end"),
        step("b", "echo b-start; sleep 20; echo b-end"),
        step("c", "echo c-should-not-run; sleep 20", ["a"]),
    ])
    # 等两个名额占满
    deadline = time.time() + 10
    running = []
    while time.time() < deadline:
        sm = step_map(server.client.get(f"/builds/{bid}").json())
        running = [k for k, v in sm.items() if v["status"] == "running"]
        if len(running) == 2:
            break
        time.sleep(0.1)
    assert sorted(running) == ["a", "b"]

    r = server.client.post(f"/builds/{bid}/cancel")
    assert r.status_code == 200
    detail = wait_build(server.client, bid, timeout=20)
    sm = step_map(detail)
    assert detail["status"] == "cancelled"
    assert sm["a"]["status"] == "cancelled" and sm["b"]["status"] == "cancelled"
    assert sm["c"]["status"] == "cancelled"
    for uid in ("a", "b"):
        log = server.client.get(f"/builds/{bid}/steps/{uid}/logs").json()
        out = command_output(log)
        assert f"{uid}-start" in out
        assert f"{uid}-end" not in out  # 取消点在 sleep，尾部输出不会产生


def test_repeated_cancel_is_idempotent(server):
    bid = submit(server.client, "cancel-idem", [step("a", "sleep 20")])
    time.sleep(0.5)
    first = server.client.post(f"/builds/{bid}/cancel")
    assert first.status_code == 200
    detail1 = wait_build(server.client, bid, timeout=20)
    second = server.client.post(f"/builds/{bid}/cancel")
    assert second.status_code == 200
    assert "终态" in second.json()["detail"]
    third = server.client.get(f"/builds/{bid}").json()
    assert third["status"] == detail1["status"] == "cancelled"
    assert third["finished_at"] == detail1["finished_at"]  # 终态未被改写


# --------------------------------------- 7. 慢清理不阻塞另一名额（跨构建）

def test_slow_cleanup_does_not_block_other_slot(server):
    # 顽固子进程忽略 SIGTERM（忽略型信号处置会被子进程继承），
    # 主命令退出后清理必须升级到 SIGKILL 流程
    stubborn = (
        "trap '' TERM; sleep 300 & echo $! > stubborn.pid; "
        "echo main-exit; exit 0"
    )
    t0 = time.time()
    bid_busy = submit(server.client, "slow-cleanup", [step("x", stubborn, timeout=10)])
    time.sleep(0.6)  # 确保 busy 已占住一个名额
    bid_fast = submit(server.client, "other-slot", [step("y", "echo fast-ok")])
    detail = wait_build(server.client, bid_fast, timeout=15)
    fast_elapsed = time.time() - t0
    assert detail["status"] == "succeeded"
    # 另一名额可用：快构建在 5s 内完成（被慢清理拖住则要等 KILL 流程更久）
    assert fast_elapsed < 5, f"另一名额疑似被慢清理阻塞: {fast_elapsed:.2f}s"
    busy = wait_build(server.client, bid_busy, timeout=30)
    assert busy["status"] == "failed"  # 顽固子进程最终被 SIGKILL，仍判失败
    workdir = server.data_dir / "workdir" / str(bid_busy)
    pid = int((workdir / "stubborn.pid").read_text().strip())
    assert_pid_dead(pid)


# --------------------------------------------------------- 8. 重启恢复全链路

def test_restart_recovers_and_does_not_kill_new_build(tmp_path_factory):
    data_dir = tmp_path_factory.mktemp("restart-data")
    port = _free_port()
    srv = ServerHarness(data_dir, port)
    srv.start()
    try:
        bid = submit(srv.client, "will-be-interrupted", [
            step("a", "sleep 30"),
            step("b", "sleep 30"),
            step("c", "echo never", ["a"]),
        ])
        deadline = time.time() + 10
        while time.time() < deadline:
            sm = step_map(srv.client.get(f"/builds/{bid}").json())
            if sm["a"]["status"] == "running":
                running_pid = _step_pid(data_dir, bid, "a")
                break
            time.sleep(0.1)
        else:
            raise AssertionError("步骤未进入 running")
        assert Path(f"/proc/{running_pid}").exists()
        srv.stop()
        # 进程组随 API 被 tini 之外的场景保留（同会话 SIGTERM 只停 uvicorn）
        assert Path(f"/proc/{running_pid}").exists()
    finally:
        pass

    # 同数据目录重启：先清理残留，再标记 interrupted，然后才对外服务
    srv2 = ServerHarness(data_dir, port)
    srv2.start()
    try:
        assert_pid_dead(running_pid)  # 旧构建残留必须被清掉
        detail = srv2.client.get(f"/builds/{bid}").json()
        assert detail["status"] == "interrupted"
        sm = step_map(detail)
        assert sm["a"]["status"] == "interrupted"
        assert sm["b"]["status"] == "interrupted"
        assert sm["c"]["status"] == "interrupted"  # 未跑步骤不再启动

        # 后来的构建必须正常工作，不得被恢复逻辑误杀
        new_id = submit(srv2.client, "after-restart", [
            step("z", "echo alive && test $PIPELINE_BUILD_ID -ge 1"),
        ])
        new_detail = wait_build(srv2.client, new_id)
        assert new_detail["status"] == "succeeded"
    finally:
        srv2.stop()


def _step_pid(data_dir: Path, build_id: int, step_uid: str) -> int:
    import sqlite3

    conn = sqlite3.connect(data_dir / "pipeline.db")
    try:
        row = conn.execute(
            "SELECT pid FROM steps WHERE build_id=? AND step_uid=?",
            (build_id, step_uid),
        ).fetchone()
        return int(row[0])
    finally:
        conn.close()


# --------------------------------- 8b. 跨构建调度：紧急优先与轮流（端到端）

def _wait_step_running(client, build_id: int, uid: str, timeout: float = 10) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        sm = step_map(client.get(f"/builds/{build_id}").json())
        if sm[uid]["status"] in ("running", "cleaning_up"):
            return
        time.sleep(0.05)
    raise AssertionError(f"build={build_id} step={uid} 未在 {timeout}s 内启动")


def _started_at(client, build_id: int, uid: str) -> str:
    sm = step_map(client.get(f"/builds/{build_id}").json())
    assert sm[uid]["started_at"], f"step {uid} 从未启动"
    return sm[uid]["started_at"]


def test_urgent_patch_jumps_ahead_of_queued_normal_builds(server):
    # 普通构建的两个慢步骤先占满两个名额；紧急补丁与另一个普通构建随后排队，
    # 名额释放后紧急补丁必须插队先启动
    normal = submit_pri(server.client, "normal-big", [
        step("n1", "sleep 2.5"),
        step("n2", "sleep 2.5"),
    ], "normal")
    _wait_step_running(server.client, normal, "n1")
    _wait_step_running(server.client, normal, "n2")

    urgent = submit_pri(server.client, "urgent-patch", [
        step("hot", "echo hotfix"),
    ], "urgent")
    queued_normal = submit_pri(server.client, "normal-queued", [
        step("q", "echo queued"),
    ], "normal")
    # 两者在名额释放前都保持排队
    time.sleep(0.5)
    assert step_map(server.client.get(f"/builds/{urgent}").json())["hot"]["status"] == "pending"
    assert step_map(server.client.get(f"/builds/{queued_normal}").json())["q"]["status"] == "pending"

    wait_build(server.client, urgent, timeout=20)
    wait_build(server.client, queued_normal, timeout=20)
    wait_build(server.client, normal, timeout=20)
    # 名额一释放：紧急步骤先于排队普通步骤启动
    assert _started_at(server.client, urgent, "hot") < _started_at(
        server.client, queued_normal, "q"
    )
    assert step_map(server.client.get(f"/builds/{urgent}").json())["hot"]["status"] == "succeeded"


def test_consecutive_two_urgent_then_normal_gets_next_slot(server):
    # 两个名额先被两个普通慢步骤占住；随后提交 3 个紧急构建 + 1 个普通构建
    holders = submit_pri(server.client, "holders", [
        step("h1", "sleep 2.5"),
        step("h2", "sleep 2.5"),
    ], "normal")
    _wait_step_running(server.client, holders, "h1")
    _wait_step_running(server.client, holders, "h2")

    u1 = submit_pri(server.client, "u1", [step("s", "sleep 0.6")], "urgent")
    u2 = submit_pri(server.client, "u2", [step("s", "sleep 0.6")], "urgent")
    u3 = submit_pri(server.client, "u3", [step("s", "sleep 0.6")], "urgent")
    nq = submit_pri(server.client, "normal-queued", [step("s", "sleep 0.4")], "normal")

    for bid in (holders, u1, u2, u3, nq):
        wait_build(server.client, bid, timeout=30)

    t_u1 = _started_at(server.client, u1, "s")
    t_u2 = _started_at(server.client, u2, "s")
    t_nq = _started_at(server.client, nq, "s")
    t_u3 = _started_at(server.client, u3, "s")
    # 前两次释放给紧急（u1、u2），第三次必须让给普通 nq，u3 排在普通之后
    assert t_u1 < t_u2 < t_nq < t_u3


def test_same_priority_builds_take_turns_no_single_build_fills_queue(server):
    # 跨构建轮流的确定性验证：
    # 构建 X、Y 的两个闸门步骤先占住两个名额；此时构建 A、B 各有两个就绪步骤
    # 在全局待选池排队（共 4 个）。释放 X、Y 后，两个名额必须分给 A、B 各一个，
    # 而不是被任一构建连拿两个 —— 证明同优先级按构建轮流、单构建无法占满队列。
    gate = server.data_dir / "rr-gate"
    if gate.exists():
        gate.unlink()

    def hold() -> str:
        return (
            f"for i in $(seq 1 400); do test -f {gate} && exit 0; "
            "sleep 0.05; done; exit 1"
        )

    bX = submit_pri(server.client, "rr-holder-X", [step("x", hold())], "normal")
    bY = submit_pri(server.client, "rr-holder-Y", [step("y", hold())], "normal")
    _wait_step_running(server.client, bX, "x")
    _wait_step_running(server.client, bY, "y")

    # 名额已满，A、B 的步骤全部排队
    bA = submit_pri(server.client, "rr-A", [
        step("a1", "sleep 0.3"), step("a2", "sleep 0.3"),
    ], "normal")
    bB = submit_pri(server.client, "rr-B", [
        step("b1", "sleep 0.3"), step("b2", "sleep 0.3"),
    ], "normal")
    time.sleep(0.4)
    for bid in (bA, bB):
        sm = step_map(server.client.get(f"/builds/{bid}").json())
        assert all(st["status"] == "pending" for st in sm.values())

    gate.write_text("go")  # 同时释放两个名额，调度器在 A、B 间轮转
    # 两个名额应分别落到 A、B（不同构建），而不是同一个构建连拿
    deadline = time.time() + 10
    first_wave: dict[int, str] = {}
    while time.time() < deadline:
        for bid in (bA, bB):
            sm = step_map(server.client.get(f"/builds/{bid}").json())
            for uid, st in sm.items():
                if st["status"] in ("running", "cleaning_up", "succeeded"):
                    first_wave.setdefault(bid, uid)
        if len(first_wave) == 2:
            break
        time.sleep(0.05)
    assert set(first_wave) == {bA, bB}  # 第一波两个名额分属 A、B 两个构建

    for bid in (bX, bY, bA, bB):
        detail = wait_build(server.client, bid, timeout=30)
        assert detail["status"] == "succeeded"
    # 每个构建的两个步骤都真实执行且各自只执行一次
    for bid in (bA, bB):
        sm = step_map(server.client.get(f"/builds/{bid}").json())
        assert sorted(sm) and all(st["status"] == "succeeded" for st in sm.values())


def test_cancelled_build_queued_steps_never_start(server):
    # 两个慢步骤占满名额，第三个步骤排队；取消构建后排队步骤不得启动
    bid = submit_pri(server.client, "cancel-queued", [
        step("a", "sleep 2"),
        step("b", "sleep 2"),
        step("c", "echo never-run", ["a"]),
    ], "normal")
    _wait_step_running(server.client, bid, "a")
    _wait_step_running(server.client, bid, "b")

    r = server.client.post(f"/builds/{bid}/cancel")
    assert r.status_code == 200
    detail = wait_build(server.client, bid, timeout=20)
    sm = step_map(detail)
    assert detail["status"] == "cancelled"
    assert sm["c"]["status"] == "cancelled"
    log = server.client.get(f"/builds/{bid}/steps/c/logs").json()
    assert "never-run" not in command_output(log)
    # 取消后再提交普通构建，两个名额应能正常释放并服务新构建（不泄漏名额）
    after = submit_pri(server.client, "after-cancel", [
        step("z", "echo slot-released"),
    ], "normal")
    after_detail = wait_build(server.client, after, timeout=15)
    assert after_detail["status"] == "succeeded"


# --------------------------------------- 9. 其他接口

def test_404_and_bad_payloads(server):
    assert server.client.get("/builds/999999").status_code == 404
    assert server.client.post("/builds/999999/cancel").status_code == 404

    bad_cycle = {"name": "cycle", "steps": [
        step("a", "true", ["b"]), step("b", "true", ["a"])
    ]}
    r = server.client.post("/builds", json=bad_cycle)
    assert r.status_code == 400 and "循环" in r.json()["detail"]

    missing_dep = {"name": "ghost", "steps": [step("a", "true", ["x"])]}
    r = server.client.post("/builds", json=missing_dep)
    assert r.status_code == 400 and "不存在" in r.json()["detail"]

    empty = {"name": "empty", "steps": []}
    assert server.client.post("/builds", json=empty).status_code == 422

    bad_priority = {"name": "bad-pri", "priority": "critical",
                    "steps": [step("a", "true")]}
    r = server.client.post("/builds", json=bad_priority)
    assert r.status_code == 422

    # 合法优先级被接受且回显
    ok = server.client.post("/builds", json={
        "name": "pri-ok", "priority": "urgent", "steps": [step("a", "true")]
    })
    assert ok.status_code == 201 and ok.json()["priority"] == "urgent"


def test_health_and_docs(server):
    health = server.client.get("/health").json()
    assert health["status"] == "ok" and health["max_parallel"] == 2
    assert server.client.get("/docs").status_code == 200
    assert server.client.get("/openapi.json").status_code == 200
