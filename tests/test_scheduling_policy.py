"""跨构建调度策略的纯单元测试。

不启动真实进程，直接构造 Scheduler 的待选池，验证中央调度器 ``_choose`` 的
选取规则：紧急优先、连续两次紧急后的普通让位、同优先级跨构建轮流、
已取消构建的排队步骤不得启动、同构建内按入池顺序。
"""
from __future__ import annotations

import asyncio

import pytest

from app.models import STEP_TERMINAL
from app.scheduler import Scheduler, _ReadyEntry


def _entry(build_id: int, uid: str, priority: str, seq: int) -> _ReadyEntry:
    # row 只用于真实执行路径，策略测试用不到，传 None 即可
    return _ReadyEntry(
        build_id=build_id,
        uid=uid,
        step_pk=seq,
        priority=priority,
        seq=seq,
        row=None,
    )


@pytest.fixture
def sched() -> Scheduler:
    # Database 不会在策略测试中被触碰
    return Scheduler(db=None)  # type: ignore[arg-type]


def _put(sched: Scheduler, entry: _ReadyEntry) -> None:
    sched._ready[(entry.build_id, entry.uid)] = entry
    sched._active_builds.add(entry.build_id)


def _cancel(sched: Scheduler, build_id: int) -> None:
    sched._build_events[build_id] = asyncio.Event()
    sched._build_events[build_id].set()


def _grant(sched: Scheduler, entry: _ReadyEntry) -> None:
    """模拟名额真正分配：从池中移除（公平性账本已在 _choose 内更新）。"""
    sched._ready.pop((entry.build_id, entry.uid), None)


def test_empty_pool_returns_none(sched):
    assert sched._choose() is None


def test_urgent_jumps_ahead_of_normal(sched):
    _put(sched, _entry(1, "a", "normal", seq=1))
    _put(sched, _entry(2, "u", "urgent", seq=2))
    chosen = sched._choose()
    assert (chosen.build_id, chosen.uid) == (2, "u")


def test_two_consecutive_urgent_then_normal_must_be_served(sched):
    # 三个紧急步骤 + 一个普通步骤同时就绪
    u1 = _entry(2, "u1", "urgent", seq=2)
    u2 = _entry(2, "u2", "urgent", seq=3)
    u3 = _entry(2, "u3", "urgent", seq=4)
    n1 = _entry(1, "n1", "normal", seq=1)
    for e in (n1, u1, u2, u3):
        _put(sched, e)

    first = sched._choose()
    assert first.priority == "urgent"
    _grant(sched, first)
    second = sched._choose()
    assert second.priority == "urgent"
    _grant(sched, second)
    # 连续两次紧急后，普通就绪 -> 第三次必须给普通
    third = sched._choose()
    assert third is not None
    assert third.priority == "normal"
    assert (third.build_id, third.uid) == (1, "n1")
    _grant(sched, third)
    # 普通被服务后紧急计数清零，剩余紧急继续优先
    fourth = sched._choose()
    assert fourth is not None and fourth.priority == "urgent"


def test_urgent_streak_resets_when_no_normal_ready(sched):
    # 没有普通步骤时，紧急不受"连续两次"限制，可持续获得名额
    for i in range(4):
        _put(sched, _entry(2, f"u{i}", "urgent", seq=i + 1))
    granted = []
    for _ in range(4):
        e = sched._choose()
        assert e is not None and e.priority == "urgent"
        granted.append(e.uid)
        _grant(sched, e)
    assert granted == ["u0", "u1", "u2", "u3"]


def test_normal_grant_resets_urgent_streak(sched):
    # 两次紧急 -> 一次普通（强制让位）-> 紧急连计应清零，可再连续两次紧急
    for e in (
        _entry(1, "n", "normal", seq=1),
        _entry(2, "u1", "urgent", seq=2),
        _entry(2, "u2", "urgent", seq=3),
        _entry(2, "u3", "urgent", seq=4),
        _entry(2, "u4", "urgent", seq=5),
    ):
        _put(sched, e)
    seq_out = []
    for _ in range(5):
        e = sched._choose()
        assert e is not None
        seq_out.append(e.priority[0].upper())  # U / N
        _grant(sched, e)
    assert seq_out == ["U", "U", "N", "U", "U"]


def test_rotation_cursors_are_independent_per_priority(sched):
    # 普通层与紧急层各有独立轮转游标，互不干扰
    _put(sched, _entry(1, "n", "normal", seq=1))
    _put(sched, _entry(2, "u", "urgent", seq=2))
    n = sched._choose()  # 紧急先
    assert n.priority == "urgent" and n.build_id == 2
    _grant(sched, n)
    u = sched._choose()  # 只剩普通
    assert u.priority == "normal" and u.build_id == 1
    _grant(sched, u)
    assert sched._last_build_by_priority["urgent"] == 2
    assert sched._last_build_by_priority["normal"] == 1


def test_same_priority_round_robin_across_builds(sched):
    # 三个普通构建，每个两个就绪步骤（已全部入池）；同构建不能连续拿走名额
    for bid in (1, 2, 3):
        _put(sched, _entry(bid, "x", "normal", seq=bid * 10))
        _put(sched, _entry(bid, "y", "normal", seq=bid * 10 + 1))

    order = []
    for _ in range(6):
        e = sched._choose()
        assert e is not None
        order.append(e.build_id)
        _grant(sched, e)

    # 前三轮必须在三个构建间轮转，且每个构建在相邻两轮不会重复
    assert order[0] == 1  # 构建 id（提交顺序）最小者先
    assert len(set(order[:3])) == 3
    for prev, cur in zip(order, order[1:]):
        assert prev != cur
    assert sorted(order) == [1, 1, 2, 2, 3, 3]


def test_cancelled_build_steps_are_never_chosen(sched):
    _put(sched, _entry(1, "a", "normal", seq=1))
    _put(sched, _entry(2, "u", "urgent", seq=2))
    _cancel(sched, 2)  # 紧急构建被取消
    chosen = sched._choose()
    assert chosen is not None
    assert chosen.build_id == 1  # 取消构建的紧急步骤不得启动
    # 池中只剩被取消构建时，无可选步骤
    _grant(sched, chosen)
    assert sched._choose() is None


def test_same_build_steps_follow_enqueue_order(sched):
    _put(sched, _entry(1, "first", "normal", seq=1))
    _put(sched, _entry(1, "second", "normal", seq=2))
    first = sched._choose()
    assert first.uid == "first"
    _grant(sched, first)
    second = sched._choose()
    assert second is not None and second.uid == "second"


def test_terminal_set_unchanged():
    # 守卫：终态集合若被改动，构建收尾语义会变，这里固定下来
    assert STEP_TERMINAL == {
        "succeeded",
        "failed",
        "skipped",
        "cancelled",
        "interrupted",
    }
