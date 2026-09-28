"""确定性虚拟时钟。

平台不使用墙钟睡眠，而是在时间轴上推进，保证相同种子下可以逐毫秒重放，
测试也无需真实等待。
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Callable


@dataclass(order=True)
class _Timer:
    deadline: float
    seq: int
    callback: Callable[[], None] = field(compare=False)
    cancelled: bool = field(default=False, compare=False)


class VirtualClock:
    """单调推进的虚拟时钟，支持一次性定时器与睡眠。"""

    def __init__(self, start: float = 0.0) -> None:
        self._now = float(start)
        self._timers: list[_Timer] = []
        self._seq = 0

    @property
    def now(self) -> float:
        return self._now

    def schedule(self, delay: float, callback: Callable[[], None]) -> _Timer:
        if delay < 0:
            raise ValueError("定时器延迟不能为负")
        self._seq += 1
        timer = _Timer(self._now + delay, self._seq, callback)
        heapq.heappush(self._timers, timer)
        return timer

    def sleep(self, delay: float) -> None:
        """同步地推进时间（供服务链调用）。"""
        if delay < 0:
            raise ValueError("睡眠时长不能为负")
        self._now += float(delay)

    def advance(self, duration: float) -> None:
        """推进至多 ``duration``，依次触发到期定时器。"""
        if duration < 0:
            raise ValueError("推进时长不能为负")
        horizon = self._now + duration
        while self._timers and self._timers[0].deadline <= horizon:
            timer = heapq.heappop(self._timers)
            if timer.cancelled:
                continue
            self._now = timer.deadline
            timer.callback()
        self._now = horizon

    def run_until(self, deadline: float) -> None:
        if deadline < self._now:
            raise ValueError("不能回退时钟")
        self.advance(deadline - self._now)

    def drain(self) -> None:
        """运行所有尚未取消的定时器。"""
        while self._timers:
            timer = heapq.heappop(self._timers)
            if timer.cancelled:
                continue
            self._now = timer.deadline
            timer.callback()

    def pending(self) -> bool:
        return any(not t.cancelled for t in self._timers)
