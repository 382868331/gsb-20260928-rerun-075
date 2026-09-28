"""确定性虚拟时钟：演练全程不依赖真实睡眠，保证种子可重放。"""

from dataclasses import dataclass


@dataclass
class VirtualClock:
    now_ns: int = 0

    def now_ms(self) -> int:
        return self.now_ns // 1_000_000

    def advance_to(self, time_ns: int) -> None:
        if time_ns < self.now_ns:
            raise ValueError("虚拟时钟不能回退")
        self.now_ns = time_ns

    @staticmethod
    def ms_to_ns(ms: int | float) -> int:
        return int(ms * 1_000_000)
