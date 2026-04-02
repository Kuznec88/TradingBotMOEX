from __future__ import annotations

import math
import statistics
from typing import Iterable, Sequence


def avg(values: Iterable[float | int | None]) -> float | None:
    vv = [float(x) for x in values if x is not None]
    return (sum(vv) / len(vv)) if vv else None


def med(values: Iterable[float | int | None]) -> float | None:
    vv = [float(x) for x in values if x is not None]
    return float(statistics.median(vv)) if vv else None


def corr(xs: Sequence[float | int | None], ys: Sequence[float | int | None]) -> float | None:
    pts = [(float(x), float(y)) for x, y in zip(xs, ys) if x is not None and y is not None]
    if len(pts) < 2:
        return None
    xv = [p[0] for p in pts]
    yv = [p[1] for p in pts]
    mx = sum(xv) / len(xv)
    my = sum(yv) / len(yv)
    num = sum((a - mx) * (b - my) for a, b in pts)
    denx = math.sqrt(sum((a - mx) ** 2 for a in xv))
    deny = math.sqrt(sum((b - my) ** 2 for b in yv))
    if denx == 0 or deny == 0:
        return None
    return num / (denx * deny)


def quantile(sorted_values: Sequence[float], p: float) -> float | None:
    if not sorted_values:
        return None
    i = (len(sorted_values) - 1) * float(p)
    lo = math.floor(i)
    hi = math.ceil(i)
    if lo == hi:
        return float(sorted_values[int(i)])
    return float(sorted_values[lo]) * (hi - i) + float(sorted_values[hi]) * (i - lo)


def safe_div(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or b == 0:
        return None
    return float(a) / float(b)

