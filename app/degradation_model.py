"""
Degradation Model — 轮胎退化曲线拟合与预测核心

提供三类退化模型:
1. PolynomialModel  — 二次多项式 (简单直观, 实时展示)
2. PowerLawModel    — 幂律模型 A*age^B (物理意义好, 长期预测)
3. PiecewiseModel  — 分段线性 (拐点检测, 策略决策)

每类模型均支持:
- fit(age, degradation) — 从数据拟合参数
- predict(age)          — 预测退化量
- crossover_point(other_model) — 两曲线交叉点
"""

from __future__ import annotations
import math
import logging
import warnings
import numpy as np
from typing import Optional

logger = logging.getLogger(__name__)


# ── 数据清洗 ──────────────────────────────────────────────────────────────

def clean_degradation_data(
    age: list[float],
    degradation: list[float],
    min_deg: float = -0.3,
    max_deg: float = 5.0,
    iqr_mult: float = 3.0,
) -> tuple[list[float], list[float]]:
    """清洗退化数据：剔除异常点

    规则:
    1. deg > min_deg (排除进站/失误圈, 以及异常快圈)
    2. deg < max_deg (排除 VSC/SC/黄旗 导致的慢圈)
    3. IQR 双尾过滤: >= 8 个点时剔除低于 Q1 - iqr_mult*IQR 或高于 Q3 + iqr_mult*IQR 的点
    """
    if len(age) < 3:
        return list(age), list(degradation)

    pairs = list(zip(age, degradation))
    # 第一层过滤
    pairs = [(a, d) for a, d in pairs if min_deg < d < max_deg]

    if len(pairs) < 8:
        return [p[0] for p in pairs], [p[1] for p in pairs]

    # IQR 过滤
    vals = sorted(p[1] for p in pairs)
    n = len(vals)
    q1 = vals[n // 4]
    q3 = vals[3 * n // 4]
    iqr = q3 - q1
    lower = q1 - iqr_mult * iqr
    upper = q3 + iqr_mult * iqr

    pairs = [(a, d) for a, d in pairs if lower <= d <= upper]

    return [p[0] for p in pairs], [p[1] for p in pairs]


def apply_gaussian_smoothing(values: list, sigma: float = 1.0) -> list:
    if len(values) < 3:
        return values
    r = int(sigma * 3)
    kernel = np.exp(-(np.arange(-r, r+1) ** 2) / (2 * sigma ** 2))
    kernel = kernel / kernel.sum()
    return list(np.convolve(np.array(values, dtype=float), kernel, mode="same"))


# ── 退化模型基类 ──────────────────────────────────────────────────────────

class DegradationModel:
    """退化模型基类"""

    def __init__(self):
        self.params: dict = {}
        self.fitted: bool = False
        self._r2: float = 0.0

    def fit(self, age: list[float], degradation: list[float]) -> None:
        """从数据拟合模型参数"""
        raise NotImplementedError

    def predict(self, age: float) -> float:
        """预测在给定轮胎圈数时的退化量(秒)"""
        raise NotImplementedError

    def predict_array(self, ages: list[float]) -> list[float]:
        """批量预测"""
        return [self.predict(a) for a in ages]

    @property
    def r_squared(self) -> float:
        return self._r2

    def crossover_point(self, other: DegradationModel,
                        start: float = 1, end: float = 50,
                        tol: float = 0.01) -> Optional[float]:
        """找到两模型退化曲线的交叉点(轮胎圈数).
        使用二分法在 [start, end] 区间搜索.
        """
        lo, hi = start, end
        d_lo = self.predict(lo) - other.predict(lo)

        # 如果没有交叉点
        if d_lo * (self.predict(hi) - other.predict(hi)) > 0:
            return None

        for _ in range(50):
            mid = (lo + hi) / 2.0
            d_mid = self.predict(mid) - other.predict(mid)
            if abs(d_mid) < tol:
                return round(mid, 1)
            if d_lo * d_mid <= 0:
                hi = mid
            else:
                lo = mid
                d_lo = d_mid
        return round((lo + hi) / 2.0, 1)

    def laps_to_threshold(self, threshold: float,
                          start: float = 1, end: float = 80) -> Optional[float]:
        """预测达到指定退化阈值所需的圈数"""
        lo, hi = start, end
        if self.predict(lo) > threshold:
            return lo
        if self.predict(hi) < threshold:
            return None
        for _ in range(50):
            mid = (lo + hi) / 2.0
            if self.predict(mid) < threshold:
                lo = mid
            else:
                hi = mid
            if abs(self.predict(mid) - threshold) < 0.01:
                return round(mid, 1)
        return round((lo + hi) / 2.0, 1)


# ── 多项式模型 (二次) ─────────────────────────────────────────────────────

class PolynomialModel(DegradationModel):
    """二次多项式: deg = a*age^2 + b*age + c

    适合: 实时趋势展示, 5-8圈以上数据即可拟合
    """

    def fit(self, age: list[float], degradation: list[float]) -> None:
        n = len(age)
        if n < 3:
            logger.warning("Not enough data points for PolynomialModel")
            return

        # 先归一化 age 再拟合，消除多峰小值数据的数值不稳定问题
        age_arr = np.array(age, dtype=float)
        mean_age = float(np.mean(age_arr))
        std_age = float(np.std(age_arr))
        if std_age > 1e-10:
            age_norm = (age_arr - mean_age) / std_age
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', np.RankWarning)
                coeffs_norm = np.polyfit(age_norm, degradation, 2)
            # 反归一化系数回原始 age 尺度
            s2 = std_age * std_age
            a = coeffs_norm[0] / s2
            b = coeffs_norm[1] / std_age - 2.0 * coeffs_norm[0] * mean_age / s2
            c = coeffs_norm[2] - coeffs_norm[1] * mean_age / std_age + coeffs_norm[0] * mean_age * mean_age / s2
            coeffs = [a, b, c]
        else:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', np.RankWarning)
                coeffs = np.polyfit(age, degradation, 2)
        self.params = {
            "a": coeffs[0],
            "b": coeffs[1],
            "c": coeffs[2],
            "type": "polynomial",
        }
        self.fitted = True
        self._calc_r2(age, degradation)

    def predict(self, age: float) -> float:
        if not self.fitted:
            return 0.0
        a = self.params["a"]
        b = self.params["b"]
        c = self.params["c"]
        return a * age * age + b * age + c

    def _calc_r2(self, age: list[float], deg: list[float]) -> None:
        mean_y = sum(deg) / len(deg)
        ss_res = sum((d - self.predict(a)) ** 2 for a, d in zip(age, deg))
        ss_tot = sum((d - mean_y) ** 2 for d in deg)
        self._r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

    def derivative(self, age: float) -> float:
        """退化速率: d(deg)/d(age)"""
        if not self.fitted:
            return 0.0
        return 2.0 * self.params["a"] * age + self.params["b"]


# ── 幂律模型 ──────────────────────────────────────────────────────────────

class PowerLawModel(DegradationModel):
    """幂律退化: deg = A * age^B

    log-log 线性化: log(deg) = log(A) + B * log(age)
    适合: 长期预测, 刻画退化加速现象
    B > 1 -> 加速退化; B = 1 -> 线性退化; B < 1 -> 减速退化
    """

    def __init__(self):
        super().__init__()

    def fit(self, age: list[float], degradation: list[float]) -> None:
        n = len(age)
        if n < 3:
            logger.warning("Not enough data points for PowerLawModel")
            return

        # 直接拟合 deg = A * age^B  (幂律)
        # log(deg) = log(A) + B * log(age)
        # 只取 deg > 0 且 age > 0 的点 (age=0 时 log(age) 无定义)
        pos_pairs = [(a, d) for a, d in zip(age, degradation) if d > 0.01 and a > 0]
        if len(pos_pairs) < 3:
            logger.warning("Not enough positive degradation for PowerLawModel")
            return

        x = [math.log(a) for a, _ in pos_pairs]
        y = [math.log(d) for _, d in pos_pairs]

        n2 = len(x)
        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(a * b for a, b in zip(x, y))
        sum_x2 = sum(v * v for v in x)

        denom = n2 * sum_x2 - sum_x * sum_x
        if abs(denom) < 1e-12:
            logger.warning("PowerLawModel: all x values identical, skipping fit")
            return
        B = (n2 * sum_xy - sum_x * sum_y) / denom
        logA = (sum_y - B * sum_x) / n2

        self.params = {
            "A": math.exp(logA),
            "B": B,
            "type": "power_law",
        }
        self.fitted = True
        self._calc_r2(age, degradation)

    def predict(self, age: float) -> float:
        if not self.fitted:
            return 0.0
        A = self.params["A"]
        B = self.params["B"]
        return A * (age ** B)

    def _calc_r2(self, age: list[float], deg: list[float]) -> None:
        mean_y = sum(deg) / len(deg)
        ss_res = sum((d - self.predict(a)) ** 2 for a, d in zip(age, deg))
        ss_tot = sum((d - mean_y) ** 2 for d in deg)
        self._r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0


# ── 分段线性模型 (Perez 模型) ─────────────────────────────────────────────

class PiecewiseModel(DegradationModel):
    """分段线性: 退化为两个阶段的线性函数

    阶段1 (线性磨损期):  deg = k1 * age
    阶段2 (悬崖期):      deg = k1 * bp + k2 * (age - bp)

    bp = breakpoint (拐点)
    k1 = 早期退化速率 (秒/圈)
    k2 = 后期退化速率 (秒/圈) — 通常 k2 > k1
    """

    def __init__(self, min_samples_per_segment: int = 2):
        super().__init__()
        self.min_samples = min_samples_per_segment

    def fit(self, age: list[float], degradation: list[float]) -> None:
        n = len(age)
        if n < self.min_samples * 2:
            logger.warning("Not enough data for PiecewiseModel")
            return

        sorted_pairs = sorted(zip(age, degradation), key=lambda p: p[0])
        x = [p[0] for p in sorted_pairs]
        y = [p[1] for p in sorted_pairs]

        # 穷举搜索拐点
        best_bp = x[len(x) // 3]
        best_k1, best_k2 = 0.0, 0.0
        best_c1, best_c2 = 0.0, 0.0
        best_r2 = -float("inf")

        # 尝试每个可能的拐点(至少 min_samples 个点在每段)
        for i in range(self.min_samples, n - self.min_samples + 1):
            bp = (x[i - 1] + x[i]) / 2.0

            # --- 左段 ---
            x_left = x[:i]
            y_left = y[:i]
            n1 = len(x_left)
            sx1 = sum(x_left)
            sy1 = sum(y_left)
            denom1 = n1 * sum(a * a for a in x_left) - sx1 * sx1
            if abs(denom1) < 1e-12:
                k1 = 0.0
                c1 = sy1 / n1
            else:
                k1 = (n1 * sum(a * b for a, b in zip(x_left, y_left)) - sx1 * sy1) / denom1
                c1 = (sy1 - k1 * sx1) / n1

            # --- 右段 ---
            x_right = x[i:]
            y_right = y[i:]
            n2 = len(x_right)
            sx2 = sum(x_right)
            sy2 = sum(y_right)
            denom2 = n2 * sum(a * a for a in x_right) - sx2 * sx2
            if abs(denom2) < 1e-12:
                k2 = 0.0
                c2 = sy2 / n2
            else:
                k2 = (n2 * sum(a * b for a, b in zip(x_right, y_right)) - sx2 * sy2) / denom2
                c2 = (sy2 - k2 * sx2) / n2

            left_val = k1 * bp + c1
            pred = []
            for xi in x:
                if xi <= bp:
                    pred.append(k1 * xi + c1)
                else:
                    pred.append(left_val + k2 * (xi - bp))

            mean_y = sum(y) / n
            ss_res = sum((yi - p) ** 2 for yi, p in zip(y, pred))
            ss_tot = sum((yi - mean_y) ** 2 for yi in y)
            r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

            if r2 > best_r2:
                best_r2 = r2
                best_bp = bp
                best_k1, best_k2 = k1, k2
                best_c1, best_c2 = c1, c2

        if best_k2 < 0:
            return  # degradation decreases, keep fitted=False
        self.params = {
            "breakpoint": best_bp,
            "k1": best_k1,
            "k2": best_k2,
            "c1": best_c1,
            "c2": best_c2,
            "type": "piecewise",
        }
        self._r2 = best_r2
        self.fitted = True

    def predict(self, age: float) -> float:
        if not self.fitted:
            return 0.0
        bp = self.params["breakpoint"]
        k1 = self.params["k1"]
        k2 = self.params["k2"]
        c1 = self.params.get("c1", 0.0)
        c2 = self.params.get("c2", 0.0)
        if age <= bp:
            return k1 * age + c1
        else:
            left_val = k1 * bp + c1
            return left_val + k2 * (age - bp)

    @property
    def breakpoint(self) -> Optional[float]:
        return self.params.get("breakpoint")

    @property
    def cliff_rate(self) -> Optional[float]:
        """悬崖段的退化速率"""
        return self.params.get("k2")

    @property
    def wear_rate(self) -> Optional[float]:
        """早期磨损速率"""
        return self.params.get("k1")


# ── 模型工厂 ──────────────────────────────────────────────────────────────


class PiecewiseQuadraticModel(DegradationModel):
    """Segmented quadratic: left=quadratic, right=quadratic with C0 continuity."""

    def __init__(self, min_samples_per_segment: int = 3):
        super().__init__()
        self.min_samples = min_samples_per_segment

    def fit(self, age, degradation):
        n = len(age)
        if n < self.min_samples * 2:
            return
        pairs = sorted(zip(age, degradation), key=lambda p: p[0])
        x = [p[0] for p in pairs]
        y = [p[1] for p in pairs]
        best_r2 = -float("inf")
        best_p = None
        for i in range(self.min_samples, n - self.min_samples + 1):
            bp = (x[i-1] + x[i]) / 2.0
            xl, yl = x[:i], y[:i]
            X1 = np.vstack([[a*a for a in xl], xl, [1.0]*len(xl)]).T
            try:
                c1 = np.linalg.lstsq(X1, yl, rcond=None)[0]
            except:
                continue
            a1, b1, c1_ = float(c1[0]), float(c1[1]), float(c1[2])
            # Left monotonicity check
            dl = min(2*a1*a + b1 for a in xl)
            if abs(a1) > 1e-12:
                v = -b1/(2*a1)
                if xl[0] <= v <= xl[-1]:
                    dl = min(dl, 2*a1*v + b1)
            if dl < -1e-6:
                continue
            # Right segment with C0 constraint
            xr, yr = x[i:], y[i:]
            lv = a1*bp*bp + b1*bp + c1_
            X2 = np.vstack([[a*a - bp*bp for a in xr], [a - bp for a in xr]]).T
            try:
                c2 = np.linalg.lstsq(X2, [yr[j] - lv for j in range(len(yr))], rcond=None)[0]
            except:
                continue
            a2, b2 = float(c2[0]), float(c2[1])
            c2_ = lv - a2*bp*bp - b2*bp
            # Right monotonicity check
            dr = min(2*a2*a + b2 for a in xr)
            if abs(a2) > 1e-12:
                v = -b2/(2*a2)
                if xr[0] <= v <= xr[-1]:
                    dr = min(dr, 2*a2*v + b2)
            if dr < -1e-6:
                continue
            # Predict and R2
            pred = [lv + a2*(xi*xi - bp*bp) + b2*(xi - bp) if xi > bp else a1*xi*xi + b1*xi + c1_ for xi in x]
            my = sum(y)/n
            ssr = sum((yi - p)**2 for yi, p in zip(y, pred))
            sst = sum((yi - my)**2 for yi in y)
            r2 = 1.0 - ssr/sst if sst > 0 else 0.0
            if r2 > best_r2:
                best_r2 = r2
                best_p = {"breakpoint": bp, "a1": a1, "b1": b1, "c1": c1_, "a2": a2, "b2": b2, "c2": c2_, "type": "piecewise_quadratic"}
        if best_p is None:
            return
        self.params = best_p
        self._r2 = best_r2
        self.fitted = True

    def predict(self, age):
        if not self.fitted:
            return 0.0
        bp = self.params["breakpoint"]
        a1, b1, c1 = self.params["a1"], self.params["b1"], self.params["c1"]
        a2, b2 = self.params["a2"], self.params["b2"]
        if age <= bp:
            return a1*age*age + b1*age + c1
        lv = a1*bp*bp + b1*bp + c1
        return lv + a2*(age*age - bp*bp) + b2*(age - bp)

    @property
    def breakpoint(self):
        return self.params.get("breakpoint")


class ThreeSegmentPiecewiseModel(DegradationModel):
    """Three-segment linear: left plateau, middle cliff, right plateau."""
    def __init__(self, min_samples_per_segment: int = 2):
        super().__init__()
        self.min_samples = min_samples_per_segment
    def fit(self, age, degradation):
        n = len(age)
        if n < self.min_samples * 3: return
        pairs = sorted(zip(age, degradation), key=lambda p: p[0])
        x = [p[0] for p in pairs]; y = [p[1] for p in pairs]
        best_r2 = -float("inf"); best_p = None
        for i in range(self.min_samples, n - 2*self.min_samples + 1):
            for j in range(i + self.min_samples, n - self.min_samples + 1):
                bp1 = (x[i-1] + x[i]) / 2.0; bp2 = (x[j-1] + x[j]) / 2.0
                xl, yl = x[:i], y[:i]; n1 = len(xl)
                sx1 = sum(xl); sy1 = sum(yl)
                d1 = n1 * sum(a*a for a in xl) - sx1*sx1
                if abs(d1) < 1e-12: k1, c1 = 0.0, sy1/n1
                else:
                    k1 = (n1 * sum(a*b for a,b in zip(xl,yl)) - sx1*sy1) / d1
                    c1 = (sy1 - k1*sx1) / n1
                if k1 < 0: continue
                lv1 = k1*bp1 + c1
                xm, ym = x[i:j], y[i:j]
                num2 = sum((a-bp1)*(ym[t]-lv1) for t,a in enumerate(xm))
                den2 = sum((a-bp1)**2 for a in xm)
                if abs(den2) < 1e-12: continue
                k2 = num2 / den2
                if k2 < 0: continue
                mv = lv1 + k2*(bp2 - bp1)
                xr, yr = x[j:], y[j:]
                num3 = sum((a-bp2)*(yr[t]-mv) for t,a in enumerate(xr))
                den3 = sum((a-bp2)**2 for a in xr)
                if abs(den3) < 1e-12: continue
                k3 = num3 / den3
                if k3 < 0: continue
                pred = [k1*xi+c1 if xi<=bp1 else (lv1+k2*(xi-bp1) if xi<=bp2 else mv+k3*(xi-bp2)) for xi in x]
                my = sum(y)/n
                ssr = sum((yi-pi)**2 for yi,pi in zip(y,pred))
                sst = sum((yi-my)**2 for yi in y)
                r2 = 1.0 - ssr/sst if sst > 0 else 0.0
                if r2 > best_r2:
                    best_r2 = r2
                    best_p = {"breakpoint1":bp1,"breakpoint2":bp2,"k1":k1,"c1":c1,"k2":k2,"k3":k3,"type":"three_segment"}
        if best_p is None: return
        self.params = best_p; self._r2 = best_r2; self.fitted = True
    def predict(self, age):
        if not self.fitted: return 0.0
        p = self.params
        if age <= p["breakpoint1"]: return p["k1"]*age + p["c1"]
        lv = p["k1"]*p["breakpoint1"] + p["c1"]
        if age <= p["breakpoint2"]: return lv + p["k2"]*(age - p["breakpoint1"])
        mv = lv + p["k2"]*(p["breakpoint2"] - p["breakpoint1"])
        return mv + p["k3"]*(age - p["breakpoint2"])
    @property
    def breakpoint1(self): return self.params.get("breakpoint1")
    @property
    def breakpoint2(self): return self.params.get("breakpoint2")
def fit_best_model(age: list[float], degradation: list[float],
                   clean: bool = True,
                   min_deg: float = -0.3,
                   max_deg: float = 5.0,
                   iqr_mult: float = 3.0,
                   piecewise_r2_threshold: float = 0.3) -> DegradationModel:
    """Fit degradation: Piecewise first, fallback to monotonic linear."""
    if clean:
        age, degradation = clean_degradation_data(
            age, degradation,
            min_deg=min_deg, max_deg=max_deg, iqr_mult=iqr_mult,
        )
    n = len(age)
    if n < 2:
        return PolynomialModel()

    # Three-segment Piecewise Linear (preferred, needs >= 6 pts)
    if n >= 6:
        ts = ThreeSegmentPiecewiseModel(min_samples_per_segment=2)
        ts.fit(age, degradation)
        if ts.fitted and ts.r_squared >= piecewise_r2_threshold:
            logger.info(
                "ThreeSegment: R2={:.3f} bp1={:.1f} bp2={:.1f} k2={:.4f} k3={:.4f}".format(
                    ts.r_squared, ts.params["breakpoint1"], ts.params["breakpoint2"],
                    ts.params["k2"], ts.params["k3"]
                )
            )
            return ts

    # Fallback: monotonic linear
    if n >= 2:
        x = np.array(age)
        y = np.array(degradation)
        A = np.vstack([x, np.ones_like(x)]).T
        slope, intercept = np.linalg.lstsq(A, y, rcond=None)[0]
        if slope < 0:
            slope = 0.0
            intercept = float(np.mean(y))
        linear_model = PolynomialModel()
        linear_model.params = {"a": 0.0, "b": slope, "c": intercept, "type": "linear"}
        linear_model.fitted = True
        pred = slope * x + intercept
        ss_res = float(np.sum((y - pred) ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        linear_model._r2 = 1.0 - ss_res / ss_tot if ss_tot != 0 else 0.0
        logger.info(
            "Linear fallback: slope={:.4f} intercept={:.4f} R2={:.3f}".format(
                slope, intercept, linear_model._r2
            )
        )
        return linear_model

    return PolynomialModel()


# ── 前端序列化 ────────────────────────────────────────────────────────────

def model_to_dict(model: DegradationModel,
                  age_min: float = 1, age_max: float = 50,
                  num_points: int = 50) -> dict:
    """将模型输出为前端可用的 JSON 结构"""
    if not model.fitted:
        return {"type": "none", "params": {}, "curve": []}

    ages = [age_min + (age_max - age_min) * i / (num_points - 1)
            for i in range(num_points)]
    preds = model.predict_array(ages)

    return {
        "type": model.params.get("type", "unknown"),
        "params": {k: round(v, 6) if isinstance(v, float) else v
                   for k, v in model.params.items()},
        "r_squared": round(model.r_squared, 4),
        "curve": [{"age": round(a, 1), "degradation": round(d, 4)}
                  for a, d in zip(ages, preds)],
    }


# ── Demo ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    # 模拟真实退化数据: MEDIUM tire over 12 laps
    age_data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    deg_data = [0.00, 0.17, 0.52, 0.63, 0.85, 0.91,
                1.03, 1.15, 1.19, 1.31, 1.37, 1.45]

    # 拟合三种模型
    poly = PolynomialModel()
    poly.fit(age_data, deg_data)
    print(f"Polynomial: R2={poly.r_squared:.3f}")
    print(f"  deg(15) = {poly.predict(15):.3f}s")
    print(f"  deg rate at lap 10 = {poly.derivative(10):.3f}s/lap")
    print()

    pw_ = PowerLawModel()
    pw_.fit(age_data, deg_data)
    print(f"PowerLaw: R2={pw_.r_squared:.3f}")
    print(f"  deg(15) = {pw_.predict(15):.3f}s")
    print()

    pw = PiecewiseModel()
    pw.fit(age_data, deg_data)
    print(f"Piecewise: R2={pw.r_squared:.3f}")
    print(f"  breakpoint = {pw.breakpoint}")
    print(f"  wear_rate = {pw.wear_rate:.4f}s/lap")
    print(f"  cliff_rate = {pw.cliff_rate:.4f}s/lap")
    print(f"  deg(15) = {pw.predict(15):.3f}s")
    print()

    # 自动选择
    ts = ThreeSegmentPiecewiseModel()
    ts.fit(age_data, deg_data)
    print("ThreeSegment: R2={:.3f}".format(ts.r_squared))
    if ts.fitted:
        print("  bp1={:.1f} bp2={:.1f}  k1={:.4f} k2={:.4f} k3={:.4f}".format(ts.params["breakpoint1"], ts.params["breakpoint2"], ts.params["k1"], ts.params["k2"], ts.params["k3"]))
        print("  deg(15) = {:.3f}s".format(ts.predict(15)))
    print()

    best = fit_best_model(age_data, deg_data)
    print(f"Best model: {type(best).__name__} R2={best.r_squared:.3f}")
    print()

    # 交叉点
    crossover = poly.crossover_point(pw_)
    print(f"Crossover: lap {crossover}" if crossover else "No crossover")
    print()

    # 退化阈值预测
    t = 1.5
    lap_needed = poly.laps_to_threshold(t)
    print(f"Laps to {t:.1f}s degradation: lap {lap_needed}")

    # 前端 JSON
    import json
    print(f"\nFrontend JSON:")
    print(json.dumps(model_to_dict(best, 1, 20, 10), indent=2))
