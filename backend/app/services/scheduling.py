"""调度计算：计划执行派生数据的唯一来源。

调度编排在复用旧计划和首次登记新计划时，都需要计算「下次执行时间」和
「计划状态」。这里把这两件事收拢成一份纯计算 + 一个原子的计划登记入口，
计划查询、试算、登记和执行台账回写都复用同一份结果。
"""
from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.store import store

MODULE = "scheduler"
DEFAULT_PLAN_CRON = "0 */6 * * *"
DEFAULT_PLAN_TIMEZONE = "Asia/Shanghai"
DEFAULT_CRON_FIRST_FIELD = "* * * * *"

# 计划状态映射保持原有行为：待执行/采集执行 → 执行中，其余 → 已完成。
ACTIVE_JOB_STATUSES = ("待执行", "采集执行")


def plan_status_for_job(job_status: str) -> str:
    """计划状态只由采集作业状态推导，复用和新建走同一处判断。"""
    return "执行中" if job_status in ACTIVE_JOB_STATUSES else "已完成"


# ---------------------------------------------------------------------------
# 固定本地时钟
# ---------------------------------------------------------------------------

_CLOCK_LOCK = threading.RLock()
_CLOCK_OVERRIDE: datetime | None = None


def utc_now() -> datetime:
    """当前时间；测试和演练可以固定，避免派生数据随真实时钟漂移。"""
    with _CLOCK_LOCK:
        fixed = _CLOCK_OVERRIDE
    return fixed if fixed is not None else datetime.now(timezone.utc)


def set_clock(value: datetime | str | None) -> datetime | None:
    """固定本地时钟；传 None 恢复系统时钟。返回生效的 UTC 时间。"""
    global _CLOCK_OVERRIDE
    parsed = _parse_datetime(value) if value is not None else None
    with _CLOCK_LOCK:
        _CLOCK_OVERRIDE = parsed
    return utc_now()


def clock_is_fixed() -> bool:
    with _CLOCK_LOCK:
        return _CLOCK_OVERRIDE is not None


@contextmanager
def fixed_clock(value: datetime | str) -> Iterator[datetime]:
    """测试用：在上下文内固定时钟，退出后恢复原时钟。"""
    global _CLOCK_OVERRIDE
    parsed = _parse_datetime(value)
    with _CLOCK_LOCK:
        previous = _CLOCK_OVERRIDE
        _CLOCK_OVERRIDE = parsed
    try:
        yield parsed
    finally:
        with _CLOCK_LOCK:
            _CLOCK_OVERRIDE = previous


def _parse_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Cron 与时区
# ---------------------------------------------------------------------------

def resolve_timezone(name: str | None) -> ZoneInfo:
    try:
        return ZoneInfo(str(name or DEFAULT_PLAN_TIMEZONE))
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"时区设置不合法：{name}") from exc


def _expand_cron_field(field: str, low: int, high: int) -> set[int]:
    values: set[int] = set()
    for part in field.split(","):
        part = part.strip()
        if not part:
            continue
        step = 1
        if "/" in part:
            part, step_text = part.split("/", 1)
            step = int(step_text)
            if step <= 0:
                raise ValueError("Cron 步长必须为正整数")
        if part in ("*", "?"):
            start, end = low, high
        elif "-" in part:
            start_text, end_text = part.split("-", 1)
            start, end = int(start_text), int(end_text)
        else:
            start = int(part)
            end = high if step != 1 else start
        if start < low or end > high or start > end:
            raise ValueError("Cron 字段超出取值范围")
        values.update(range(start, end + 1, step))
    if not values:
        raise ValueError("Cron 字段为空")
    return values


@dataclass(frozen=True)
class CronSpec:
    minutes: set[int]
    hours: set[int]
    days: set[int]
    months: set[int]
    weekdays: set[int]
    day_restricted: bool
    weekday_restricted: bool


def parse_cron(expr: str | None) -> CronSpec:
    text = " ".join(str(expr or "").split())
    if not text:
        text = DEFAULT_CRON_FIRST_FIELD
    fields = text.split(" ")
    if len(fields) != 5:
        raise ValueError(f"Cron表达式不合法（需要 5 段）：{expr}")
    try:
        minutes = _expand_cron_field(fields[0], 0, 59)
        hours = _expand_cron_field(fields[1], 0, 23)
        days = _expand_cron_field(fields[2], 1, 31)
        months = _expand_cron_field(fields[3], 1, 12)
        weekdays = _expand_cron_field(fields[4], 0, 7)
    except ValueError as exc:
        raise ValueError(f"Cron表达式不合法：{expr}") from exc
    weekdays = {0 if day == 7 else day for day in weekdays}
    return CronSpec(
        minutes=minutes,
        hours=hours,
        days=days,
        months=months,
        weekdays=weekdays,
        day_restricted=fields[2] not in ("*", "?"),
        weekday_restricted=fields[4] not in ("*", "?"),
    )


def _cron_matches(spec: CronSpec, moment: datetime) -> bool:
    if moment.month not in spec.months:
        return False
    day_ok = moment.day in spec.days
    # Cron 约定：星期字段 0/7 为周日，Python weekday 周一为 0。
    weekday = (moment.weekday() + 1) % 7
    weekday_ok = weekday in spec.weekdays
    if spec.day_restricted and spec.weekday_restricted:
        if not (day_ok or weekday_ok):
            return False
    elif not (day_ok and weekday_ok):
        return False
    return moment.hour in spec.hours and moment.minute in spec.minutes


def cron_next(expr: str | None, timezone_setting: str | None,
              after: datetime | None = None) -> datetime:
    """返回严格晚于 after 的下一次 Cron 执行时间（带计划时区偏移）。"""
    spec = parse_cron(expr)
    zone = resolve_timezone(timezone_setting)
    moment = (after or utc_now()).astimezone(zone)
    candidate = moment.replace(second=0, microsecond=0) + timedelta(minutes=1)
    for _ in range(366 * 24 * 60):
        if _cron_matches(spec, candidate):
            return candidate
        candidate += timedelta(minutes=1)
    raise ValueError(f"Cron表达式在一年内没有可执行时间：{expr}")


def cron_next_iso(expr: str | None, timezone_setting: str | None,
                  after: datetime | None = None) -> str:
    return cron_next(expr, timezone_setting, after).isoformat()


def parse_iso(value: str | datetime | None) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# 计划执行派生数据
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ScheduleExecution:
    """计划执行派生数据：计划登记、试算、确认和台账回写的共同载体。"""
    plan_id: str
    job_id: str
    cron_expr: str
    timezone_setting: str
    execution_time: str
    plan_status: str
    job_status: str
    reused_plan: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "计划编号": self.plan_id,
            "关联作业": self.job_id,
            "Cron表达式": self.cron_expr,
            "时区设置": self.timezone_setting,
            "计划时间": self.execution_time,
            "计划状态": self.plan_status,
            "作业状态": self.job_status,
            "复用计划": self.reused_plan,
        }


def plan_defaults(job_id: str) -> dict[str, str]:
    """首次登记计划时沿用的默认配置。"""
    return {
        "计划编号": f"SCHED-{job_id}",
        "Cron表达式": DEFAULT_PLAN_CRON,
        "时区设置": DEFAULT_PLAN_TIMEZONE,
    }


def find_plan_for_job(job_id: str) -> dict[str, Any] | None:
    for row in store.rows(MODULE):
        if str(row.get("关联作业", "")) == str(job_id):
            return row
    return None


def find_plan_by_id(plan_id: str) -> dict[str, Any] | None:
    for row in store.rows(MODULE):
        if str(row.get("计划编号", "")) == str(plan_id):
            return row
    return None


def derive_schedule(
    *,
    plan_id: str,
    job_id: str,
    job_status: str,
    cron_expr: str | None,
    timezone_setting: str | None,
    reused_plan: bool,
    now: datetime | None = None,
    after: datetime | str | None = None,
) -> ScheduleExecution:
    """唯一的派生入口：执行时间与计划状态在这里一起算完。"""
    cron = str(cron_expr or DEFAULT_PLAN_CRON).strip()
    tz = str(timezone_setting or DEFAULT_PLAN_TIMEZONE).strip()
    anchor = parse_iso(after) if after is not None else None
    execution = cron_next_iso(cron, tz, after=anchor or now or utc_now())
    return ScheduleExecution(
        plan_id=str(plan_id),
        job_id=str(job_id),
        cron_expr=cron,
        timezone_setting=tz,
        execution_time=execution,
        plan_status=plan_status_for_job(job_status),
        job_status=str(job_status),
        reused_plan=reused_plan,
    )


def derive_for_plan_row(plan_row: dict[str, Any], job_status: str, *,
                        now: datetime | None = None,
                        after: datetime | str | None = None) -> ScheduleExecution:
    """按已有计划自身的编号、Cron 和时区派生，保证旧计划兼容。"""
    return derive_schedule(
        plan_id=str(plan_row.get("计划编号", "")),
        job_id=str(plan_row.get("关联作业", "")),
        job_status=job_status,
        cron_expr=str(plan_row.get("Cron表达式") or DEFAULT_PLAN_CRON),
        timezone_setting=str(plan_row.get("时区设置") or DEFAULT_PLAN_TIMEZONE),
        reused_plan=True,
        now=now,
        after=after,
    )


def derive_for_new_plan(job_id: str, job_status: str, *,
                        plan_id: str | None = None,
                        cron_expr: str | None = None,
                        timezone_setting: str | None = None,
                        now: datetime | None = None) -> ScheduleExecution:
    defaults = plan_defaults(job_id)
    return derive_schedule(
        plan_id=str(plan_id or defaults["计划编号"]),
        job_id=job_id,
        job_status=job_status,
        cron_expr=cron_expr or defaults["Cron表达式"],
        timezone_setting=timezone_setting or defaults["时区设置"],
        reused_plan=False,
        now=now,
    )


def apply_derived_to_row(plan_row: dict[str, Any], derived: ScheduleExecution) -> dict[str, Any]:
    """把派生数据回写到计划行，字段集合保持原有登记契约不变。"""
    plan_row["计划编号"] = derived.plan_id
    plan_row["关联作业"] = derived.job_id
    plan_row["Cron表达式"] = derived.cron_expr
    plan_row["时区设置"] = derived.timezone_setting
    plan_row["下次执行"] = derived.execution_time
    plan_row["status"] = derived.plan_status
    plan_row["pending"] = derived.plan_status != "已停用"
    plan_row["abnormal"] = False
    return plan_row


# ---------------------------------------------------------------------------
# 试算 / 原子登记
# ---------------------------------------------------------------------------

_PLAN_LOCK = threading.RLock()


def preview_plan(job_id: str, job_status: str, *,
                 plan_id: str | None = None,
                 cron_expr: str | None = None,
                 timezone_setting: str | None = None,
                 now: datetime | None = None) -> ScheduleExecution:
    """只计算不落库；中断或重试都不会留下半条计划。

    已有计划优先沿用自身的编号、Cron、时区，忽略本次试算参数。
    """
    existing = find_plan_for_job(job_id)
    if existing is not None:
        return derive_for_plan_row(existing, job_status, now=now)
    return derive_for_new_plan(
        job_id, job_status,
        plan_id=plan_id, cron_expr=cron_expr, timezone_setting=timezone_setting,
        now=now,
    )


def register_plan(job_id: str, job_status: str, *,
                  plan_id: str | None = None,
                  cron_expr: str | None = None,
                  timezone_setting: str | None = None,
                  extra_fields: dict[str, Any] | None = None,
                  now: datetime | None = None) -> tuple[dict[str, Any], ScheduleExecution, bool]:
    """原子登记计划：同一作业只保留一条关联，重复登记只刷新派生数据。

    先完成全部校验和派生，再一次性追加完整行；任一步失败都不会留下半条计划。
    返回 (计划行, 派生数据, 是否新建)。
    """
    with _PLAN_LOCK:
        existing = find_plan_for_job(job_id)
        if existing is not None:
            derived = derive_for_plan_row(existing, job_status, now=now)
            apply_derived_to_row(existing, derived)
            return existing, derived, False

        derived = derive_for_new_plan(
            job_id, job_status,
            plan_id=plan_id, cron_expr=cron_expr,
            timezone_setting=timezone_setting, now=now,
        )

        duplicate_id = find_plan_by_id(derived.plan_id)
        if duplicate_id is not None and str(duplicate_id.get("关联作业")) != job_id:
            raise ValueError(f"计划编号 {derived.plan_id} 已被其他作业占用")

        rows = store.rows(MODULE)
        row: dict[str, Any] = {
            "id": max((int(item.get("id", 0)) for item in rows), default=0) + 1,
            "计划编号": derived.plan_id,
            "关联作业": job_id,
            "Cron表达式": derived.cron_expr,
            "时区设置": derived.timezone_setting,
            "依赖检查": str((extra_fields or {}).get("依赖检查") or ""),
            "失败通知": str((extra_fields or {}).get("失败通知") or ""),
            "并发控制": str((extra_fields or {}).get("并发控制") or ""),
            "下次执行": derived.execution_time,
            "status": derived.plan_status,
            "pending": derived.plan_status not in ("已停用",),
            "abnormal": False,
        }
        rows.append(row)  # 完整行一次落库
        return row, derived, True


def plan_view(plan_row: dict[str, Any]) -> dict[str, Any]:
    return dict(plan_row)
