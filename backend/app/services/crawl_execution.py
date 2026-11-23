"""采集任务执行台账：一轮采集一个实际入口，迟到结果与补跑都不破坏已确认的轮次。

一轮采集的生命周期：
- 领取入口（claim_next）：分配本轮唯一的实际入口，并通过计划执行派生数据回写台账；
- 产出批次（attach_batch）：把本轮落地的批次挂到入口上，同一轮只认一次产出；
- 确认完成（confirm_round）：批次产出后完成即确认，即使完成回执返回前中断也保持完成；
- 送达回执（deliver_receipt）：回执可重放，重复送达只增长回执序号；
- 补跑（resume_round）：同一轮再次领取时复用原入口与已落地批次，不重复产出；
- 迟到的执行中结果（apply_inflight_result）：只能回填到仍在执行中的最近一轮；
  已确认完成的轮次只留痕，不覆盖批次与完成，也不拦住下一轮。

计划确认结果通过 ``execution_context`` 读取计划执行派生数据并回写台账，
采集任务据此认领实际入口，并能从补跑结果查回本轮已经落地的批次。
"""
from __future__ import annotations

import threading
from datetime import datetime
from typing import Any

from app.services.scheduling import (
    derive_for_plan_row,
    find_plan_for_job,
    parse_iso,
    utc_now,
)
from app.store import store

MODULE = "crawl_execution"
ENTITY = "采集执行台账"

ROUND_IN_PROGRESS = "执行中"
ROUND_COMPLETED = "已完成"
ROUND_STATES = (ROUND_IN_PROGRESS, ROUND_COMPLETED)

RECEIPT_NONE = "未生成"
RECEIPT_INTERRUPTED = "中断"
RECEIPT_DELIVERED = "已回执"

# 只有仍在执行中的轮次占用入口，会拦住同作业的下一轮。
BLOCKING_STATES = (ROUND_IN_PROGRESS,)

_LEDGER_LOCK = threading.RLock()


# ---------------------------------------------------------------------------
# 行访问
# ---------------------------------------------------------------------------

def _rows() -> list[dict[str, Any]]:
    return store.rows(MODULE)


def find_round(round_key: str) -> dict[str, Any] | None:
    for row in _rows():
        if str(row.get("轮次")) == str(round_key):
            return row
    return None


def rounds_for_job(job_id: str) -> list[dict[str, Any]]:
    rows = [row for row in _rows() if str(row.get("关联作业")) == str(job_id)]
    return sorted(rows, key=lambda row: int(row.get("轮次序号", 0) or 0))


def find_round_for_job(job_id: str) -> dict[str, Any] | None:
    """最近一轮（按轮次序号）的台账行。"""
    rows = rounds_for_job(job_id)
    return rows[-1] if rows else None


def job_row(job_id: str) -> dict[str, Any] | None:
    for row in store.rows("crawl_job"):
        if str(row.get("作业编号")) == str(job_id):
            return row
    return None


def entry_url_for_job(job_id: str) -> str:
    row = job_row(job_id) or {}
    raw = str(row.get("入口URL", ""))
    urls = [part.strip() for part in raw.split(",") if part.strip()]
    return urls[0] if urls else (raw.strip() or f"job://{job_id}")


def _round_entry_url(base_url: str, round_no: int) -> str:
    joiner = "&" if "?" in base_url else "?"
    return f"{base_url}{joiner}round={round_no}"


def _next_round_no(job_id: str) -> int:
    numbers = [int(row.get("轮次序号", 0) or 0) for row in rounds_for_job(job_id)]
    return max(numbers, default=0) + 1


def _append_round(row: dict[str, Any]) -> dict[str, Any]:
    rows = _rows()
    row["id"] = max((int(item.get("id", 0)) for item in rows), default=0) + 1
    rows.append(row)
    return row


# ---------------------------------------------------------------------------
# 计划执行派生数据 → 台账
# ---------------------------------------------------------------------------

def execution_context(job_id: str, now: datetime | None = None) -> dict[str, Any]:
    """计划确认结果：按已有计划的编号、Cron 和时区重新派生执行时间。"""
    plan_row = find_plan_for_job(job_id)
    if plan_row is None:
        return {}
    job = job_row(job_id) or {}
    job_status = str(job.get("status") or "待执行")
    derived = derive_for_plan_row(plan_row, job_status, now=now)
    return derived.as_dict()


def write_plan_derived(round_row: dict[str, Any],
                       plan_derived: dict[str, Any] | None) -> dict[str, Any]:
    """把计划执行派生数据回写到轮次台账。"""
    if not plan_derived:
        return round_row
    round_row["计划编号"] = str(plan_derived.get("计划编号", round_row.get("计划编号", "")))
    round_row["计划时间"] = str(plan_derived.get("计划时间", ""))
    round_row["计划状态"] = str(plan_derived.get("计划状态", ""))
    round_row["Cron表达式"] = str(plan_derived.get("Cron表达式", ""))
    round_row["时区设置"] = str(plan_derived.get("时区设置", ""))
    if plan_derived.get("关联作业"):
        round_row["关联作业"] = str(plan_derived["关联作业"])
    return round_row


# ---------------------------------------------------------------------------
# 领取入口 / 补跑
# ---------------------------------------------------------------------------

def _claimed(row: dict[str, Any], *, resumed: bool) -> dict[str, Any]:
    return {
        "轮次": row.get("轮次"),
        "关联作业": row.get("关联作业"),
        "计划编号": row.get("计划编号", ""),
        "轮次序号": row.get("轮次序号"),
        "实际入口": row.get("实际入口", ""),
        "批次编号": row.get("批次编号", ""),
        "批次数量": row.get("批次数量", 0),
        "轮次状态": row.get("轮次状态"),
        "回执状态": row.get("回执状态", RECEIPT_NONE),
        "补跑": resumed,
        "领取次数": row.get("领取次数", 0),
        "计划时间": row.get("计划时间", ""),
        "计划状态": row.get("计划状态", ""),
    }


def _count_claim(row: dict[str, Any]) -> None:
    row["领取次数"] = int(row.get("领取次数", 0) or 0) + 1


def claim_next(job_id: str, *, plan_derived: dict[str, Any] | None = None,
               now: datetime | None = None) -> tuple[dict[str, Any] | None, str]:
    """领取作业的下一轮实际入口。

    只有仍在执行中的轮次会复用入口；已完成（含回执中断）的轮次不拦下一轮，
    会分配新的轮次入口。
    """
    with _LEDGER_LOCK:
        if job_row(job_id) is None:
            return None, f"采集作业 {job_id} 不存在"

        context = plan_derived if plan_derived is not None else execution_context(job_id, now=now)
        latest = find_round_for_job(job_id)

        if latest is not None and latest.get("轮次状态") in BLOCKING_STATES:
            # 同一轮重复领取：入口与计划派生数据都保持首次领取时记录的值。
            _count_claim(latest)
            return _claimed(latest, resumed=True), (
                f"轮次 {latest.get('轮次')} 仍在执行，复用实际入口 {latest.get('实际入口')}")

        round_no = _next_round_no(job_id)
        base_url = entry_url_for_job(job_id)
        row: dict[str, Any] = {
            "轮次": f"RND-{job_id}-{round_no:04d}",
            "关联作业": job_id,
            "计划编号": str((context or {}).get("计划编号", "")),
            "轮次序号": round_no,
            "实际入口": _round_entry_url(base_url, round_no),
            "计划时间": str((context or {}).get("计划时间", "")),
            "计划状态": str((context or {}).get("计划状态", "")),
            "Cron表达式": str((context or {}).get("Cron表达式", "")),
            "时区设置": str((context or {}).get("时区设置", "")),
            "批次编号": "",
            "批次数量": 0,
            "轮次状态": ROUND_IN_PROGRESS,
            "回执状态": RECEIPT_NONE,
            "领取时间": utc_now().isoformat(),
            "完成时间": "",
            "回执序号": 0,
            "领取次数": 1,
            "迟到结果": [],
            "status": ROUND_IN_PROGRESS,
            "pending": True,
            "abnormal": False,
        }
        _append_round(row)
        return _claimed(row, resumed=False), (
            f"已领取第 {round_no} 轮实际入口 {row['实际入口']}")


def resume_round(job_id: str, round_key: str | None = None, *,
                 plan_derived: dict[str, Any] | None = None,
                 now: datetime | None = None) -> tuple[dict[str, Any] | None, str]:
    """值班员补跑同一轮：实际领取原入口，并复用已落地批次，绝不新建关联轮次。"""
    with _LEDGER_LOCK:
        if job_row(job_id) is None:
            return None, f"采集作业 {job_id} 不存在"

        target: dict[str, Any] | None
        if round_key:
            target = find_round(round_key)
            if target is None:
                return None, f"轮次 {round_key} 不存在"
            if str(target.get("关联作业")) != str(job_id):
                return None, f"轮次 {round_key} 不属于作业 {job_id}"
        else:
            target = find_round_for_job(job_id)
            if target is None:
                return claim_next(job_id, plan_derived=plan_derived, now=now)

        context = plan_derived if plan_derived is not None else execution_context(job_id, now=now)
        # 补跑是重放同一轮：只在实际领取入口时补记计划派生数据，已完成轮次不重置。
        if target.get("轮次状态") != ROUND_COMPLETED:
            write_plan_derived(target, context)
        elif not target.get("计划时间"):
            write_plan_derived(target, context)
        _count_claim(target)
        batch = str(target.get("批次编号") or "")
        suffix = f"，本轮批次 {batch}" if batch else "，本轮批次待产出"
        return _claimed(target, resumed=True), (
            f"已补跑轮次 {target.get('轮次')}，实际领取入口 {target.get('实际入口')}{suffix}")


# ---------------------------------------------------------------------------
# 批次与完成回执
# ---------------------------------------------------------------------------

def attach_batch(round_key: str, batch: Any) -> dict[str, Any] | None:
    """把本轮已经落地的批次挂到入口上；同一轮只认第一次产出。"""
    with _LEDGER_LOCK:
        row = find_round(round_key)
        if row is None:
            return None
        if not row.get("批次编号"):
            row["批次编号"] = str(getattr(batch, "batch_id", "") or "")
            row["批次数量"] = int(getattr(batch, "row_count", 0) or 0)
        return row


def record_batch(round_key: str, batch_id: str, row_count: int = 0) -> dict[str, Any] | None:
    with _LEDGER_LOCK:
        row = find_round(round_key)
        if row is None:
            return None
        if not row.get("批次编号"):
            row["批次编号"] = str(batch_id)
            row["批次数量"] = int(row_count or 0)
        return row


def confirm_round(round_key: str, *, deliver: bool = True,
                  receipt: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """确认完成：完成先于回执落定，回执中断不回滚完成状态。

    deliver=False 表示完成回执在返回前中断：轮次仍为已完成，回执状态记「中断」。
    """
    with _LEDGER_LOCK:
        row = find_round(round_key)
        if row is None:
            return None
        if row.get("轮次状态") != ROUND_COMPLETED:
            _set_round_status(row, ROUND_COMPLETED)
            row["完成时间"] = utc_now().isoformat()
        row["回执"] = dict(receipt or row.get("回执") or {})
        if deliver:
            _deliver_locked(row, receipt)
        else:
            row["回执状态"] = RECEIPT_INTERRUPTED
        return row


def deliver_receipt(round_key: str, receipt: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """重放/送达完成回执：幂等，只增长回执序号。"""
    with _LEDGER_LOCK:
        row = find_round(round_key)
        if row is None:
            return None
        if row.get("轮次状态") != ROUND_COMPLETED:
            confirm_round(round_key, deliver=False, receipt=receipt)
            row = find_round(round_key)
            if row is None:
                return None
        if receipt:
            row["回执"] = dict(receipt)
        _deliver_locked(row, receipt)
        return row


def _deliver_locked(row: dict[str, Any], receipt: dict[str, Any] | None) -> None:
    row["回执状态"] = RECEIPT_DELIVERED
    row["回执序号"] = int(row.get("回执序号", 0) or 0) + 1
    if receipt:
        row["回执"] = dict(receipt)


def _set_round_status(row: dict[str, Any], status: str) -> None:
    row["轮次状态"] = status
    row["status"] = status
    row["pending"] = status in BLOCKING_STATES


def is_round_completed(round_key: str) -> bool:
    row = find_round(round_key)
    return bool(row and row.get("轮次状态") == ROUND_COMPLETED)


def round_blocks_next(job_id: str) -> bool:
    latest = find_round_for_job(job_id)
    return bool(latest and latest.get("轮次状态") in BLOCKING_STATES)


# ---------------------------------------------------------------------------
# 迟到的执行中结果
# ---------------------------------------------------------------------------

def apply_inflight_result(job_id: str, result: dict[str, Any],
                          received_at: datetime | str | None = None) -> dict[str, Any]:
    """接收迟到的执行中结果；只有仍在执行中的最近一轮才能被回填。

    返回 ``applied`` 为 False 表示结果被同轮完成挡下，批次与完成状态保持不变。
    """
    with _LEDGER_LOCK:
        explicit_round = str(result.get("轮次") or result.get("round_key") or "")
        row = find_round(explicit_round) if explicit_round else find_round_for_job(job_id)

        decision: dict[str, Any] = {
            "applied": False,
            "job_id": job_id,
            "round_key": explicit_round,
            "batch_id": "",
            "reason": "",
        }
        if row is None:
            decision["reason"] = "该作业还没有领取过采集入口"
            return decision

        decision["round_key"] = str(row.get("轮次"))
        world_time = parse_iso(received_at) or utc_now()
        result_batch = str(result.get("批次编号") or result.get("batch_id") or "")
        result_entry = str(result.get("实际入口") or result.get("entry_url") or "")
        trace = {
            "结果批次": result_batch,
            "结果入口": result_entry,
            "到达时间": world_time.isoformat(),
        }

        if row.get("轮次状态") == ROUND_COMPLETED:
            trace["原因"] = "同轮已完成，迟到结果只留痕"
            row.setdefault("迟到结果", []).append(trace)
            decision["batch_id"] = str(row.get("批次编号", ""))
            decision["reason"] = "同轮已经确认完成，迟到执行中结果不覆盖批次与完成"
            return decision

        if result_entry and result_entry != str(row.get("实际入口", "")):
            trace["原因"] = "入口与已认领入口不一致"
            row.setdefault("迟到结果", []).append(trace)
            decision["batch_id"] = str(row.get("批次编号", ""))
            decision["reason"] = "迟到结果的实际入口与本轮已认领入口不一致"
            return decision

        existing_batch = str(row.get("批次编号", "") or "")
        if existing_batch and result_batch and result_batch != existing_batch:
            trace["原因"] = "本轮批次已落地，迟到结果不覆盖"
            row.setdefault("迟到结果", []).append(trace)
            decision["batch_id"] = existing_batch
            decision["reason"] = "本轮批次已经落地，迟到结果不覆盖"
            return decision

        trace["原因"] = "回填到进行中的本轮"
        row.setdefault("迟到结果", []).append(trace)
        if result_batch and not existing_batch:
            row["批次编号"] = result_batch
            row["批次数量"] = int(result.get("数据行数", 0) or result.get("row_count", 0) or 0)
        decision["applied"] = True
        decision["batch_id"] = str(row.get("批次编号", ""))
        decision["reason"] = "执行中结果已回填到本轮"
        return decision


# ---------------------------------------------------------------------------
# 查询
# ---------------------------------------------------------------------------

def batch_row(batch_id: str) -> dict[str, Any] | None:
    if not batch_id:
        return None
    for row in store.rows("batch_store"):
        if str(row.get("批次编号")) == str(batch_id):
            return row
    return None


def batches_for_job(job_id: str) -> list[dict[str, Any]]:
    return [row for row in store.rows("batch_store")
            if str(row.get("来源作业")) == str(job_id)]


def round_summary(round_key: str) -> dict[str, Any] | None:
    """补跑结果查回本轮已经落地的批次。"""
    with _LEDGER_LOCK:
        row = find_round(round_key)
        if row is None:
            return None
        batch_id = str(row.get("批次编号", "") or "")
        stored = batch_row(batch_id)
        return {
            "轮次": row.get("轮次"),
            "关联作业": row.get("关联作业"),
            "计划编号": row.get("计划编号", ""),
            "轮次序号": row.get("轮次序号"),
            "实际入口": row.get("实际入口", ""),
            "轮次状态": row.get("轮次状态"),
            "回执状态": row.get("回执状态", RECEIPT_NONE),
            "批次编号": batch_id,
            "批次数量": row.get("批次数量", 0),
            "批次可查": stored is not None,
            "批次校验摘要": stored.get("校验摘要", "") if stored else "",
            "批次状态": stored.get("status", "") if stored else "",
            "计划时间": row.get("计划时间", ""),
            "计划状态": row.get("计划状态", ""),
            "Cron表达式": row.get("Cron表达式", ""),
            "时区设置": row.get("时区设置", ""),
            "领取时间": row.get("领取时间", ""),
            "完成时间": row.get("完成时间", ""),
            "回执序号": row.get("回执序号", 0),
            "领取次数": row.get("领取次数", 0),
            "迟到结果": list(row.get("迟到结果", [])),
        }


def job_execution_summary(job_id: str) -> dict[str, Any]:
    rounds = [round_summary(str(row.get("轮次"))) for row in rounds_for_job(job_id)]
    batches = batches_for_job(job_id)
    return {
        "关联作业": job_id,
        "轮次数": len(rounds),
        "轮次": [item for item in rounds if item],
        "批次": [{"批次编号": str(b.get("批次编号")), "数据行数": b.get("数据行数"),
                  "状态": b.get("status")} for b in batches],
        "批次总数": len(batches),
    }


def ledger_snapshot() -> dict[str, Any]:
    rows = list(_rows())
    return {
        "module": MODULE,
        "entity": ENTITY,
        "count": len(rows),
        "by_status": {
            status: sum(1 for row in rows if row.get("轮次状态") == status)
            for status in ROUND_STATES
        },
        "receipt_interrupted": sum(1 for row in rows
                                   if row.get("回执状态") == RECEIPT_INTERRUPTED),
    }


# ---------------------------------------------------------------------------
# 资源服务：让台账也能走统一查询接口
# ---------------------------------------------------------------------------

class CrawlExecutionService:
    MODULE = MODULE
    ENTITY = ENTITY
    LIST_FIELDS = ["轮次", "关联作业", "计划编号", "实际入口", "批次编号",
                   "批次数量", "轮次状态", "计划时间"]
    REQUIRED_FIELDS = ["关联作业"]
    STATUS_ORDER = list(ROUND_STATES)
    ACTION_RULES = {"确认完成": ROUND_COMPLETED}
    NEGATIVE_ACTIONS: list[str] = []

    def list_entries(self, *, filters: dict[str, str] | None = None,
                     keyword: str | None = None, status: str | None = None,
                     page: int = 1, size: int = 20) -> tuple[list[dict[str, Any]], int]:
        rows = list(_rows())
        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                rows = [row for row in rows
                        if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows
                    if keyword.casefold() in str(row.get(self.LIST_FIELDS[0], "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("轮次状态") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        job_id = str(values.get("关联作业") or "").strip()
        if not job_id:
            return None, ["关联作业"]
        claimed, _ = claim_next(job_id)
        if claimed is None:
            return None, ["关联作业"]
        return find_round(str(claimed["轮次"])), []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        row = store.find(MODULE, entry_id)
        if row is None:
            return None, f"{ENTITY} {entry_id} 不存在或已归档"
        round_key = str(row.get("轮次"))
        if action == "确认完成":
            confirm_round(round_key, deliver=True, receipt={"来源": "资源接口动作"})
            return row, "采集轮次已确认完成"
        if action == "补跑轮次":
            claimed, message = resume_round(str(row.get("关联作业")), round_key)
            found = find_round(str(claimed["轮次"])) if claimed else None
            return found, message
        return None, f"动作「{action}」不属于{MODULE}可执行范围"

    def domain_snapshot(self) -> dict[str, Any]:
        return ledger_snapshot()
