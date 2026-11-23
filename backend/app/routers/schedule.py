"""调度执行接口：计划试算/确认、采集轮次领取/回执/补跑，以及 E5 演练。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.integration import run_crawl_pipeline
from app.services import crawl_execution as ledger
from app.services import e5_scenario, scheduling
from app.services.scheduling import fixed_clock, parse_iso, set_clock

router = APIRouter(prefix="/api/schedule", tags=["调度执行"])


class PreviewPayload(BaseModel):
    job_id: str = Field(..., description="关联作业编号")
    job_status: str = "待执行"
    plan_id: str | None = None
    cron_expr: str | None = None
    timezone_setting: str | None = None


class ConfirmPayload(BaseModel):
    job_id: str
    job_status: str = "待执行"
    plan_id: str | None = None
    cron_expr: str | None = None
    timezone_setting: str | None = None
    clock: str | None = None


class ClaimPayload(BaseModel):
    job_id: str
    clock: str | None = None


class ResumePayload(BaseModel):
    job_id: str
    round_key: str | None = None
    clock: str | None = None


class InflightPayload(BaseModel):
    job_id: str
    round_key: str | None = None
    batch_id: str | None = None
    entry_url: str | None = None
    row_count: int = 0
    received_at: str | None = None


class ReceiptPayload(BaseModel):
    round_key: str
    batch_id: str | None = None
    interrupt: bool = False
    receipt: dict[str, Any] = Field(default_factory=dict)


class ProducePayload(BaseModel):
    round_key: str
    batch_id: str | None = None
    clock: str | None = None


class ClockPayload(BaseModel):
    clock: str | None = None


class E5Payload(BaseModel):
    job_id: str = "CRAW-E5-DEMO"
    entry_url: str = "https://e5.example.com/list"
    base_time: str = "2026-11-23T10:00:00+08:00"


@router.get("/plan/preview")
def preview_plan(job_id: str = Query(...), job_status: str = "待执行",
                 plan_id: str | None = None, cron_expr: str | None = None,
                 timezone_setting: str | None = None) -> dict[str, Any]:
    """只试算不落库：中断或重试都不会留下半条计划。"""
    try:
        derived = scheduling.preview_plan(
            job_id, job_status, plan_id=plan_id,
            cron_expr=cron_expr, timezone_setting=timezone_setting)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "derived": derived.as_dict(),
            "ledger_rows": len(scheduling.store.rows(scheduling.MODULE))}


@router.post("/plan/confirm")
def confirm_plan(payload: ConfirmPayload) -> dict[str, Any]:
    """计划确认：原子登记（同作业只一条关联），并把派生数据回写到台账。"""
    now = parse_iso(payload.clock) if payload.clock else None
    try:
        with fixed_clock(payload.clock) if payload.clock else _null_clock():
            row, derived, created = scheduling.register_plan(
                payload.job_id, payload.job_status,
                plan_id=payload.plan_id, cron_expr=payload.cron_expr,
                timezone_setting=payload.timezone_setting, now=now)
            # 把计划确认结果回写到该作业当前轮次（若有），否则留待领取时写入。
            context = derived.as_dict()
            latest = ledger.find_round_for_job(payload.job_id)
            if latest is not None:
                ledger.write_plan_derived(latest, context)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "created": created, "plan": dict(row),
            "derived": derived.as_dict(),
            "ledger": ledger.round_summary(str(latest.get("轮次"))) if latest else None}


@router.post("/round/claim")
def claim_round(payload: ClaimPayload) -> dict[str, Any]:
    """领取下一轮实际入口；进行中的轮次复用入口，不会重复占用。"""
    now = parse_iso(payload.clock) if payload.clock else None
    claimed, message = ledger.claim_next(payload.job_id, now=now)
    if claimed is None:
        raise HTTPException(status_code=404, detail=message)
    return {"ok": True, "message": message, "claimed": claimed,
            "round": ledger.round_summary(str(claimed["轮次"]))}


@router.post("/round/produce")
def produce_round_batch(payload: ProducePayload) -> dict[str, Any]:
    """按本轮实际入口产出并落盘批次（E5 演练中的「原批次落盘」）。"""
    row = ledger.find_round(payload.round_key)
    if row is None:
        raise HTTPException(status_code=404, detail=f"轮次 {payload.round_key} 不存在")
    now = parse_iso(payload.clock) if payload.clock else None
    job_id = str(row.get("关联作业"))
    job = ledger.job_row(job_id) or {}
    batch = run_crawl_pipeline(
        site_row=e5_scenario.DEFAULT_DEMO_SITE,
        job_row={
            "id": 0, "status": "采集执行", "pending": True, "abnormal": False,
            "作业编号": job_id, "目标站点": job.get("目标站点", ""),
            "入口URL": str(row.get("实际入口")), "最大深度": "1",
            "并发线程": "1", "调度策略": "cron", "完成页数": "0", "失败页数": "0",
        },
        extract_rows=[e5_scenario.DEFAULT_DEMO_EXTRACT],
        clean_rows=[e5_scenario.DEFAULT_DEMO_CLEAN],
        entry_urls=[str(row.get("实际入口"))],
        batch_id=payload.batch_id or f"BATCH-{payload.round_key}",
        now=now,
    )
    if batch is None:
        raise HTTPException(status_code=409, detail="本轮批次未能产出")
    ledger.attach_batch(payload.round_key, batch)
    return {"ok": True, "batch_id": batch.batch_id, "row_count": batch.row_count,
            "batch_status": batch.status,
            "round": ledger.round_summary(payload.round_key)}


@router.post("/round/inflight")
def inflight_result(payload: InflightPayload) -> dict[str, Any]:
    """接收执行中结果：只可能回填到尚未确认完成的最近一轮。"""
    result = {
        "轮次": payload.round_key,
        "实际入口": payload.entry_url,
        "批次编号": payload.batch_id,
        "数据行数": payload.row_count,
    }
    decision = ledger.apply_inflight_result(
        payload.job_id, result, received_at=payload.received_at)
    return {"ok": True, "decision": decision,
            "round": ledger.round_summary(decision["round_key"])
            if decision.get("round_key") else None}


@router.post("/round/receipt")
def round_receipt(payload: ReceiptPayload) -> dict[str, Any]:
    """完成回执：interrupt=true 时完成已确认但回执中断，可稍后重放送达。"""
    row = ledger.find_round(payload.round_key)
    if row is None:
        raise HTTPException(status_code=404, detail=f"轮次 {payload.round_key} 不存在")
    receipt = {"轮次": payload.round_key, "批次编号": payload.batch_id,
               "确认结果": "完成", **payload.receipt}
    if payload.interrupt:
        ledger.confirm_round(payload.round_key, deliver=False, receipt=receipt)
        return {"ok": True, "interrupted": True,
                "round": ledger.round_summary(payload.round_key)}
    ledger.deliver_receipt(payload.round_key, receipt)
    return {"ok": True, "interrupted": False,
            "round": ledger.round_summary(payload.round_key)}


@router.post("/round/resume")
def resume_round(payload: ResumePayload) -> dict[str, Any]:
    """同一轮补跑：复用原入口与已落地批次。"""
    now = parse_iso(payload.clock) if payload.clock else None
    claimed, message = ledger.resume_round(
        payload.job_id, payload.round_key, now=now)
    if claimed is None:
        raise HTTPException(status_code=404, detail=message)
    return {"ok": True, "message": message, "claimed": claimed,
            "round": ledger.round_summary(str(claimed["轮次"]))}


@router.get("/round/{round_key}")
def get_round(round_key: str) -> dict[str, Any]:
    summary = ledger.round_summary(round_key)
    if summary is None:
        raise HTTPException(status_code=404, detail=f"轮次 {round_key} 不存在")
    return summary


@router.get("/job/{job_id}")
def get_job_execution(job_id: str) -> dict[str, Any]:
    return ledger.job_execution_summary(job_id)


@router.post("/clock")
def set_fixed_clock(payload: ClockPayload) -> dict[str, Any]:
    """固定本地时钟；clock 为空表示恢复系统时钟。"""
    value = set_clock(payload.clock)
    return {"ok": True, "fixed": scheduling.clock_is_fixed(),
            "now": value.isoformat() if value else None}


@router.post("/e5/run")
def run_e5(payload: E5Payload) -> dict[str, Any]:
    with fixed_clock(payload.base_time) if payload.base_time else _null_clock():
        session = e5_scenario.run_e5(
            job_id=payload.job_id, entry_url=payload.entry_url,
            base_time=payload.base_time)
        return {"ok": True, "report": e5_scenario.e5_report(session)}


@router.post("/e5/next-round")
def run_e5_next_round(payload: E5Payload) -> dict[str, Any]:
    with fixed_clock(payload.base_time) if payload.base_time else _null_clock():
        session = e5_scenario.run_e5(
            job_id=payload.job_id, entry_url=payload.entry_url,
            base_time=payload.base_time)
        next_info = e5_scenario.run_next_round(session)
        return {"ok": True, "next": next_info,
                "report": e5_scenario.e5_report(session)}

