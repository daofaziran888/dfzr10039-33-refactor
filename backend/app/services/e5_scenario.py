"""E5 采集轮次演练：把迟到执行中结果 / 完成回执中断 / 同轮补跑串成可回放的场景。

演练只编排现有领域能力：
- 计划执行派生数据来自 ``services.scheduling``；
- 轮次入口、批次和回执取自 ``services.crawl_execution``；
- 批次本身仍由 ``integration.run_crawl_pipeline`` 产出。

步骤固定、时钟固定，结果可重复核对。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.integration import run_crawl_pipeline
from app.services import crawl_execution as ledger
from app.services import scheduling
from app.services.scheduling import fixed_clock, parse_iso
from app.store import store


DEFAULT_DEMO_SITE = {
    "id": 9001, "status": "已验证", "pending": False, "abnormal": False,
    "站点编号": "CRAW-E5", "站点域名": "e5.example.com",
    "站点类型": "新闻门户", "Robots协议": "allow",
    "站点语言": "zh-CN", "地域归属": "CN", "接入方式": "direct",
    "注册时间": "2026-11-01",
}

DEFAULT_DEMO_EXTRACT = {
    "id": 9001, "status": "生效中", "pending": False, "abnormal": False,
    "规则编号": "CONT-E5", "目标站点": "e5.example.com",
    "XPath选择器": "//article", "字段映射": "title,body",
    "数据类型": "string", "清洗正则": "", "抽取顺序": "1",
    "规则状态": "生效中",
}

DEFAULT_DEMO_CLEAN = {
    "id": 9001, "status": "生效中", "pending": False, "abnormal": False,
    "规则编号": "FIEL-E5", "目标字段": "title",
    "清洗类型": "去空格", "处理表达式": "",
    "空值策略": "skip", "校验范围": "",
    "清洗顺序": "1", "规则状态": "生效中",
}


@dataclass
class E5Session:
    """E5 演练上下文：固定时钟、作业、暂扣的执行中结果。"""
    job_id: str
    base_time: str
    round_key: str = ""
    entry_url: str = ""
    original_batch_id: str = ""
    withheld_result: dict[str, Any] = field(default_factory=dict)
    receipt_payload: dict[str, Any] = field(default_factory=dict)
    steps: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "关联作业": self.job_id,
            "固定时钟": self.base_time,
            "轮次": self.round_key,
            "实际入口": self.entry_url,
            "原批次": self.original_batch_id,
            "暂扣结果": dict(self.withheld_result),
            "完成回执": dict(self.receipt_payload),
            "步骤": list(self.steps),
        }


def _job_row(job_id: str, entry_url: str) -> dict[str, Any]:
    return {
        "id": 9101, "status": "采集执行", "pending": True, "abnormal": False,
        "作业编号": job_id, "目标站点": "e5.example.com",
        "入口URL": entry_url, "最大深度": "1", "并发线程": "1",
        "调度策略": "cron", "完成页数": "0", "失败页数": "0",
    }


def _ensure_job(job_id: str, entry_url: str) -> None:
    row = ledger.job_row(job_id)
    if row is None:
        store.rows("crawl_job").append(_job_row(job_id, entry_url))
    else:
        row["入口URL"] = entry_url
        row["status"] = "采集执行"


def _step(session: E5Session, name: str, detail: dict[str, Any]) -> None:
    session.steps.append({"步骤": name, **detail})


def run_e5(job_id: str = "CRAW-E5-DEMO", entry_url: str = "https://e5.example.com/list",
           base_time: datetime | str = "2026-11-23T10:00:00+08:00",
           with_plan: bool = True) -> E5Session:
    """回放 E5：暂扣执行中结果 → 原批次落盘 → 完成回执断开 → 重送旧结果 → 同轮补跑。

    with_plan=True 时先确认计划，让台账拿到计划执行派生数据；
    之后再由调用方触发下一轮，核对它使用自己的入口正常落批。
    """
    session = E5Session(job_id=job_id, base_time=str(base_time))
    _ensure_job(job_id, entry_url)

    with fixed_clock(base_time):
        clock = parse_iso(base_time)
        plan_derived: dict[str, Any] = {}
        if with_plan:
            _, derived, created = scheduling.register_plan(
                job_id, "采集执行", now=clock)
            plan_derived = derived.as_dict()
            _step(session, "确认计划", {"计划编号": derived.plan_id,
                                        "计划时间": derived.execution_time,
                                        "计划状态": derived.plan_status,
                                        "新建": created})

        # 1) 领取本轮实际入口，并把计划执行派生数据回写到台账。
        claim, claim_msg = ledger.claim_next(job_id, plan_derived=plan_derived, now=clock)
        assert claim is not None, claim_msg
        session.round_key = str(claim["轮次"])
        session.entry_url = str(claim["实际入口"])
        _step(session, "领取实际入口", {"轮次": session.round_key,
                                         "实际入口": session.entry_url,
                                         "计划编号": claim.get("计划编号"),
                                         "计划时间": claim.get("计划时间"),
                                         "计划状态": claim.get("计划状态")})

        # 2) 已产出批次并确认完成，但完成回执在返回前中断（deliver=False）。
        batch_id = f"BATCH-{job_id}-E5-{session.round_key.rsplit('-', 1)[-1]}"
        session.receipt_payload = {
            "轮次": session.round_key,
            "批次编号": batch_id,
            "确认结果": "完成",
        }
        # 暂扣这一轮的执行中结果：先不投递给调度。
        session.withheld_result = {
            "轮次": session.round_key,
            "关联作业": job_id,
            "实际入口": session.entry_url,
            "批次编号": batch_id,
            "数据行数": 2,
            "结果状态": "执行中",
        }
        _step(session, "暂扣执行中结果", {"暂扣批次": batch_id})

        # 3) 原批次落盘：本轮只产出一个批次，并挂到入口上。
        batch = run_crawl_pipeline(
            site_row=DEFAULT_DEMO_SITE,
            job_row=_job_row(job_id, entry_url),
            extract_rows=[DEFAULT_DEMO_EXTRACT],
            clean_rows=[DEFAULT_DEMO_CLEAN],
            entry_urls=[session.entry_url],
            batch_id=batch_id,
            now=clock,
        )
        assert batch is not None, "原批次未产出"
        session.original_batch_id = batch.batch_id
        ledger.attach_batch(session.round_key, batch)
        _step(session, "原批次落盘", {"批次编号": batch.batch_id,
                                      "数据行数": batch.row_count,
                                      "批次状态": batch.status})

        session.receipt_payload["数据行数"] = batch.row_count
        ledger.confirm_round(session.round_key, deliver=False,
                             receipt=session.receipt_payload)
        interrupted = ledger.round_summary(session.round_key) or {}
        _step(session, "断开完成回执", {"回执批次": batch.batch_id,
                                        "轮次状态": interrupted.get("轮次状态"),
                                        "回执状态": interrupted.get("回执状态")})

        # 4) 重送迟到的执行中结果：同轮已完成，结果只留痕，不覆盖批次与完成。
        late = ledger.apply_inflight_result(job_id, session.withheld_result)
        _step(session, "重送迟到结果", {"是否回填": late["applied"],
                                        "批次编号": late["batch_id"],
                                        "原因": late["reason"]})

        # 5) 值班员对同一轮发出补跑：实际领取原入口，并查回已落地批次。
        resumed, resume_msg = ledger.resume_round(job_id, session.round_key)
        assert resumed is not None, resume_msg
        resume_view = ledger.round_summary(session.round_key) or {}
        _step(session, "同轮补跑", {"轮次": resumed["轮次"],
                                    "实际入口": resumed["实际入口"],
                                    "批次编号": resumed["批次编号"],
                                    "批次可查": resume_view.get("批次可查"),
                                    "补跑": resumed["补跑"],
                                    "轮次状态": resume_view.get("轮次状态")})

        # 6) 完成回执恢复：重放只增长回执序号，原批次仍可查。
        ledger.deliver_receipt(session.round_key, session.receipt_payload)
        summary = ledger.round_summary(session.round_key) or {}
        _step(session, "完成回执恢复", {"轮次状态": summary.get("轮次状态"),
                                        "回执状态": summary.get("回执状态"),
                                        "批次编号": summary.get("批次编号"),
                                        "回执序号": summary.get("回执序号")})

    return session


def run_next_round(session: E5Session, *,
                   later_time: datetime | str = "2026-11-23T16:00:00+08:00") -> dict[str, Any]:
    """完成回执确认后触发下一轮：必须用自己的入口，正常落批。"""
    with fixed_clock(later_time):
        clock = parse_iso(later_time)
        claim, claim_msg = ledger.claim_next(session.job_id, now=clock)
        assert claim is not None, claim_msg
        next_round = str(claim["轮次"])
        next_entry = str(claim["实际入口"])

        batch = run_crawl_pipeline(
            site_row=DEFAULT_DEMO_SITE,
            job_row=_job_row(session.job_id, claim.get("实际入口") or next_entry),
            extract_rows=[DEFAULT_DEMO_EXTRACT],
            clean_rows=[DEFAULT_DEMO_CLEAN],
            entry_urls=[next_entry],
            batch_id=f"BATCH-{session.job_id}-{next_round.rsplit('-', 1)[-1]}",
            now=clock,
        )
        assert batch is not None, "下一轮批次未产出"
        ledger.attach_batch(next_round, batch)
        ledger.confirm_round(next_round, deliver=True,
                             receipt={"轮次": next_round, "批次编号": batch.batch_id,
                                      "确认结果": "完成"})
        summary = ledger.round_summary(next_round) or {}
        _step(session, "下一轮落批", {"轮次": next_round, "实际入口": next_entry,
                                      "批次编号": batch.batch_id})
        return {
            "轮次": next_round,
            "实际入口": next_entry,
            "上一轮入口": session.entry_url,
            "新入口": next_entry != session.entry_url,
            "批次编号": batch.batch_id,
            "轮次状态": summary.get("轮次状态"),
        }


def e5_report(session: E5Session) -> dict[str, Any]:
    original = ledger.round_summary(session.round_key) or {}
    return {
        "演练": session.as_dict(),
        "原轮": original,
        "批次": ledger.job_execution_summary(session.job_id),
        "台账": ledger.ledger_snapshot(),
    }
