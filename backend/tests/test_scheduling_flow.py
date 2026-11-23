"""调度执行派生数据与 E5 轮次台账测试：固定时钟、旧计划兼容、迟到结果与补跑。"""
from __future__ import annotations

import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.integration import run_crawl_pipeline, schedule_job
from app.models import CrawlJobSpec, SchedulePlan, SiteConfig, SiteType
from app.services import crawl_execution as ledger
from app.services import e5_scenario, scheduling
from app.store import store

FIXED = "2026-11-23T10:00:00+08:00"


# ---------------------------------------------------------------------------
# 隔离：每个用例前备份涉及的模块表，结束后恢复
# ---------------------------------------------------------------------------

TABLES = ("scheduler", "crawl_job", "crawl_execution", "batch_store", "parse_log")


@pytest.fixture(autouse=True)
def isolate_store():
    saved = {name: copy.deepcopy(store.rows(name)) for name in TABLES}
    scheduling.set_clock(None)
    try:
        yield
    finally:
        for name, rows in saved.items():
            table = store.rows(name)
            table.clear()
            table.extend(rows)
        scheduling.set_clock(None)


def _site_row() -> dict:
    return {
        "id": 101, "status": "已验证", "pending": False, "abnormal": False,
        "站点编号": "CRAW-SCH", "站点域名": "sched.example.com",
        "站点类型": "新闻门户", "Robots协议": "allow",
        "站点语言": "zh-CN", "地域归属": "CN", "接入方式": "direct",
        "注册时间": "2026-11-01",
    }


def _job_row(job_id: str, status: str = "待执行") -> dict:
    return {
        "id": 201, "status": status, "pending": True, "abnormal": False,
        "作业编号": job_id, "目标站点": "sched.example.com",
        "入口URL": "https://sched.example.com/list", "最大深度": "1",
        "并发线程": "1", "调度策略": "cron", "完成页数": "0", "失败页数": "0",
    }


def _extract_rows() -> list[dict]:
    return [{
        "id": 301, "status": "生效中", "pending": False, "abnormal": False,
        "规则编号": "CONT-SCH", "目标站点": "sched.example.com",
        "XPath选择器": "//article", "字段映射": "title", "数据类型": "string",
        "清洗正则": "", "抽取顺序": "1", "规则状态": "生效中",
    }]


def _clean_rows() -> list[dict]:
    return [{
        "id": 401, "status": "生效中", "pending": False, "abnormal": False,
        "规则编号": "FIEL-SCH", "目标字段": "title", "清洗类型": "去空格",
        "处理表达式": "", "空值策略": "skip", "校验范围": "",
        "清洗顺序": "1", "规则状态": "生效中",
    }]


# ---------------------------------------------------------------------------
# 共用调度计算：复用旧计划 / 首次登记
# ---------------------------------------------------------------------------

def test_reuse_and_new_plan_share_one_calculation() -> None:
    """复用和新建走同一份派生计算，计划时间、状态完全一致。"""
    with scheduling.fixed_clock(FIXED):
        reused_derived = scheduling.derive_for_plan_row(
            {"计划编号": "SCHE-OLD", "关联作业": "JOB-OLD",
             "Cron表达式": "0 */6 * * *", "时区设置": "Asia/Shanghai"},
            "待执行",
        )
        new_derived = scheduling.derive_for_new_plan("JOB-NEW", "待执行")

    assert reused_derived.execution_time == new_derived.execution_time
    assert reused_derived.plan_status == new_derived.plan_status == "执行中"


def test_existing_plan_keeps_id_cron_and_timezone() -> None:
    """兼容已有计划：编号、Cron、时区原样沿用。"""
    store.rows("scheduler").append({
        "id": 50, "status": "待激活", "pending": True, "abnormal": False,
        "计划编号": "SCHE-LEGACY", "关联作业": "JOB-LEGACY",
        "Cron表达式": "30 4 * * 1", "时区设置": "UTC",
        "依赖检查": "前置批次", "失败通知": "值班群",
        "并发控制": "1", "下次执行": "旧时间",
    })

    with scheduling.fixed_clock(FIXED):
        row, derived, created = scheduling.register_plan("JOB-LEGACY", "已完成")

    assert created is False, "已有计划应复用，不应新建"
    assert derived.plan_id == "SCHE-LEGACY"
    assert derived.cron_expr == "30 4 * * 1"
    assert derived.timezone_setting == "UTC"
    assert row["依赖检查"] == "前置批次", "复用不应丢失已有配置"
    assert row["失败通知"] == "值班群"
    assert row["status"] == "已完成"
    assert row["下次执行"] == derived.execution_time
    # UTC 时区下，Cron 30 4 * * 1 的下一次执行应为下一个周一 04:30 UTC。
    assert derived.execution_time == "2026-11-23T04:30:00+00:00"


def test_first_registration_uses_defaults() -> None:
    """首次登记沿用默认配置：SCHED-作业 编号、每6小时、Asia/Shanghai。"""
    with scheduling.fixed_clock(FIXED):
        _, derived, created = scheduling.register_plan("JOB-FIRST", "待执行")

    rows = [row for row in store.rows("scheduler") if row["关联作业"] == "JOB-FIRST"]
    assert created is True
    assert len(rows) == 1
    assert rows[0]["计划编号"] == "SCHED-JOB-FIRST"
    assert derived.cron_expr == "0 */6 * * *"
    assert derived.timezone_setting == "Asia/Shanghai"
    assert derived.execution_time == "2026-11-23T12:00:00+08:00"


def test_preview_leaves_no_plan_row() -> None:
    """试算只读：不会留下半条计划，中断后重试仍然从干净状态登记。"""
    before = len(store.rows("scheduler"))
    with scheduling.fixed_clock(FIXED):
        derived = scheduling.preview_plan("JOB-TRIAL", "待执行")
        # 模拟试算中断：不确认，直接再试一次
        scheduling.preview_plan("JOB-TRIAL", "待执行")
        assert len(store.rows("scheduler")) == before
        row, confirmed, created = scheduling.register_plan("JOB-TRIAL", "待执行")

    assert created is True
    assert row["计划编号"] == derived.plan_id
    assert len([r for r in store.rows("scheduler") if r["关联作业"] == "JOB-TRIAL"]) == 1


def test_same_job_registers_only_one_association() -> None:
    """同一作业重复登记只保留一条关联，重复登记只刷新派生数据。"""
    with scheduling.fixed_clock(FIXED):
        first, _, created_first = scheduling.register_plan("JOB-DUP", "待执行")
        second, _, created_second = scheduling.register_plan("JOB-DUP", "已完成")

    rows = [row for row in store.rows("scheduler") if row["关联作业"] == "JOB-DUP"]
    assert created_first is True
    assert created_second is False
    assert len(rows) == 1, "同一作业不能多登记一条关联"
    assert first["id"] == second["id"]
    assert second["status"] == "已完成"


def test_schedule_job_receipt_fields_unchanged() -> None:
    """计划回执字段不变：schedule_job 仍返回原 SchedulePlan 字段集合。"""
    store.rows("crawl_job").append(_job_row("JOB-RECEIPT", "待执行"))
    job = CrawlJobSpec.from_seed_row(
        _job_row("JOB-RECEIPT"), site=SiteConfig(
            site_id="S", domain="sched.example.com", site_type=SiteType.NEWS,
            robots_policy="allow", language="zh-CN", region="CN",
            access_method="direct", registered_at="2026-11-01", verified=True,
        ))

    with scheduling.fixed_clock(FIXED):
        plan = schedule_job(job)

    assert isinstance(plan, SchedulePlan)
    assert plan.plan_id == "SCHED-JOB-RECEIPT"
    assert plan.linked_job_id == "JOB-RECEIPT"
    assert plan.cron_expr == "0 */6 * * *"
    assert plan.timezone_setting == "Asia/Shanghai"
    assert plan.next_run == "2026-11-23T12:00:00+08:00"
    assert plan.status == "执行中"
    assert set(plan.__dict__) == {
        "plan_id", "linked_job_id", "cron_expr", "timezone_setting",
        "dependency_check", "failure_notify", "concurrency_control",
        "next_run", "status",
    }


def test_fixed_clock_handles_pending_completed_and_no_plan() -> None:
    """固定时钟：旧计划待执行/已完成作业、无计划新作业分别核对。"""
    store.rows("crawl_job").extend([
        _job_row("JOB-WAIT", "待执行"),
        _job_row("JOB-DONE", "已完成"),
        _job_row("JOB-NONE", "采集执行"),
    ])
    store.rows("scheduler").extend([
        {"id": 61, "status": "待激活", "pending": True, "abnormal": False,
         "计划编号": "SCHE-WAIT", "关联作业": "JOB-WAIT", "Cron表达式": "0 9 * * *",
         "时区设置": "Asia/Shanghai", "依赖检查": "", "失败通知": "",
         "并发控制": "", "下次执行": ""},
        {"id": 62, "status": "执行中", "pending": True, "abnormal": True,
         "计划编号": "SCHE-DONE", "关联作业": "JOB-DONE", "Cron表达式": "15 2 * * *",
         "时区设置": "Asia/Shanghai", "依赖检查": "", "失败通知": "",
         "并发控制": "", "下次执行": ""},
    ])

    before = len(store.rows("scheduler"))
    with scheduling.fixed_clock(FIXED):
        wait_row, wait_derived, wait_created = scheduling.register_plan("JOB-WAIT", "待执行")
        done_row, done_derived, done_created = scheduling.register_plan("JOB-DONE", "已完成")
        none_row, none_derived, none_created = scheduling.register_plan("JOB-NONE", "采集执行")

    assert wait_created is False and done_created is False
    assert wait_derived.execution_time == "2026-11-24T09:00:00+08:00"
    assert done_derived.execution_time == "2026-11-24T02:15:00+08:00"
    assert wait_derived.plan_status == "执行中"
    assert done_derived.plan_status == "已完成"
    assert none_derived.plan_status == "执行中"
    assert none_derived.cron_expr == "0 */6 * * *"

    assert len(store.rows("scheduler")) == before + 1, "只有无计划作业新增登记行"
    assert wait_row["计划编号"] == "SCHE-WAIT"
    assert done_row["计划编号"] == "SCHE-DONE"
    assert none_row["关联作业"] == "JOB-NONE"
    assert none_row["下次执行"] == "2026-11-23T12:00:00+08:00"


# ---------------------------------------------------------------------------
# 台账：计划派生数据回写与入口领取
# ---------------------------------------------------------------------------

def test_claim_writes_plan_derived_to_ledger() -> None:
    """计划确认结果回写到采集任务执行台账，采集任务据此认领实际入口。"""
    store.rows("crawl_job").append(_job_row("JOB-CLAIM", "采集执行"))
    with scheduling.fixed_clock(FIXED):
        _, derived, _ = scheduling.register_plan("JOB-CLAIM", "采集执行")
        claimed, message = ledger.claim_next("JOB-CLAIM")
        summary = ledger.round_summary(str(claimed["轮次"]))

    assert claimed["计划编号"] == derived.plan_id
    assert claimed["实际入口"] == "https://sched.example.com/list?round=1"
    assert summary["计划时间"] == derived.execution_time
    assert summary["计划状态"] == "执行中"
    assert summary["Cron表达式"] == "0 */6 * * *"
    assert summary["时区设置"] == "Asia/Shanghai"
    assert "实际入口" in message


def test_claim_without_plan_uses_job_entry() -> None:
    """无计划的新作业也能领取入口，台账计划字段留空。"""
    store.rows("crawl_job").append(_job_row("JOB-NOPLAN", "待执行"))
    with scheduling.fixed_clock(FIXED):
        claimed, _ = ledger.claim_next("JOB-NOPLAN")

    assert claimed is not None
    assert claimed["计划编号"] == ""
    assert claimed["实际入口"].startswith("https://sched.example.com/list")


# ---------------------------------------------------------------------------
# E5：迟到结果 / 完成回执中断 / 同轮补跑 / 下一轮
# ---------------------------------------------------------------------------

def _produce_batch(job_id: str, entry_url: str, batch_id: str) -> object:
    batch = run_crawl_pipeline(
        site_row=_site_row(), job_row=_job_row(job_id, "采集执行"),
        extract_rows=_extract_rows(), clean_rows=_clean_rows(),
        entry_urls=[entry_url], batch_id=batch_id,
    )
    assert batch is not None
    return batch


def test_e5_late_result_does_not_override_completed_round() -> None:
    """迟到执行中结果不能覆盖同轮完成，批次保持原样。"""
    store.rows("crawl_job").append(_job_row("JOB-E5", "采集执行"))
    with scheduling.fixed_clock(FIXED):
        claim, _ = ledger.claim_next("JOB-E5")
        round_key = str(claim["轮次"])
        entry = str(claim["实际入口"])
        original = _produce_batch("JOB-E5", entry, "BATCH-JOB-E5-0001")
        ledger.attach_batch(round_key, original)
        ledger.confirm_round(round_key, deliver=False,
                             receipt={"轮次": round_key, "批次编号": original.batch_id})

        late = ledger.apply_inflight_result("JOB-E5", {
            "轮次": round_key, "实际入口": entry,
            "批次编号": "BATCH-LATE-DIFFERENT", "数据行数": 99,
        })
        summary = ledger.round_summary(round_key)

    assert late["applied"] is False
    assert summary["轮次状态"] == "已完成"
    assert summary["批次编号"] == "BATCH-JOB-E5-0001"
    assert summary["批次数量"] == original.row_count
    assert summary["批次可查"] is True
    assert summary["迟到结果"], "迟到结果应留痕"


def test_e5_inflight_result_before_completion_is_applied() -> None:
    """对照：仍在执行中的轮次可以接收执行中结果。"""
    store.rows("crawl_job").append(_job_row("JOB-E5-OPEN", "采集执行"))
    with scheduling.fixed_clock(FIXED):
        claim, _ = ledger.claim_next("JOB-E5-OPEN")
        round_key = str(claim["轮次"])
        resolution = ledger.apply_inflight_result("JOB-E5-OPEN", {
            "轮次": round_key, "实际入口": claim["实际入口"],
            "批次编号": "BATCH-INFLIGHT-0001", "数据行数": 2,
        })
        summary = ledger.round_summary(round_key)

    assert resolution["applied"] is True
    assert summary["批次编号"] == "BATCH-INFLIGHT-0001"
    assert summary["轮次状态"] == "执行中"


def test_e5_resume_claims_entry_and_finds_batch() -> None:
    """补跑实际领取采集入口，并能查回本轮已经落地的批次。"""
    store.rows("crawl_job").append(_job_row("JOB-E5-RESUME", "采集执行"))
    with scheduling.fixed_clock(FIXED):
        claim, _ = ledger.claim_next("JOB-E5-RESUME")
        round_key = str(claim["轮次"])
        entry = str(claim["实际入口"])
        batch = _produce_batch("JOB-E5-RESUME", entry, "BATCH-E5-RESUME-0001")
        ledger.attach_batch(round_key, batch)
        ledger.confirm_round(round_key, deliver=False)

        resumed, message = ledger.resume_round("JOB-E5-RESUME", round_key)
        summary = ledger.round_summary(round_key)

    assert resumed is not None
    assert resumed["实际入口"] == entry, "补跑应领取同一轮的实际入口"
    assert resumed["补跑"] is True
    assert resumed["批次编号"] == "BATCH-E5-RESUME-0001"
    assert summary["批次可查"] is True
    assert summary["批次状态"] == "校验通过"
    assert str(round_key) in message


def test_e5_completion_does_not_block_next_round() -> None:
    """这次完成（即使回执中断）不拦下一轮，下一轮用自己的入口正常落批。"""
    store.rows("crawl_job").append(_job_row("JOB-E5-NEXT", "采集执行"))
    with scheduling.fixed_clock(FIXED):
        first, _ = ledger.claim_next("JOB-E5-NEXT")
        first_round = str(first["轮次"])
        first_entry = str(first["实际入口"])
        batch = _produce_batch("JOB-E5-NEXT", first_entry, "BATCH-E5-NEXT-0001")
        ledger.attach_batch(first_round, batch)
        ledger.confirm_round(first_round, deliver=False)

        second, _ = ledger.claim_next("JOB-E5-NEXT")
        second_round = str(second["轮次"])
        second_entry = str(second["实际入口"])
        next_batch = _produce_batch("JOB-E5-NEXT", second_entry, "BATCH-E5-NEXT-0002")
        ledger.attach_batch(second_round, next_batch)
        ledger.confirm_round(second_round, deliver=True)

        first_summary = ledger.round_summary(first_round)
        second_summary = ledger.round_summary(second_round)
        execution = ledger.job_execution_summary("JOB-E5-NEXT")

    assert second_round != first_round
    assert second_entry != first_entry, "下一轮应使用自己的入口"
    assert second_summary["批次编号"] == "BATCH-E5-NEXT-0002"
    assert second_summary["批次可查"] is True
    assert first_summary["批次编号"] == "BATCH-E5-NEXT-0001"
    assert first_summary["批次可查"] is True
    assert execution["批次总数"] == 2, "每轮只产出一个批次"


def test_e5_duplicate_rule_does_not_persist_second_batch_row() -> None:
    """同一轮重复落批不会登记第二行，批次可查且唯一。"""
    store.rows("crawl_job").append(_job_row("JOB-E5-DUP", "采集执行"))
    with scheduling.fixed_clock(FIXED):
        claim, _ = ledger.claim_next("JOB-E5-DUP")
        entry = str(claim["实际入口"])
        _produce_batch("JOB-E5-DUP", entry, "BATCH-E5-DUP-0001")
        _produce_batch("JOB-E5-DUP", entry, "BATCH-E5-DUP-0001")
        matches = [row for row in store.rows("batch_store")
                   if row["批次编号"] == "BATCH-E5-DUP-0001"]

    assert len(matches) == 1


def test_e5_full_scenario_replay() -> None:
    """完整 E5 回放：原轮只产一次，补跑可查回，下一轮正常落批。"""
    session = e5_scenario.run_e5(job_id="JOB-E5-FULL")
    next_info = e5_scenario.run_next_round(session)
    report = e5_scenario.e5_report(session)

    steps = {step["步骤"]: step for step in session.steps}
    assert steps["重送迟到结果"]["是否回填"] is False
    assert steps["断开完成回执"]["轮次状态"] == "已完成"
    assert steps["断开完成回执"]["回执状态"] == "中断"
    assert steps["同轮补跑"]["实际入口"] == session.entry_url
    assert steps["同轮补跑"]["批次可查"] is True
    assert steps["完成回执恢复"]["回执状态"] == "已回执"

    assert report["原轮"]["轮次状态"] == "已完成"
    assert report["原轮"]["批次编号"] == session.original_batch_id
    assert report["批次"]["批次总数"] == 2, "原轮与下一轮各一个批次"
    assert next_info["新入口"] is True
    assert next_info["轮次状态"] == "已完成"

    original_batches = [b for b in report["批次"]["批次"]
                        if b["批次编号"] == session.original_batch_id]
    assert len(original_batches) == 1, "原轮只产出一次"
