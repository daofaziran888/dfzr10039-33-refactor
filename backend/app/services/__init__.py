"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.crawl_site import CrawlSiteService
from app.services.proxy_pool import ProxyPoolService
from app.services.anti_block import AntiBlockService
from app.services.crawl_job import CrawlJobService
from app.services.url_dedup import UrlDedupService
from app.services.content_extract import ContentExtractService
from app.services.field_clean import FieldCleanService
from app.services.batch_store import BatchStoreService
from app.services.incr_crawl import IncrCrawlService
from app.services.parse_log import ParseLogService
from app.services.data_delivery import DataDeliveryService
from app.services.quality_sample import QualitySampleService
from app.services.site_monitor import SiteMonitorService
from app.services.robot_verify import RobotVerifyService
from app.services.crawl_limit import CrawlLimitService
from app.services.template_lib import TemplateLibService
from app.services.scheduler import SchedulerService
from app.services.crawl_execution import CrawlExecutionService
from app.services.crawl_cost import CrawlCostService
from app.services.data_archive import DataArchiveService
from app.services.diff_check import DiffCheckService
from app.services.export_task import ExportTaskService
from app.services.audit_log import AuditLogService

SERVICES: dict[str, BaseModuleService] = {
    "crawl_site": CrawlSiteService(),
    "proxy_pool": ProxyPoolService(),
    "anti_block": AntiBlockService(),
    "crawl_job": CrawlJobService(),
    "url_dedup": UrlDedupService(),
    "content_extract": ContentExtractService(),
    "field_clean": FieldCleanService(),
    "batch_store": BatchStoreService(),
    "incr_crawl": IncrCrawlService(),
    "parse_log": ParseLogService(),
    "data_delivery": DataDeliveryService(),
    "quality_sample": QualitySampleService(),
    "site_monitor": SiteMonitorService(),
    "robot_verify": RobotVerifyService(),
    "crawl_limit": CrawlLimitService(),
    "template_lib": TemplateLibService(),
    "scheduler": SchedulerService(),
    "crawl_execution": CrawlExecutionService(),
    "crawl_cost": CrawlCostService(),
    "data_archive": DataArchiveService(),
    "diff_check": DiffCheckService(),
    "export_task": ExportTaskService(),
    "audit_log": AuditLogService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)
