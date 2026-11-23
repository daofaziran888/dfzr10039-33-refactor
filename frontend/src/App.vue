<template>
  <div class="app-shell">
    <aside class="app-side">
      <h1 class="app-title">虫巢工坊 · 分布式爬虫调度与反爬对抗平台</h1>
      <nav class="nav-list">
        <RouterLink v-for="item in navItems" :key="item.path" :to="item.path" class="nav-item">
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>
    <main class="app-main">
      <header class="app-head">
        <span class="head-desc">面向大规模网页采集的分布式爬虫调度、代理池管理、反爬策略对抗、内容抽取清洗与采集结果交付平台。</span>
        <span class="head-user">当前值班：数据工程组</span>
      </header>
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
const navItems = [{ label: "运营概览", path: "/" }, { label: "采集站点", path: "/crawl_site" }, { label: "代理池", path: "/proxy_pool" }, { label: "反爬对抗", path: "/anti_block" }, { label: "采集任务", path: "/crawl_job" }, { label: "URL去重", path: "/url_dedup" }, { label: "内容抽取", path: "/content_extract" }, { label: "字段清洗", path: "/field_clean" }, { label: "批次存储", path: "/batch_store" }, { label: "增量采集", path: "/incr_crawl" }, { label: "解析日志", path: "/parse_log" }, { label: "数据投递", path: "/data_delivery" }, { label: "质量抽样", path: "/quality_sample" }, { label: "站点监控", path: "/site_monitor" }, { label: "验证码识别", path: "/robot_verify" }, { label: "采集限速", path: "/crawl_limit" }, { label: "模板库", path: "/template_lib" }, { label: "调度编排", path: "/scheduler" }, { label: "调度执行", path: "/schedule_execution" }, { label: "采集执行台账", path: "/crawl_execution" }, { label: "采集计费", path: "/crawl_cost" }, { label: "数据归档", path: "/data_archive" }, { label: "差异检测", path: "/diff_check" }, { label: "导出任务", path: "/export_task" }, { label: "操作审计", path: "/audit_log" }]
</script>
