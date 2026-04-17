# Changelog

本项目的所有重要变更都会记录在这里。

格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

## [0.1.0] - 2026-04-17

首次开源发布。

### 新增

- 分层架构：`UI -> Service -> Host -> Agent`，边界稳定。
- Engine：Runner / Agent 事件流、状态机、ToolTrace、截断与压缩管理、SSE 解析。
- Fins：财报 capability 定位、两条执行路径、文件系统仓储实现。
- 财报数据管线：SEC 10-K / 10-Q / 20-F 下载与预处理，XBRL 与 HTML 双路径提取。
- CLI 入口 `dayu`（`python -m dayu.cli` 等价）：`prompt`、`interactive`、`download`、`write` 四类工作流。
- 配置系统：默认配置 + `workspace/config/` 覆盖，prompt 模板可插拔。
- Web 骨架：FastAPI 路由与应用装配。
- WeChat 入口：iLink 文本消息首版。
- 渲染：Markdown -> HTML / PDF / Word。
- 文档：用户手册（根 README）、开发总览（`dayu/README.md`）、Engine / Fins / Config / Tests 分册手册、贡献指南。

### 已知限制

- A 股、港股财报下载未实现。
- GUI 未实现；Web UI 仅有骨架。
- 财报电话会议音频转录后的问答区分未实现。
- 定性分析模板对不同公司的差异化判断路径仍偏机械。

[Unreleased]: https://github.com/noho/dayu-agent/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/noho/dayu-agent/releases/tag/v0.1.0
