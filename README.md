# FlowBridge / LongFlow — 流式回复修复版

面向 **SillyTavern** 的 LongFlow 扩展，修复长回复在流式工具调用中可能出现的 **空回、丢字、异常截断** 等解析问题。

## 通过 GitHub 安装

1. 打开 SillyTavern → **扩展** → **安装扩展**。
2. 粘贴仓库地址：`https://github.com/chengcheng321/FlowBridge`
3. 安装完成后刷新页面，在扩展设置中启用 LongFlow。
4. 保持 **Streaming 流式传输**开启。排查问题时先关闭 **平滑显示**，再用长回复进行测试。

已安装旧版时，可在扩展管理中尝试更新，然后刷新网页；无法更新时请先备份原插件再重新安装。

## 改进点

- 更稳健的 SSE 流式事件边界解析（支持分片 CRLF）。
- 按工具调用索引分别拼接名称与参数，支持工具名分片。
- 读取并解析 `[DONE]`，处理工具参数未完整返回和上游连接中断。
- 尽量保留已接收的正文，输出可诊断提示以避免无提示空回。
- 保留上游 `length`、`content_filter` 等结束原因。
- 改进非流式响应的异常兜底和扩展禁用时的钩子恢复。

## 回退

修改前原始版本（v0.3.6）保存在分支：
[`backup/longflow-original-20261014-110224`](https://github.com/chengcheng321/FlowBridge/tree/backup/longflow-original-20261014-110224)

## 说明

此修复版已通过模拟流式分片测试，**尚未验证所有模型和 API 中转组合**。它无法补回模型或 API 上游没有实际生成/发送的文字。

请在浏览器开发者工具 Console 里查看 `[LongFlow]` 日志，帮助定位异常。
