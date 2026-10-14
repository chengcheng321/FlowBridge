FlowBridge / LongFlow 流式修复实验版

来自 https://github.com/chengcheng321/FlowBridge (v0.3.6) 的本地补丁。
已上传至此仓库测试分支 test/longflow-stream-fix-20261014-110224；并非经过实机验证的正式发布，建议保留原版备份。

修复重点：
1. 流式 SSE 消息边界，支持 CRLF 跨数据块、纯 CR、纯 LF。
2. 按工具调用的 index 独立拼接函数名称和 arguments，支持函数名分片。
3. 收到 [DONE] 后处理，不必持续等待连接关闭。
4. JSON 工具参数中途断开时尽量恢复 content 字符串并显示告警。
5. 没有可显示正文时给出可诊断错误，避免无提示空回。
6. 保留上游 length / content_filter 等结束原因，不统一伪装成 stop。
7. 非流式 resp.clone().json()，解析失败时保留可供消费的原响应。
8. 扩展禁用时尽量卸载 fetch hook，自动更新禁用避免补丁被覆盖。

获取测试版：打开 https://github.com/chengcheng321/FlowBridge/tree/test/longflow-stream-fix-20261014-110224
在 GitHub 页面点击 Code -> Download ZIP，解压后使用根目录的修复文件。
注意：直接通过仓库默认地址安装仍会获取 main 原版；测试分支需要手动切换或覆盖对应文件。

使用：先备份原插件文件，把本目录中的 index.js 覆盖现有插件的 index.js，
然后刷新 SillyTavern 网页。也可将整个目录放到 SillyTavern 的第三方扩展目录，
但不要和原版 LongFlow 同时启用，因为二者使用相同配置键和 hook 标记。

测试顺序：
* 启用流式。
* 禁用「平滑显示」排查显示效果（不影响工具参数的后台拼接）。
* 用以前会空回的长篇提示词连续测试 3 次。
* 出错时打开浏览器 F12 -> Console，查找 [LongFlow] 开头的日志。

局限：无法恢复上游真正未生成/未发出的正文；如果中转不支持流式 Tool Calling，
本补丁会显示诊断信息，无法使不兼容的接口支持工具调用。
