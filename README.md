<div align="center">

# Octop OneBot 11

**把个人 QQ 接入 Octop 的 OneBot 11 / NapCat 通道插件**

正向 / 反向 WebSocket · 群聊触发门控 · 主动发送 · 断线排队补发

[![Version](https://img.shields.io/badge/version-0.7.0-blue.svg)](https://github.com/EchoAI-01/octop-onebot11/releases)
[![Python](https://img.shields.io/badge/python-3.12%2B-3776AB.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Octop](https://img.shields.io/badge/Octop-channel%20plugin-6f42c1.svg)](https://github.com/TencentCloud/Octop)

[功能](#功能) · [环境要求](#环境要求) · [安装](#安装) · [配置](#配置) · [使用](#使用) · [常见问题](#常见问题) · [文档](#文档)

</div>

---

## 简介

Octop 官方的 QQ 通道只支持 QQ 开放平台机器人（AppID + Token），无法使用个人 QQ。本插件通过 Octop 的 `kind: channel` 扩展点接入 **OneBot 11** 协议，让 Octop 收发个人 QQ 消息，兼容 [NapCat](https://github.com/NapNeko/NapCatQQ) 及其他 OneBot 11 实现。

插件由三部分组成：

| 组件 | 类型 | 作用 |
|---|---|---|
| `octop-onebot11` | channel | 连接 NapCat，收发消息，处理触发策略、白名单、断线重连与排队补发；持久化聊天记录供 Agent 按需查询 |
| `octop-onebot11-tools` | tool | 给 Agent 提供 13 个操作工具（发送、查询记录、群管理、探测等） |
| `octop-onebot11-skills` | skill | 教 Agent 怎么用这些能力的技能文档（纯文档，无代码） |

## 功能

- **正向与反向连接**：`forward` 由 Octop 连接 NapCat，`reverse` 由 NapCat 连接 Octop，适配内网、NAS 与 NAT 环境。
- **群聊触发门控**：默认只响应 `@` 或关键词；可选「自主参与」在被 @ 之外对提问/求助消息判断是否插话。
- **私聊策略**：`always` / `keyword` / `off` 三档。
- **主动发送**：向任意群或用户发消息，支持文本、图片、语音、@ 成员、引用与合并转发，不要求已有会话；发送返回 `message_id` 回执。
- **聊天记录持久化**：所有收发的消息（含机器人自己的发言）落入本地 SQLite，按「昵称(QQ号) + 时间 + 内容」标注；Agent 通过 `onebot11_query_history` 按时间/关键词/QQ 号按需检索，替代固定条数上下文注入。
- **多媒体理解**：入站图片由插件代为下载后交给多模态模型；语音经 NapCat 自带的语音转文字（或外部 ASR 服务）转成文本。
- **群管理工具**：表情回应、撤回自己的消息、禁言/改名片/撤回他人消息（后者仅限 `admin_users` 名单里的真实发送者触发，不信任模型转述的身份）。
- **断线排队补发**：主动消息在断线时进入持久化队列，重连后按序补发；也可配置 OneBot HTTP API 作为备用发送通道。
- **多账号（反向）**：一条反向通道可接入多个 QQ，按 `self_id` 路由收发。
- **访问控制**：群 / 私聊 / 发送人白名单与黑名单，出站目标白名单，可按 Agent 绑定通道。
- **事件处理**：好友请求、群邀请可自动通过；戳一戳可触发回复；被踢下线时状态如实降级。
- **群巡检**：周期性汇总未触发的群消息并唤醒 Agent，带间隔与每小时上限。
- **一键探测**：`onebot11_probe_napcat` 工具 / `scripts/probe_napcat.py` 扫描本机 NapCat 并报告地址、令牌与登录账号，免手填。
- **安全默认**：反向监听非本机地址必须配置 token；握手阶段拒绝未授权连接；本地媒体只能读取授权目录。
- **弱依赖启动**：NapCat 离线或 QQ 未登录时通道照常注册，状态降级并自动重连，不阻塞 Octop 启动。

## 架构

```mermaid
flowchart LR
    QQ[QQ] <-->|协议| NC["NapCat<br/>OneBot 11 实现"]
    NC <-->|"WebSocket<br/>forward：Octop 连接<br/>reverse：NapCat 连接"| CH["octop-onebot11<br/>channel 插件"]
    CH <-->|ChannelSubject / InboundMessage| GW[Octop Gateway]
    GW <--> AG["Agent / Cron / Tool"]
    TOOLS["octop-onebot11-tools<br/>tool 插件"] -.->|push_text / push_segments| CH
```

- 一条 WebSocket 同时承载入站事件与出站 action，通过 `echo` 关联请求与响应。
- 每条连接只有一个读循环：action 响应与心跳就地处理，业务事件进入该连接的有序队列，由独立 worker 依次处理。
- 插件只依赖 Octop 自带的 `websockets`，不修改 Octop 核心或数据库。

## 环境要求

| 依赖 | 版本 | 说明 |
|---|---|---|
| Octop | 1.0.2 | 需支持 `kind: channel` 插件；验证组合：octop 1.0.2 / octop-gateway 1.0.3 / octop-harness 1.0.4 |
| Python | 3.12+ | 跟随 Octop 运行时，无需单独安装 |
| websockets | 13+ | Octop 内置，验证版本 15.0.1 |
| NapCat | 较新版本 | 或其他兼容 OneBot 11 的实现 |

## 安装

### 从源码安装

```bash
git clone https://github.com/EchoAI-01/octop-onebot11.git
cd octop-onebot11

octop plugin install ./plugins/octop-onebot11 --force
octop plugin install ./plugins/octop-onebot11-tools --force
octop plugin install ./plugins/octop-onebot11-skills --force
```

### Docker 部署

```bash
# 宿主机：把插件目录放进 Octop 数据目录（路径按实际调整）
scp -r plugins/octop-onebot11 plugins/octop-onebot11-tools plugins/octop-onebot11-skills \
  user@host:/opt/octop/data/uploads/

# 容器内安装并重启
docker exec octop octop plugin install /data/.octop/uploads/octop-onebot11 --force
docker exec octop octop plugin install /data/.octop/uploads/octop-onebot11-tools --force
docker exec octop octop plugin install /data/.octop/uploads/octop-onebot11-skills --force
docker restart octop
```

`deploy/install.sh` 支持 Docker / Compose / systemd 三种形态，可远程执行；安装通道插件后默认自动执行 `scripts/patch_dashboard.py`，把连接方式/回复策略字段渲染成中文下拉并按连接方式联动显隐（`--no-dashboard-patch` 跳过）。也可以直接按上面的命令手动安装。

> `octop plugin install` 只写磁盘。安装或升级后必须重启 Octop，运行中的 Gateway 才会加载新插件。

### 验证安装

```bash
octop plugin list
docker logs --tail 100 octop | grep -i "plugin channel kind"
```

看到 `applied 1 plugin channel kind(s): onebot11` 即加载成功；`plugin list` 在新进程里显示 `[not loaded]` 属正常现象。

## 配置

配置保存在通道的 `config_json` 中，可在 Octop Dashboard 的通道表单填写，也可以用 CLI 创建。每个通道独立配置，同一 Octop 可运行多个 QQ 通道。

### 正向：Octop 连接 NapCat

```bash
octop channel create --agent <agent_id> --kind onebot11 --name "QQ (NapCat)" \
  --config '{
    "connection_mode": "forward",
    "ws_url": "ws://127.0.0.1:3001",
    "token": "change-me",
    "group_reply_mode": "mention",
    "trigger_keywords": "小助手,日报,提醒"
  }'
```

### 反向：NapCat 连接 Octop

```bash
octop channel create --agent <agent_id> --kind onebot11 --name "QQ (NapCat 反向)" \
  --config '{
    "connection_mode": "reverse",
    "listen_host": "0.0.0.0",
    "listen_port": 3002,
    "listen_path": "/onebot/v11/ws",
    "token": "change-me"
  }'
```

创建后重启 Octop 让 Gateway 加载通道。

> **安全提示**
>
> - 反向模式监听非本机地址时必须设置 `token`，否则通道拒绝监听；仅当网络完全可信时才开启 `reverse_allow_no_token`。
> - 示例中的 `change-me` 只是占位。真实 token 不要写进命令行（会留在 shell 历史与进程列表），请用 Dashboard 密码框或环境变量方式注入。

### 常用字段

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `connection_mode` | select | `forward` | `forward` / `reverse` |
| `ws_url` | text | — | 正向必填，如 `ws://napcat:3001` |
| `token` | password | 空 | 正向发给 NapCat 校验；反向校验 NapCat 携带的 token |
| `listen_host` / `listen_port` / `listen_path` | text/number/text | `0.0.0.0` / `3002` / `/onebot/v11/ws` | 反向监听参数 |
| `self_id` | text | 空 | 机器人 QQ，留空自动获取 |
| `group_reply_mode` | select | `mention` | `mention` / `auto` / `always` / `off`；`auto` 为自主参与（见回复策略） |
| `trigger_keywords` | text | 空 | 逗号分隔，命中后群聊无需 @ |
| `private_reply_mode` | select | `always` | `always` / `keyword` / `off` |
| `group_history_limit` | number | `10` | 回复时携带的群上下文条数（设为 0 即只看当前消息，需要背景时 Agent 自己查记录） |
| `reply_with_quote` | switch | `false` | 回复是否引用原消息 |
| `sender_label_with_id` | switch | `true` | 消息标注为「昵称(QQ号)」，QQ 号是用户唯一标识 |
| `message_time_prefix` | switch | `true` | 每条消息前缀 `[MM-DD HH:MM]` 时间戳 |
| `allowed_groups` / `allowed_users` | text | 空 | 群 / 私聊白名单，逗号分隔 |
| `allowed_senders` / `blocked_senders` | text | 空 | 发送人白名单 / 黑名单（黑名单优先） |
| `admin_users` | text | 空 | 允许通过 Agent 触发群管理工具（禁言/改名片/撤回他人消息）的 QQ 号，逗号分隔 |
| `history_enabled` | switch | `true` | 聊天记录持久化（本地 SQLite，供 Agent 按需查询） |
| `media_fetch_enabled` | switch | `true` | 入站图片下载后交给多模态模型、语音转文字 |
| `asr_base_url` | text | 空 | OpenAI 兼容语音转写服务地址；留空则用 NapCat 自带转写 |
| `outbox_enabled` | switch | `true` | 断线时主动消息进入队列，重连后补发 |
| `media_allowed_dirs` | text | 空 | 允许主动发送读取的本地媒体目录 |
| `multi_account` | switch | `false` | 反向模式允许接入多个 QQ |
| `monitor_enabled` | switch | `false` | 群巡检 |
| `poke_triggers_reply` | switch | `false` | 戳一戳触发回复 |
| `auto_accept_friend_request` / `auto_accept_group_invite` | switch | `false` | 自动通过好友请求 / 群邀请 |

### 高级字段

| 字段 | 默认 | 说明 |
|---|---|---|
| `reverse_allow_no_token` | `false` | 允许无 token 的反向监听（不安全） |
| `keyword_case_sensitive` | `false` | 关键词是否区分大小写 |
| `ignore_self_message` | `true` | 忽略机器人自己发出的消息 |
| `max_text_chars` | `1500` | 单条文本上限，超长自动分片 |
| `group_history_ttl_seconds` | `300` | 群上下文缓存有效期 |
| `display_timezone` | `Asia/Shanghai` | 消息时间戳与记录查询使用的时区（IANA 名称） |
| `history_dir` / `history_retention_days` / `history_max_rows_per_chat` | 空 / `30` / `20000` | 记录库位置（默认 `~/.octop/onebot11/history`）、保留天数、每会话行数上限 |
| `media_max_mb` / `media_timeout_seconds` / `media_allowed_hosts` | `8` / `20` / 空 | 入站媒体的大小上限、下载超时、额外信任的主机（默认仅 QQ 官方域名与 NapCat 自身） |
| `asr_api_key` / `asr_model` / `asr_language` | 空 / `whisper-1` / `zh` | 外部转写服务的密钥、模型、语言 |
| `asr_napcat_fallback` | `true` | 外部转写未配置或失败时，用 NapCat 自带的语音转文字兜底 |
| `auto_reply_threshold` | `0.6` | 自主参与阈值，越小越积极（0.3 很积极，1.0 基本只在明确求助时出声） |
| `auto_reply_cooldown_seconds` / `auto_reply_max_per_hour` | `120` / `6` | 自主参与的每群冷却与每小时上限 |
| `send_rate_limit_count` / `send_rate_limit_window` | `20` / `60` | 发送限速（条数 / 秒），`0` 关闭 |
| `send_rate_limit_per_chat_count` | `0` | 每会话限速（滑动窗口，`0` 关闭），降低单群刷屏风控风险 |
| `outbox_max_items` / `outbox_ttl_seconds` / `outbox_max_attempts` | `100` / `3600` / `3` | 队列容量、有效期、单条最大尝试次数 |
| `outbox_retry_min_seconds` / `outbox_retry_max_seconds` | `2` / `60` | 失败重试的指数退避区间 |
| `outbox_persist` / `outbox_persist_dir` | `true` / 空 | 队列落盘，默认 `~/.octop/onebot11/state` |
| `http_api_url` / `http_api_token` / `http_api_timeout_seconds` | 空 / 空 / `10` | OneBot HTTP API 备用发送通道 |
| `resolve_forward` | `false` | 入站合并转发是否展开为文本 |
| `monitor_interval_seconds` / `monitor_min_messages` / `monitor_max_per_hour` | `600` / `5` / `4` | 巡检周期、触发条数、每小时上限 |
| `monitor_keywords` / `monitor_min_trigger_gap_seconds` | 空 / `1800` | 巡检关键词过滤；同一群两次触发的最小间隔 |
| `connect_timeout_seconds` / `request_timeout_seconds` | `8` / `15` | 连接与 action 超时 |
| `startup_wait_seconds` | `2` | 创建通道时等待首次连接的时间 |
| `reconnect_min_seconds` / `reconnect_max_seconds` | `1` / `30` | 重连退避区间 |

## 使用

### 回复策略

| 场景 | 配置 | 模型调用 |
|---|---|---|
| 群聊仅 @ 或关键词 | `group_reply_mode=mention` + `trigger_keywords` | 命中时 1 轮 |
| 群聊自主参与 | `group_reply_mode=auto` + `auto_reply_*` | 被提问/求助的消息才触发，受冷却与每小时上限约束 |
| 群聊全部回复 | `group_reply_mode=always` | 每条群消息 |
| 群聊不回复 | `group_reply_mode=off` | 0 |
| 私聊全部回复 | `private_reply_mode=always` | 每条私聊 |
| 私聊仅关键词 | `private_reply_mode=keyword` | 命中时 1 轮 |
| 私聊不回复 | `private_reply_mode=off` | 0 |
| 定时纯文本推送 | cron `task_type=text` | 0 |
| 定时生成内容 | cron `task_type=agent` | 每次 1 轮 |

非触发的群消息仍进入短期上下文缓存（默认 10 条 / 300 秒），同时全量落入持久记录库——把 `group_history_limit` 降为 0 也不损失可查性，需要背景时 Agent 自己查询。

### Agent 工具

`octop-onebot11-tools` 注册 13 个工具；`octop-onebot11-skills` 里有面向 Agent 的完整用法说明。

| 工具 | 作用 |
|---|---|
| `onebot11_send_group` / `onebot11_send_private` | 发送文本/图片/语音/@/引用到群或私聊，返回 `message_id` 回执 |
| `onebot11_send_forward_group` | 群合并转发 |
| `onebot11_send_voice` | 发送语音条（本地文件或 URL） |
| `onebot11_query_history` / `onebot11_history_overview` | 按时间/关键词/QQ 号查询聊天记录；先看会话概览再决定查什么 |
| `onebot11_status` | 通道健康：在线、登录、重连、发送成功率、队列 |
| `onebot11_list_groups` / `onebot11_group_members` | 列出白名单内的群与群成员（不含隐私字段） |
| `onebot11_react` | 给消息贴表情回应 |
| `onebot11_recall_message` | 撤回机器人自己发的消息（本地库核对后才执行） |
| `onebot11_group_admin` | 禁言/解禁/改名片/撤回他人消息，仅 `admin_users` 中的真实发送者可触发 |
| `onebot11_probe_napcat` | 探测本机 NapCat（仅管理端） |

```python
onebot11_send_group(group_id="123456789", text="构建完成，产物已上传")

# 图片、@ 成员、引用
onebot11_send_group(
    group_id="123456789",
    text="新版本截图",
    image_url="https://example.com/build.png",
    at_user_id="10001",
)
onebot11_send_group(group_id="123456789", text="补充说明", reply_to_message_id="9001")

# 私聊
onebot11_send_private(user_id="10001", text="任务已结束")

# 合并转发
onebot11_send_forward_group(
    group_id="123456789",
    messages_json='[{"name": "早报", "text": "第一条"}, {"name": "早报", "text": "第二条"}]',
)
```

- 断线时：配置了 `http_api_url` 则改走 HTTP；否则进入待发队列，重连后补发。
- 发送成功返回 `message_id`；超时区分「未发出」与「已发出未确认」，不会盲目重发。
- 返回值以 `发送失败:` 开头表示未成功，不会静默丢弃。
- 多通道部署时，请在 Agent 的工具配置里绑定 `channel_id`，否则工具无法确定用哪个通道。

### 查询聊天记录

```python
# 昨天 10001 在群里说了什么
onebot11_query_history(chat_id="123456789", sender="10001", hours=24)

# 检索关键词
onebot11_query_history(chat_id="123456789", keyword="发布", hours=72)
```

记录按「昵称(QQ号) + 时间 + 内容」存储，昵称改了也能通过 QQ 号跨会话追踪同一人；查询结果只包含数据，不会被当成指令执行。

### 定时任务

`task_type=text` 直接走通道发送，不启动 Agent：

```json
{
  "name": "每日群提醒",
  "trigger": "cron:0 9 * * *",
  "prompt": "早上好，今天的任务已排期。",
  "session_key": "<agent_id>:onebot11:group:123456789:group",
  "task_type": "text",
  "enabled": true
}
```

> 该群需要已有会话记录；群从未说过话时改用 `onebot11_send_group` 工具。

### NapCat 侧配置

完整步骤见 [docs/napcat.md](docs/napcat.md)，最小配置：

| 模式 | NapCat 侧 | 关键项 |
|---|---|---|
| 正向 | WebSocket 服务器 | 监听 `0.0.0.0:3001`，Token 与通道一致，消息格式 `array`，关闭上报自身消息 |
| 反向 | WebSocket 客户端 | URL `ws://octop:3002/onebot/v11/ws`，Token 与通道一致，消息格式 `array` |

## 常见问题

| 现象 | 原因 | 处理 |
|---|---|---|
| `等待 NapCat 反向连接` | NapCat 未配置 WS 客户端或网络不通 | 检查 NapCat 反向配置与 `listen_port` |
| `NapCat 未登录，等待登录` | QQ 未扫码 | 在 NapCat WebUI 登录，无需重建通道 |
| `反向模式未配置 token，已拒绝监听` | 非本机监听且无 token | 配置 `token`，或显式开启 `reverse_allow_no_token` |
| 群消息不触发 | 未 @ 且未命中关键词 | 检查 `group_reply_mode` 与 `trigger_keywords` |
| 自主参与不触发 | 消息得分未到阈值，或在冷却/上限内 | 调低 `auto_reply_threshold`，检查冷却与每小时上限 |
| 图片/语音没被理解 | 媒体抓取被关、超限或域名不可信 | 检查 `media_fetch_enabled` / `media_max_mb` / `media_allowed_hosts`；跨机部署时在 NapCat 打开 `enableLocalFile2Url` |
| 语音转写失败 | NapCat 无 ffmpeg 或消息已不在缓存 | 配置外部 `asr_base_url`，详见 [NapCat 能力文档](docs/napcat-capabilities.md) |
| `发送失败: 存在 N 个 OneBot 11 通道` | 多通道且未绑定 | 在 Agent 工具配置中填写 `channel_id` |
| `发送失败: 本地文件路径发送未启用` | 未授权本地媒体目录 | 改用 http(s) 地址，或配置 `media_allowed_dirs` |
| 群管理工具被拒绝 | 触发者不在 `admin_users`，或消息不属于该会话 | 身份只认 NapCat 事件里的 QQ 号；把使用者加进 `admin_users` |
| Dashboard 字段显示为文本框 | 未打 Dashboard 补丁 | 跑 `scripts/patch_dashboard.py`（install.sh 默认已自动执行），然后硬刷新页面 |
| 修改配置后不生效 | CLI 只写数据库 | 重启 Octop，或用 `scripts/patch_channel.py` 的 HTTP 模式 |
| `channel test` 返回 ok 但没连上 | 该命令只验证配置与生命周期 | 真实状态看 Dashboard 或日志 |

## 限制

- 投递语义是**至少一次**：发送超时可能重复，最多 `outbox_max_attempts` 条。
- 队列持久化为文件级，进程崩溃可能丢失最后一批未落盘消息。
- HTTP 备用通道仅覆盖出站；登录态与入站事件仍依赖 WebSocket。
- 多账号仅支持反向模式，主动发送到未连接的账号会直接报错。
- 群巡检每次触发都会消耗一轮模型调用，节流参数用于控制频次。

## 文档

| 文档 | 内容 |
|---|---|
| [docs/napcat.md](docs/napcat.md) | NapCat 部署与正反向配置 |
| [docs/napcat-capabilities.md](docs/napcat-capabilities.md) | NapCat / OneBot 11 能力全集与通道差距（扩展 Agent 能力前先看） |
| [docs/design.md](docs/design.md) | 实现机制、消息映射、故障排查 |
| [docs/development.md](docs/development.md) | 开发环境、代码地图、开发约定 |
| [ROADMAP.md](ROADMAP.md) | 开发计划（独立文档） |

## License

[MIT](LICENSE)
