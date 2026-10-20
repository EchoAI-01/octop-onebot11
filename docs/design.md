# 设计说明

本文说明 `octop-onebot11` 的内部实现，便于二次开发与排障。面向使用者的安装配置见 [README](../README.md)。

## 1. 为什么做成插件

Octop 的通道体系由 `octop_gateway` 提供，内置飞书、钉钉、QQ 官方 Bot、企业微信、Telegram 等实现。要新增通道有两条路：

1. 修改 `octop_gateway/channels/` 并重建镜像；
2. 使用插件系统的 `kind: channel` 扩展点，通过 `PluginContext.channel()` 注册。

本插件选择第 2 条：不修改 Octop 源码、不动数据库 schema，升级 Octop 时只需要回归 `BaseChannel` 契约。

```python
def setup(ctx):
    ctx.channel(
        "onebot11",
        OneBot11Channel,
        label="OneBot 11 (NapCat)",
        icon=_QQ_ICON,  # QQ 企鹅 SVG data URI，与内置 qq 通道同款（Dashboard 以 <img src> 渲染）
        intro_url="https://napcat.napneko.icu/",
        fields=_FIELDS,
    )
```

`fields` 是 Dashboard 表单的声明式 schema（`name/label/type/placeholder/required/helpKey/options`），配置值最终落到 `channels.config_json`，由 `ChannelConfig.from_dict()` 映射进 `OneBot11Config` dataclass。

## 2. 通道契约

`OneBot11Channel` 实现/覆盖了以下成员：

| 成员 | 作用 |
|---|---|
| `start()` | 启动 supervisor，最多等待 `startup_wait_seconds`，**连接失败不抛异常** |
| `stop()` | 取消任务、关闭客户端与服务端、失败所有 pending action |
| `is_connected` | `_connected_transport and _login_confirmed` |
| `runtime_error` | 暴露给 Dashboard 的中文状态 |
| `parse_inbound()` | OneBot 事件 → `InboundMessage` |
| `_send_text()` / `_send_content()` | Agent 回复路径，尽力而为 |
| `push_text()` | 主动推送路径，`strict_push` 时失败会抛错 |
| `_default_constraints()` | 限速 `20 条 / 60 秒`，关闭 thinking / tool hints |

`start()` 的“失败不抛异常”是刻意设计：NapCat 未启动、QQ 未登录时，通道仍然要能创建、落库、显示降级状态。否则 Dashboard 上连“等待登录”这个状态都看不到。

## 3. 状态机

```
未启动
  ├─ forward → 连接中 → 等待 NapCat 就绪 → 已连接
  └─ reverse → 启动反向服务 → 等待 NapCat 反向连接 → 已连接
                    ↑                    │
                    └──── 断开/失败 ←────┘   （退避 1→2→4…→30s，带抖动）
```

登录态判定有两条来源：

- `get_login_info` 返回非 0 的 `user_id`；
- `heartbeat.status.online == true` 且已知 `self_id`。

两者都失败时只更新 `runtime_error`，不影响通道存活。

## 4. 正向模式

```
_run_forever()
  └─ _connect_once()
       ├─ websockets.connect(ws_url, Authorization: Bearer <token>)
       ├─ _receiver(ws)          # async for raw in ws → _dispatch_payload
       └─ _login_probe_loop(ws)  # 每 15s 探测登录态
```

- 兼容 websockets 13–17：优先 `additional_headers`，`TypeError` 时回退 `extra_headers`。
- 断线后 `_fail_pending()` 让所有等待中的 action 立刻失败，避免调用方挂 15 秒。
- 退避上限 `reconnect_max_seconds`，并叠加 0–0.5s 抖动，避免多通道同时重连。

## 5. 反向模式

```
_serve_forever()
  └─ async with serve(_handle_reverse_client, host, port) as server
       ├─ _handle_reverse_client(ws)   # 路径校验 → token 校验 → 入站循环
       │    ├─ _add_session(ws)        # 每条连接一个 _ConnSession
       │    │    ├─ sessions[key] = {ws, self_id, login, probe, pending}
       │    │    ├─ 启动 _login_probe_loop(session)
       │    │    └─ multi_account=false：关闭全部旧连接（单连接语义）
       │    │       multi_account=true ：不同 self_id 共存，同 self_id 顶旧
       │    └─ _remove_session(session)：清探针/pending，_refresh_state() 重算聚合计
       └─ 监听循环（0.5s tick），端口占用时按退避重建
```

细节：

- **聚合计派生**：`_ws` / `_connected_transport` / `_login_confirmed` / `_self_id` 不再直接维护，由 `_refresh_state()` 从会话表派生（主会话 = `config.self_id` 匹配者或首条），单账号行为与旧版完全一致。
- **账号路由**：出站 `_call_api(account=…)` 严格匹配会话 `self_id`；主动发送指定未连接账号直接报错，被动回复降级回主连接；outbox 条目携带 account，补发走原账号。
- **outbox 持久化**：enqueue/flush/stop 时原子落盘 `{outbox_persist_dir}/{channel_id}.json`，构造时恢复（epoch↔monotonic 换算保留 TTL 年龄），Octop 重启后继续补发。
- **服务端 API**：优先 `websockets.asyncio.server.serve`；旧版本回退到 `websockets.serve`。两者都支持 `async with`，生命周期一致。
- **鉴权**：`Authorization: Bearer <token>` 或查询参数 `?access_token=<token>`，使用 `hmac.compare_digest` 比较；`token` 为空时接受任意连接并打印告警。
- **路径**：默认只接受 `/onebot/v11/ws`，不匹配直接 1008 关闭；`listen_path="/"` 表示任意路径。
- **`X-Self-ID`**：NapCat 的反向连接会带该头，插件只用来预填 `self_id`，不据此判定已登录。
- **单连接（默认）**：`multi_account=false` 时同一通道只保留最新一条连接。NapCat 重连时新连接顶掉旧连接，旧连接关闭时不会误清状态。`multi_account=true` 时多个 QQ 账号可同时接入（每账号一条连接，同账号重连仍顶旧）。
- **探针端口**：`octop channel test` 会创建 `channel_id="__probe__"` 的临时实例；反向模式下自动改用端口 `0`，避免与线上监听冲突。

## 6. 触发门控

`parse_inbound()` 决定“这条消息要不要进 Agent”：

1. `at` 段中 `qq == self_id` → `bot_mentioned=True`；
2. 文本命中 `trigger_keywords` → `bot_mentioned=True`、`keyword_triggered=True`；
3. `_handle_message_event()` 按 `group_reply_mode` / `private_reply_mode` 过滤；
4. 未触发的群消息不入队，只存在 `GroupContextManager` 的短期缓存里。

`group_reply_mode` 同时会映射到 `GroupContextConfig`：

| `group_reply_mode` | `group_context.enabled` | `activation` |
|---|---|---|
| `mention` | `true` | `mention` |
| `always` | `true` | `always` |
| `off` | `false` | — |

这样非触发消息既不进 Agent，又能在下一次触发时提供上下文。

v0.4.0 在同一入口叠加了两层入站策略与巡检：`blocked_senders` / `allowed_senders` 在自查与去重之后过滤发送人（黑名单优先）；`monitor_enabled` 时，mention 模式下未触发的群消息额外进入插件自己的监控缓冲，`_monitor_sweep()` 按条数 / 关键词 / 间隔 / 每小时上限节流后合成一条摘要事件（`trigger_reason=monitor`）唤醒 Agent。摘要文本要求 Agent 无需介入时只回「〔巡检跳过〕」，出站时被 `_send_text()` 的哨兵检查静默丢弃——这依赖入站 metadata 随会话回流出站，与 `reply_with_quote` 使用 `metadata.message_id` 是同一机制。

## 7. 发送与路由

`_build_send_action()` 根据 subject 判断群/私聊。判断信号按优先级：

```
subject.chat_type == "group"
metadata.chat_type == "group"
metadata.message_type == "group"
metadata.is_group is True
metadata.group_id 非空
subject_id 以 "group:" 开头
```

之所以要看这么多字段：Octop 的 `message_keys.chat_type_from_message()` **只读 `metadata["chat_type"]`**。早期实现没有写入该键，群会话被持久化成 `dm`，主动推送会走 `send_private_msg` 发给群成员。现在 `parse_inbound()` 会同时写 `chat_type` 与 `is_group`，`_enrich_push_metadata()` 还会给主动构造的稀疏 subject 补齐路由信息。

失败语义分两条：

- 被动回复：`_send_text()` 捕获 `OneBotAPIError` / 连接异常，只记日志，不影响 Agent 流程；
- 主动发送：`strict_push=True` 时（工具构造的 subject）直接抛错，让 Agent 知道“没发出去”。

## 8. 故障排查

| 日志 / 状态 | 含义 | 处理 |
|---|---|---|
| `OneBot11 reverse WS listening on ws://...` | 反向监听已就绪 | 检查 NapCat 客户端是否连入 |
| `OneBot11 reverse WS client connected` | 反向连接建立 | — |
| `OneBot11 reverse WS rejected token` | token 不一致 | 对齐两端 token |
| `反向 WS 端口监听失败` | 端口占用 | 换端口或释放占用 |
| `OneBot11 WS unavailable: Connect call failed` | 正向连不上 | 检查 `ws_url`、容器网络 |
| `NapCat 未登录，等待登录` | 已连上但未登录 | NapCat WebUI 扫码 |

常用命令：

```bash
docker logs --tail 200 octop | grep -i onebot11
docker exec octop octop channel list --agent <agent_id>
docker exec octop octop channel test <channel_id>
```

`channel test` 只验证配置可实例化、生命周期可走通，**不验证** NapCat 是否登录。

## 9. 已知限制与路线图

- 无离线队列 / 重试 / 幂等键，断线期间的主动消息会失败。
- 无投递回执，只保证 OneBot `retcode=0`。
- 主动发送工具仅文本，富媒体依赖 Agent 回复路径。
- 无群白名单与跨群发送配额，群发有 QQ 风控风险。
- 反向模式单连接，不支持多账号同时接入同一通道。

欢迎围绕以上几点提 PR。
