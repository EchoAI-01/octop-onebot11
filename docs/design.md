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
       │    ├─ _attach_client(ws)
       │    │    ├─ _clients.add(ws); _ws = ws; _connected_transport = True
       │    │    ├─ 启动 _login_probe_loop(ws)
       │    │    └─ 关闭上一条连接（单连接语义）
       │    └─ _detach_client(ws): 仅当 ws 仍是主连接时复位状态
       └─ 监听循环（0.5s tick），端口占用时按退避重建
```

细节：

- **服务端 API**：优先 `websockets.asyncio.server.serve`；旧版本回退到 `websockets.serve`。两者都支持 `async with`，生命周期一致。
- **鉴权**：`Authorization: Bearer <token>` 或查询参数 `?access_token=<token>`，使用 `hmac.compare_digest` 比较；`token` 为空时接受任意连接并打印告警。
- **路径**：默认只接受 `/onebot/v11/ws`，不匹配直接 1008 关闭；`listen_path="/"` 表示任意路径。
- **`X-Self-ID`**：NapCat 的反向连接会带该头，插件只用来预填 `self_id`，不据此判定已登录。
- **单连接**：同一通道同时只保留一条主连接。NapCat 重连时新连接顶掉旧连接，旧连接关闭时不会误清状态。
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
