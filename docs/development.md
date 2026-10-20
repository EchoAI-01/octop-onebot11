# 开发与接手指南

> 面向接手本项目的开发者。目标：**1 小时跑起来，半天能改第一个需求**。
> 使用说明见 [README](../README.md)，NapCat 配置见 [napcat.md](napcat.md)，实现细节见 [design.md](design.md)。

## 1. 项目速览

| 项 | 值 |
|---|---|
| 定位 | Octop 的 OneBot 11 / NapCat 通道插件，让 Octop 接入个人 QQ |
| 组成 | 1 个 channel 插件 + 1 个 tool 插件 + 测试/部署/运维脚本 |
| 代码量 | 插件约 3.6k 行（通道 3.2k + 工具 0.4k），测试与测试夹具约 4.2k 行，运维/部署脚本约 1.1k 行 |
| 运行环境 | Octop 进程内，由插件加载器动态导入 |
| 外部依赖 | 仅 `websockets`（Octop 已内置，验证 15.0.1）；HTTP 回退用标准库 `urllib` |
| 支持版本 | websockets 13–17，Python 3.12+ |
| 当前版本 | v0.6.0 |

### 仓库结构

```
octop-onebot11/
├─ plugins/
│  ├─ octop-onebot11/            # channel 插件，入口 setup(ctx)
│  │  ├─ plugin.yaml             # id / kind: channel / entry
│  │  └─ main.py                 # 配置类 + 通道实现 + 字段 schema
│  └─ octop-onebot11-tools/      # tool 插件，入口 setup(ctx)
│     ├─ plugin.yaml             # id / kind: tool
│     └─ main.py                 # onebot11_send_group / _private / _forward_group
├─ tools/
│  ├─ mock_napcat.py             # 假 NapCat：WS/HTTP 服务端 + action 响应（可模拟慢/无应答）
│  ├─ octop_stubs.py             # octop_gateway 契约的共享替身（离线测试与回归测试用）
│  ├─ test_plugin_offline.py     # 纯逻辑测试，不需要 Octop
│  ├─ test_plugin_regression.py  # 每个审计发现一条回归用例（进程内假 NapCat + 真 websockets）
│  ├─ test_plugin_e2e.py         # 真实 WS + mock NapCat 子进程，含反向/outbox/慢应答场景
│  └─ test_plugin_core_integration.py  # 真实 octop_gateway/octop_harness 契约测试（未安装则跳过）
├─ scripts/
│  ├─ patch_channel.py           # 配置读-合并-写（防 channel patch 整体替换，密钥走环境变量）
│  └─ patch_dashboard.py         # Dashboard bundle 补丁：select / help / 简介渲染
├─ deploy/
│  ├─ install.sh                 # 跨形态部署脚本（systemd / docker，R2）
│  ├─ install.ps1                # Windows 辅助脚本（参数化，仅 Docker 形态；不创建通道）
│  └─ onebot11_config.template.json
├─ docs/                         # 本文件、design.md、napcat.md
└─ .github/workflows/ci.yml
```

### 运行形态

Octop 启动时扫描插件目录，把 `main.py` 以 `harness_plugin_<plugin_id>` 的模块名导入，然后调用 `setup(ctx)`：

```
octop 启动
  └─ plugin loader
       ├─ import harness_plugin_octop_onebot11 → setup(ctx) → ctx.channel("onebot11", ...)
       └─ import harness_plugin_octop_onebot11_tools → setup(ctx) → ctx.tool(...)
```

两个关键推论：

1. **插件无法注册 HTTP 路由**。`PluginContext` 只提供 `tool()` / `channel()` / `skills()` / `hook()`，没有 `route()`。任何“通过 HTTP 改配置”的能力，要么复用 Octop 核心已有接口，要么跑独立 sidecar。
2. **tool 插件通过模块名找 channel 实例**：`sys.modules["harness_plugin_octop_onebot11"]`。改 `plugin.yaml` 的 `id` 会同时改变模块名，必须同步改 `plugins/octop-onebot11-tools/main.py` 里的 `_CHANNEL_MODULE`。

## 2. 本地开发环境（10 分钟）

```bash
git clone https://github.com/EchoAI-01/octop-onebot11.git
cd octop-onebot11

python -m venv .venv
.venv/Scripts/python -m pip install "websockets==15.0.1"   # Windows
# source .venv/bin/activate && pip install "websockets==15.0.1"   # Linux/macOS

python tools/test_plugin_offline.py       # 不需要 Octop，秒级
python tools/test_plugin_regression.py    # 审计回归，进程内假 NapCat，约 20 秒
python tools/test_plugin_e2e.py           # 起 mock NapCat 子进程，约 25 秒
python tools/test_plugin_core_integration.py   # 需要真实 Octop 包，否则自动跳过
```

离线与回归测试用 `tools/octop_stubs.py` 里的替身代替 `octop_gateway`，所以**不需要安装 Octop** 就能验证解析、路由、配置、鉴权和重试逻辑。改动协议层时优先补这里的断言。

想在真实 Octop 组件上跑契约测试（推荐在改动配置、群上下文、限速、工具参数时跑一遍）：

```bash
pip install "octop-gateway==1.0.3" "octop-harness==1.0.4"
ONEBOT11_REQUIRE_CORE=1 python tools/test_plugin_core_integration.py   # 装不上就报错而不是跳过
```

它在同一进程里用真实插件加载器加载两个插件，用真实 `ChannelManager` 驱动通道，对着进程内假 NapCat 收发；只占用 OS 分配的回环端口，不触碰任何现网服务。

**验证“测试真的能抓到回归”**：`ONEBOT11_PLUGIN_MAIN=/path/to/old/main.py python tools/test_plugin_regression.py` 会让测试加载另一份通道插件（例如 `git show <旧提交>:plugins/octop-onebot11/main.py > /tmp/old_main.py`）。新增回归用例时，先在旧实现上确认它会失败，再在新实现上确认它通过。

### 在真实 Octop 上调试

```bash
# 把插件目录放进 Octop 数据卷
scp -r plugins/octop-onebot11 user@host:/opt/octop/data/uploads/

# 容器内安装并重启
docker exec octop octop plugin install /data/.octop/uploads/octop-onebot11 --force
docker restart octop

# 看日志
docker logs -f octop | grep -i onebot11
```

常用观察点：

| 日志 | 含义 |
|---|---|
| `applied N plugin channel kind(s): onebot11` | 插件注册成功 |
| `OneBot11 WS connected` / `reverse WS listening` | 传输就绪 |
| `OneBot11 reverse WS client connected` | NapCat 反向连入 |
| `OneBot11 WS unavailable: ...` | 正向连不上，看 `ws_url` 与网络 |

### 提交前检查

```bash
python -m compileall -q plugins scripts tools
python tools/test_plugin_offline.py
python tools/test_plugin_regression.py
python tools/test_plugin_e2e.py
```

CI 的 `test` 任务在 Python 3.12 / 3.13 上跑以上全部，`core-integration` 任务在 3.12 上装真实 Octop 包跑契约测试；本地至少保证 3.12 通过。

## 3. 代码地图

### `plugins/octop-onebot11/main.py`

| 区块 | 关键成员 | 说明 |
|---|---|---|
| 工具函数 | `_to_bool` / `_to_int` / `_to_float` / `_split_id_list` / `_normalize_path` | Dashboard 表单值是字符串，统一归一化 |
| 模块级 | `_LIVE_CHANNELS` / `get_live_channel()` / `list_live_channels()` / `build_group_subject()` / `build_private_subject()` | 供 tool 插件取活实例、构造主动发送目标；注册表按实例身份注销，旧实例停止不会踢掉新实例 |
| 配置 | `OneBot11Config` | `ChannelConfig` 子类，`from_dict` 由 Octop 调用；`missing_credentials()` 按模式校验 |
| 生命周期 | `start()` / `stop()` / `is_connected` / `runtime_error` | `start()` 永不因连接失败抛错；`start()` 同时拉起 outbox flush 协程 |
| 连接会话 | `_ConnSession` / `_add_session()` / `_remove_session()` / `_primary_session()` / `_select_session()` / `_register_session_identity()` / `_refresh_state()` / `_fail_pending()` | 每条连接独立 self_id/登录态/探针/pending/事件队列；聚合计从会话表派生；多账号共存、同账号重连顶旧；会话上限 32 |
| 收包 | `_run_session()` / `_read_frames()` / `_ingest_payload()` / `_dispatch_fast()` / `_session_worker()` / `_drain_session_events()` / `_dispatch_payload()` | 读循环只收帧：echo 与 meta 就地处理，其余事件入该会话的有序队列；worker 逐条处理；连接关闭后先失败 pending 再限时排空 |
| 正向 | `_run_forever()` / `_connect_once()` / `_receiver(session)` | 拨号、退避重连、收帧（同样走会话） |
| 反向 | `_serve_forever()` / `_reverse_listen_refusal()` / `_server_factory()` / `_reverse_process_request()` / `_reverse_rejection()` / `_handle_reverse_client()` | 安全默认（非回环无 token 不监听）、握手前拒绝（404/401/503）、会话挂载/摘除（`multi_account` 控制共存或替换） |
| 登录态 | `_login_probe_loop()` / `_handle_meta_event()` | `get_login_info` + heartbeat 双来源 |
| 入站 | `parse_inbound()` / `_parse_message_content()` / `_parse_cq_string()` | OneBot 事件 → `InboundMessage` |
| 触发 | `_parse_keywords()` / `_keyword_hit()` / `_handle_message_event()` | @ / 关键词 / 私聊策略过滤 |
| 出站 | `_send_text()` / `_send_content()` / `_send_media()` / `_send_segments()` / `_build_send_action()` | 文本分片、base64 媒体、群/私聊路由；被动路径过白名单门控 |
| 主动发送 | `push_text()` / `push_segments()` / `_compose_push_chunks()` / `_materialize_media_segment()` / `_read_allowed_local_file()` | strict 语义；at/图片/文本混排；本地文件仅限 `media_allowed_dirs`，转 base64 |
| 出站策略 | `_send_denial()` / `_default_constraints()` | `allowed_groups` / `allowed_users` 白名单；限速读配置 |
| 入站策略 | `_sender_allowed()`（消息、戳一戳、自动通过共用） | 发送人黑名单优先于白名单 |
| 事件处理 | `_handle_notice_event()` / `_handle_request_event()` / `_enqueue_poke()` | 好友/群邀请自动通过、戳一戳合成触发、撤回/入群日志 |
| HTTP 回退 | `_http_available()` / `_call_api_http()` | `_call_api()` 在 WS 不可用时改走 `POST {http_api_url}/{action}`（标准库 urllib + to_thread） |
| 群巡检 | `_ensure_monitor_task()` / `_monitor_loop()` / `_monitor_record()` / `_monitor_sweep()` / `_monitor_keyword_hit()` / `_synthetic_message()` | 非触发消息入缓冲；周期检查；间隔/每小时双重节流；摘要触发 Agent；`_MONITOR_SKIP_TEXTS` 哨兵在 `_send_text()` 静默丢弃 |
| 待发队列 | `_OutboxItem` / `_enqueue_outbox()` / `_flush_outbox()` / `_outbox_flush_loop()` / `_outbox_backoff()` | 断线排队、按序补发、指数退避、TTL/次数上限；WS 断开且 HTTP 可用时也会补发；条目携带 account |
| 队列持久化 | `_persist_path()` / `_persist_outbox()` / `_load_outbox()` | enqueue/flush/stop 原子落盘；构造时恢复（epoch↔monotonic 换算保 TTL） |
| 协议 | `_call_api()` / `_call_via()` / `_api_via_session()` | `echo` 关联、超时、断线失败；`_call_via(session)` 保证在收到事件的那条连接上追问 |
| 字段 schema | `_FIELDS` / `setup()` | Dashboard 表单声明 |

### `plugins/octop-onebot11-tools/main.py`

| 成员 | 说明 |
|---|---|
| `_channel_module()` | 从 `sys.modules` 取 channel 插件模块 |
| `_CONFIG_FIELDS` / `_bound_channel_id()` | 每个工具声明 `channel_id` 配置项；经 `octop_harness.plugins.tools.get_tool_config()` 读取该 Agent 的绑定 |
| `_resolve_channel()` | 绑定优先；未绑定时仅在恰有一个通道时自动选用，多通道拒绝；不要求 `is_connected`（断线时走 HTTP 回退或 outbox 排队） |
| `onebot11_send_group()` / `onebot11_send_private()` | 构造 subject → `push_segments()`，支持 `image_url` / `at_user_id` / `reply_to_message_id` / `account`（多账号路由），失败返回 `发送失败: ...` |
| `onebot11_send_forward_group()` | `messages_json` → OneBot node 数组 → `push_forward()`（合并转发，仅群） |
| `_push_note()` | 断线排队提示；HTTP 回退成功时不提示 |
| `setup()` | `ctx.tool()` 注册，description 会影响模型是否调用，修改时注意措辞 |

### `tools/mock_napcat.py`

| 参数 | 作用 |
|---|---|
| `--host` / `--port` | WS 监听地址，默认 `127.0.0.1:3001` |
| `--token` | 校验 `Authorization`，空表示不校验 |
| `--logged-in` | `get_login_info` 返回有效 `user_id` |
| `--send-test-after N` | 启动 N 秒后推一条群 @ 测试消息 |
| `--log-file` | 把收到的 action 写入文件，E2E 用它断言 |
| `--http-port` | 同时开 OneBot HTTP API，用来测 HTTP 回退 |
| `--ack-delay S` / `--no-ack-send` | `send_*` 照常执行并记日志，但应答延迟 S 秒 / 永不应答（模拟“已送达但没确认”） |

## 4. 核心机制

### 4.1 状态机

```
未启动 ──start()──> 连接中 / 启动反向服务
                        │
                        ├─ 成功 ──> 等待 NapCat 就绪 ──登录确认──> 已连接
                        └─ 失败 ──> 退避重连（1→2→4…→30s，带抖动）
```

`is_connected = _connected_transport and _login_confirmed`。传输通了但没登录，状态是“等待登录”，不能用于主动发送。

### 4.2 入站与触发

`parse_inbound()` 负责把 OneBot 事件变成 `InboundMessage`，同时决定“是否被提及”：

- 结构化 `at` 段且 `qq == self_id`；
- 文本命中 `trigger_keywords`（子串、默认不区分大小写）。

`_handle_message_event()` 再按 `group_reply_mode` / `private_reply_mode` 过滤并入队。**未触发的群消息不会触发 Agent**：它们照常进入核心队列，由核心的 `GroupContextManager.prepare()` 判定为被动聊天，只进短期缓存（缓存里的发送者来自 `metadata["sender_id"]`）。

### 4.3 出站路由

`_build_send_action()` 用多个信号判断群/私聊，原因是 Octop 的 `message_keys.chat_type_from_message()` 只读 `metadata["chat_type"]`。改动这块时必须保留“多信号兜底”，否则历史会话会误发私聊。

三种失败语义：

| 路径 | 行为 |
|---|---|
| `_send_text()`（被动回复） | 捕获异常只记日志，不影响 Agent |
| `push_segments()` / `push_text(strict_push=True)`（工具主动发送） | 断线或发送失败时进入待发队列（outbox），重连后按序补发；队列禁用/已满、白名单拦截时抛错返回给 Agent |

白名单（`allowed_groups` / `allowed_users`）在被动路径静默丢弃并记日志（Agent 已产出回复，抛错无人接），在主动路径抛 `PermissionError`。

### 4.4 配置加载

```
_FIELDS（表单 schema） → Dashboard 表单 → channels.config_json
                                         ↓
                    OneBot11Config.from_dict()（只取 dataclass 字段）
                                         ↓
                                 __post_init__ 归一化
```

- 表单只覆盖 `_FIELDS`；高级字段只能通过 CLI/JSON 写入。
- `from_dict` 支持 `field_aliases`（`access_token` → `token`），并会去掉字符串首尾空白。
- `__post_init__` 统一转换：Dashboard 给的数字/布尔常常是字符串，空值与非法值回退默认并写一条警告，永远不让配置构造失败。
- 反向模式通过 `missing_credentials()` 跳过 `ws_url` 必填校验。**不要**把“反向需要 token”放进 `missing_credentials()`：核心会据此抛 `ChannelCredentialsError`，整条通道创建失败，状态栏也看不到原因；这条规则在启动监听时判定并写入 `runtime_error`。

## 5. 开发约定

1. **不引入新依赖**。插件必须能在 Octop 自带环境里直接跑。
2. **不改 Octop 核心**。需要核心能力时，先确认 `PluginContext` 是否已提供；没有再考虑上游 PR。
3. **状态文案中文，日志英文**。`runtime_error` 会显示在 Dashboard；日志给运维 grep。
4. **新增配置项三步**：dataclass 字段 → `_FIELDS` 条目 → 离线测试断言。缺一不可。
5. **新增行为必须带测试**。协议/传输相关加 E2E，纯逻辑加离线断言。
6. **兼容旧版本**。改 `websockets` 调用时保留 13–17 的兼容分支。
7. **不打印 token**。日志里只允许出现是否配置，不允许出现值；URL 先过 `_redact_url()`。
8. **不可信输入不直接生效**：群消息文本/昵称、模型传入的路径与 ID，必须先校验或中和，再进入提示词、文件系统或 OneBot 动作。
9. **收包循环不 await 业务**：读循环里只做解码、应答投递和入队；任何要等 API 应答的逻辑都放进事件 worker。
10. **仓库里不放现网标识**：服务器 IP、Agent ID、通道 ID、token 一律用占位符；示例用 `user@octop-host`、`<AGENT_ID>`。

## 6. 常见开发任务

### 6.1 新增一个配置项

```python
# 1) OneBot11Config
max_image_bytes: int = 10 * 1024 * 1024

# 2) __post_init__ 归一化（表单可能给字符串）
self.max_image_bytes = _to_int(self.max_image_bytes, 10 * 1024 * 1024)

# 3) _FIELDS
{"name": "max_image_bytes", "label": "图片上限（字节）", "type": "number", "required": False}

# 4) test_plugin_offline.py 断言 from_dict 能正确解析
```

同步更新 README 的字段表。

### 6.2 新增一种消息段

在 `_parse_message_content()` 里加分支，映射到已有 `ContentPart`；没有合适的就新增 dataclass 并在 `_send_media()` 里实现反向发送。补一条离线解析断言。

### 6.3 新增一个 Agent 工具

在 tools 插件里加 `async def`，`setup()` 中 `ctx.tool(...)` 注册。要求：

- 返回字符串，失败以 `发送失败:` 开头；
- 参数有默认值，方便模型省略；
- description 写清“什么时候用”，这直接决定模型是否调用。

### 6.4 接入另一个 IM 平台

复制 channel 插件，保留 `parse_inbound()` 的输出结构（`ChannelSubject` + `metadata.chat_type`），替换传输与协议层。这样 Octop 的会话、主动推送、cron 都能直接复用。

## 7. 已知坑

| 坑 | 现象 | 规避 |
|---|---|---|
| 未写 `metadata.chat_type` | 群会话落库为 `dm`，主动推送发私聊 | 入站必须写 `chat_type` / `is_group` |
| CLI 改配置不生效 | `octop channel patch` 只写 DB | 改完 `docker restart octop`，或用 HTTP PATCH（见需求 R1） |
| `channel test` 恒返回 ok | 它只验证实例化与生命周期 | 真实状态看 `is_connected` / 日志 |
| 反向探针抢端口 | `channel test` 与线上监听冲突 | 探针实例用端口 0（已实现，别改回去） |
| 反向单连接 | 新连接顶掉旧连接 | 同一 QQ 不要配两条反向 WS |
| NapCat `string` 格式 | 消息变成 CQ 码 | 配置 `array`；插件仅保留 CQ 回退 |
| websockets 版本差异 | `additional_headers` / `serve` 参数不同 | 保留 `TypeError` 回退分支 |
| 改插件 id | tool 插件找不到 channel 模块 | 同步改 `_CHANNEL_MODULE` |
| 读循环里等 API 应答 | 应答只能由读循环收，等到超时 | 事件一律交给会话 worker（见 design.md 4.1） |
| `always` 没生效 | 核心要求 `visibility=all` 才兑现 `activation=always` | `group_reply_mode=always` 时同时写 `visibility="all"` |
| 群历史里发送者全是 `unknown` | 核心用 `metadata["sender_id"]` 记发送者 | `parse_inbound()` 必须写 `sender_id` / `sender_name` |
| 工具串到别的通道 | 多通道时按“第一个”选通道 | 用每个 Agent 的 `channel_id` 工具配置绑定；未绑定多通道直接拒绝 |

## 8. 路线图

开发计划、待办优先级、验收标准与需求状态统一记录在 [ROADMAP.md](../ROADMAP.md)。本文件只保留开发环境、代码结构与设计说明，不再重复计划内容。

## 9. 设计稿 R1：通道配置热更新接口

> 状态与排期见 [ROADMAP.md](../ROADMAP.md)：Phase 1（`scripts/patch_channel.py`）已实现，Phase 2/3 待 Octop 核心 PR。

### 9.1 背景

当前修改通道配置有三条路，各有缺陷：

| 方式 | 行为 | 问题 |
|---|---|---|
| Dashboard 编辑 | 调 `PATCH /agents/{agent_id}/channels/{channel_id}` | 需要人工操作，无法脚本化 |
| `octop channel patch` | 只写数据库 | 运行中的 Gateway 不 reload，必须重启 Octop |
| 直接改 `config_json` | 绕过校验 | 容易写坏配置，且同样要重启 |

好消息：**Octop 核心已经有热生效能力**。`Gateway.update_channel()` 的流程是：

```
更新 DB → _unregister(channel_id) → _safe_register_channel(row)
```

也就是说，HTTP PATCH 会停掉旧实例并重建，配置立即生效，不需要重启 Octop。真正缺的是“好用、可脚本化、可回滚”的接口层。

### 9.2 现状缺口

1. **整体替换**：`ChannelPatchBody.config` 是完整 dict，`json.dumps` 后直接落库。调用方必须回传全量配置，漏字段就会丢配置。
2. **无校验**：非法配置先落库，再注册；注册失败被 `_safe_register_channel` 吞掉，只把状态标成 `error`，DB 里留下坏配置。
3. **无回滚**：注册失败后不会恢复旧配置。
4. **无审计**：谁在什么时候改了什么，没有记录。
5. **无读取接口**：拿不到当前配置（token 还需要脱敏），脚本无法做“读-改-写”。
6. **一律重建**：改一个关键词也会断开 WebSocket；反向模式下监听端口会短暂关闭，NapCat 需要重连。
7. **插件无 `apply_config` 钩子**：核心只能重建，不能“原地生效”。

### 9.3 目标与非目标

**目标**

- 提供稳定的 HTTP 接口，支持**部分字段更新**、校验、失败回滚、变更审计。
- 区分“可原地生效”和“必须重建”的字段，减少无谓断线。
- 提供配置读取接口，token 等敏感字段脱敏。
- 兼容现有 `PATCH .../channels/{id}`，不破坏 Dashboard。

**非目标**

- 不做配置版本历史/回滚到任意历史版本（只回滚到上一次）。
- 不做多租户权限模型改造，沿用 `require_permission("channels")`。
- 不做配置模板/继承。

### 9.4 接口设计（v1）

#### 读取配置

```http
GET /api/v1/agents/{agent_id}/channels/{channel_id}/config
```

```json
{
  "channel_id": "<CHANNEL_ID>",
  "kind": "onebot11",
  "version": "sha256:9f2c...",
  "config": {
    "connection_mode": "forward",
    "ws_url": "ws://napcat:3001",
    "token": "***",
    "group_reply_mode": "mention"
  }
}
```

- `version` 为配置内容规范化后的 SHA-256，用于乐观锁。
- `token` 返回 `***`；`null` 表示未设置。写回时 `***` 视为“保持不变”。

#### 更新配置

```http
PATCH /api/v1/agents/{agent_id}/channels/{channel_id}/config
Content-Type: application/json
```

```json
{
  "patch": {
    "group_reply_mode": "always",
    "trigger_keywords": "日报,提醒"
  },
  "replace": false,
  "validate_only": false,
  "if_version": "sha256:9f2c..."
}
```

语义：

| 字段 | 说明 |
|---|---|
| `patch` | 浅合并；值为 `null` 表示删除该键；`***` 表示保持原值 |
| `replace` | `true` 时等价于现有全量替换（兼容老客户端） |
| `validate_only` | `true` 时只校验并返回将要变化的字段，不落库 |
| `if_version` | 可选；与当前版本不一致返回 `409` |

处理流程：

```
1. 取旧配置 → 2. 合并 patch → 3. 构建 OneBot11Config 并校验
4. missing_credentials() / 未知字段检查
5. 计算 changed / restart_required
6. validate_only ? 返回 : 落库 → apply → 更新 version → 审计
7. apply 失败 → 回滚 DB → 重新注册旧配置 → 返回 422
```

成功响应：

```json
{
  "channel_id": "<CHANNEL_ID>",
  "applied": true,
  "restarted": false,
  "changed": ["group_reply_mode", "trigger_keywords"],
  "restart_required": false,
  "warnings": [],
  "version": "sha256:1ab3...",
  "runtime": {"connected": true, "error": null}
}
```

错误码：

| HTTP | code | 场景 |
|---|---|---|
| 400 | `INVALID_CONFIG` | 类型错误、未知字段、必填缺失 |
| 403 | `FORBIDDEN` | 无 `channels` 权限或跨 Agent 访问 |
| 404 | `NOT_FOUND` | 通道不存在 |
| 409 | `VERSION_CONFLICT` | `if_version` 与当前不一致 |
| 422 | `APPLY_FAILED` | 注册失败，已回滚，`detail` 带原因 |

#### 插件侧钩子

```python
class BaseChannel:
    async def apply_config(self, config: ChannelConfig) -> None:
        """原地应用新配置；未实现则抛 NotImplementedError，由核心回退到重建。"""
        raise NotImplementedError
```

OneBot 通道的实现要点：

```python
HOT_FIELDS = {
    "group_reply_mode", "trigger_keywords", "keyword_case_sensitive",
    "private_reply_mode", "group_history_limit", "group_history_ttl_seconds",
    "reply_with_quote", "ignore_self_message", "max_text_chars",
}
RESTART_FIELDS = {"connection_mode", "ws_url", "token", "listen_host", "listen_port", "listen_path"}

async def apply_config(self, config):
    restart = {f for f in RESTART_FIELDS if getattr(config, f) != getattr(self._config, f)}
    if restart:
        raise RestartRequired(sorted(restart))
    self._config = config
    self._keywords = self._parse_keywords(config.trigger_keywords)
    self._apply_group_context(config)   # 原地更新 group_context
```

核心在 `update_channel()` 中优先调用 `apply_config`，捕获 `RestartRequired` 再走 unregister/register。

#### 审计

新增表（或复用事件表）：

```sql
CREATE TABLE channel_config_events (
  id           INTEGER PRIMARY KEY,
  channel_id   TEXT NOT NULL,
  actor        TEXT NOT NULL,
  created_at   TIMESTAMP NOT NULL,
  patch_json   TEXT NOT NULL,
  changed_json TEXT NOT NULL,
  result       TEXT NOT NULL,   -- applied | validate_only | rolled_back
  version      TEXT NOT NULL
);
```

不允许改 schema 时，退化为结构化日志：`channel_config_change channel_id=... actor=... changed=... result=...`。

### 9.5 实施路径

| 阶段 | 内容 | 改动范围 | 预估 |
|---|---|---|---|
| Phase 1 | ✅ v0.3.0 已完成：`scripts/patch_channel.py` 读取现有配置、合并后调用旧 PATCH；实测发现 CLI `--config` 为**整体替换**且 `get` 会掩码 token（`secr****`），脚本因此强制 masked 字段必须显式 `--set` 或经 `--base-file` 提供 | 本仓库 | 1–2 人日 |
| Phase 2 | 核心新增 `/config` GET/PATCH、合并、校验、版本、回滚、审计 | Octop 核心 PR | 3–5 人日 |
| Phase 3 | `BaseChannel.apply_config` + OneBot 热字段实现，核心优先原地生效 | Octop 核心 + 本仓库 | 2–3 人日 |

Phase 1 可以先落地：脚本解决“不想手动改库”的诉求，不依赖核心改动。Phase 2/3 需要给 Octop 提 PR。

> 不推荐在插件里起 sidecar HTTP 服务来改配置：插件拿不到 Octop 的 DB/仓储层，最终仍要回调核心接口，反而多一个端口和鉴权面。

### 9.6 验收标准

- [ ] 修改 `trigger_keywords` / `group_reply_mode`：1 秒内生效，`is_connected` 保持 true，无 WebSocket 断开。
- [ ] 修改 `listen_port`：通道重建，旧端口释放，新端口监听，状态正确。
- [ ] 提交非法配置（类型错误 / 未知字段 / 反向缺端口）：返回 400，DB 与运行实例均不变。
- [ ] 制造注册失败（如端口被占用）：返回 422，配置回滚，旧实例恢复。
- [ ] 并发两次 PATCH 带同一 `if_version`：一次成功，一次 409。
- [ ] `GET .../config` 返回脱敏 token，且 `***` 回写不会覆盖真实 token。
- [ ] 审计记录能查到 actor / changed / result。

### 9.7 测试计划

| 层级 | 用例 |
|---|---|
| 单元 | merge 语义、`null` 删除、`***` 保持、version 计算、字段分类（hot/restart） |
| 集成 | PATCH → 通道重建 → `is_connected` 恢复；PATCH 非法 → 400 且无副作用 |
| E2E | 改关键词后，同一群里非 @ 消息从“不入队”变为“入队” |
| 回归 | 旧 `PATCH .../channels/{id}` 行为不变 |

## 10. 已实现 R2：跨部署形态安装/迭代脚本

### 10.1 背景

`deploy/install.ps1` 只覆盖一种部署形态（Windows 开发机 + SSH + Docker Octop），且硬编码了主机名、远程目录与容器名。实测（2026-10，systemd 裸机 Octop v1.0.2）至少存在两类形态差异：

| 维度 | Docker / compose | systemd 裸机（实测） |
|---|---|---|
| CLI 调用 | `docker exec <container> octop ...` | `/opt/octop/venv/bin/octop ...` |
| 插件上传目标 | 数据卷映射目录（如 `/opt/octop/data/uploads`） | `/root/.octop/uploads/` |
| 重启生效 | `docker restart <container>` | `systemctl restart octop` |
| 日志验证 | `docker logs <container>` | `/root/.octop/logs/octop.log` |
| Python / websockets | 容器镜像自带 | 宿主机 venv（实测 3.12.15 / 15.0.1） |

理想生效路径是 `POST /api/plugins/reload` 热重载（免重启，服务不中断），但需要管理员凭证；无凭证时降级为重启。CLI 的 `octop plugin reload` 是离线操作，同样需要重启 `octop run`。

### 10.2 方案

新增 `deploy/install.sh`（bash，macOS/Linux 通用，替代 install.ps1 成为默认部署入口；install.ps1 保留为 Windows 辅助脚本，只做上传/安装/重启，不创建通道）：

- `--mode auto|docker|systemd`：auto 探测——systemd 服务 `octop.service` 存在且 active → systemd；存在名为 octop 的容器 → docker；两者都有或都没有则报错，要求显式指定。
- `--host user@server`：远程模式，内部走 scp/ssh；缺省在本地执行。
- `--container <name>` / `--uploads-dir <path>` / `--cli <path>`：覆盖各自的默认值。
- `--reload-token <token>`（或环境变量 `OCTOP_RELOAD_TOKEN`，更推荐：命令行参数在进程列表里可见）：优先对 API 端口 `POST /api/plugins/reload`，成功则跳过重启；失败（401/超时）降级为重启并提示。
- 流程：上传插件目录（排除 `__pycache__`）→ `plugin install --force`（channel + tools 两个插件）→ reload/重启 → grep 日志 `applied N plugin channel kind(s)` 验证 → 汇总输出。
- `--sync-only`：开发迭代快捷模式，只处理指定插件目录，本机改完一条命令到服务器生效。

### 10.3 验收标准

- [ ] Docker 形态与 systemd 形态各完整跑通一次安装并验证加载。
- [ ] auto 探测在两种形态下判断正确，混合环境（容器 + systemd 并存）要求显式指定。
- [ ] 提供 `--reload-token` 时不重启即生效，服务无中断。
- [ ] 失败路径（上传目录不可写、CLI 不存在、重启后日志无 applied）给出可读错误并退出非 0。

## 11. v0.4.0 新增（R3）：事件、巡检、HTTP 回退

六项能力，全部在本仓库内完成并有离线/E2E 覆盖：

| 能力 | 入口配置 | 机制要点 |
|---|---|---|
| notice/request 事件 | `auto_accept_friend_request` / `auto_accept_group_invite` / `poke_triggers_reply` | `_handle_notice_event()` 全部 best-effort（异常只记日志）；好友/群邀请调 `set_*_add_request(approve=true)`；戳机器人合成 `_octop_poke` 事件入队 |
| 入站发送人白名单 | `allowed_senders` / `blocked_senders` | 在 `_handle_message_event()` 自查/去重之后过滤；黑名单优先 |
| HTTP 备用通道 | `http_api_url`（+可选 `http_api_token`，空则复用 `token`） | `_call_api()` WS 不可用时走 `POST {url}/{action}`；标准库 `urllib` + `asyncio.to_thread`；覆盖主动/被动/outbox 三条出站路径 |
| 群巡检 | `monitor_enabled` + 5 个高级参数 | 非触发群消息进 `_monitor_buffer`（每群 50 条）；`_monitor_sweep()` 按 min_messages / keywords / gap / max_per_hour 节流后合成 `_octop_monitor` 摘要事件唤醒 Agent |
| 巡检跳过哨兵 | — | 摘要文本要求 Agent 无需介入时只回「〔巡检跳过〕」；`_send_text()` 检测 `metadata.monitor` + 哨兵文本则静默丢弃（依赖入站 metadata 随会话回流出站，与 `reply_with_quote` 同一机制） |
| 引用 / 合并转发 / 结构化 at | `resolve_forward`（入站转发展开） | `push_segments()` 把 `reply` 段置首；`push_forward()` 走 `send_group_forward_msg`（仅群）；`parse_inbound()` 输出 `metadata.mentions`；`_resolve_forward_payload()` 在入队前调 `get_forward_msg` 附加 `_forward_messages` |

注意事项：

- **巡检触发即消耗一轮 LLM**——节流参数是 Token 成本的主控开关，`monitor_keywords` 建议配置以减少无意义唤醒。
- **HTTP 回退不解决登录态**：`is_connected` 仍要求 WS + 登录确认；HTTP 只给出站兜底。
- **戳一戳/自动通过默认关闭**：未经真实 NapCat 验证的行为一律 opt-in。

## 12. v0.5.0 新增（R4）：outbox 持久化与反向多账号

| 能力 | 入口配置 | 机制要点 |
|---|---|---|
| outbox 持久化 | `outbox_persist`（默认 true）/ `outbox_persist_dir` | `_persist_outbox()` 在 enqueue / flush / stop 时原子落盘（tmp+replace，`{dir}/{channel_id}.json`）；`_load_outbox()` 在构造时恢复，epoch↔monotonic 换算保留 TTL 年龄；探针通道（`__probe__`）不持久化；损坏文件忽略并告警 |
| 反向多账号 | `multi_account`（默认 false） | 连接会话化为 `_ConnSession`（key/ws/self_id/login/probe/pending 独立）；多模式同 self_id 重连顶旧、不同账号共存；单模式保留"新顶旧"语义；聚合计（`_ws`/`_connected_transport`/`_login_confirmed`/`_self_id`）由 `_refresh_state()` 从会话表派生，兼容全部旧测试 |
| 账号路由 | subject `metadata.self_id` / 工具 `account` 参数 | `_select_session(account)` 严格匹配；主动发送（push）指定未连接账号直接报错不排队；被动回复在账号会话消失时降级回主连接；outbox 条目携带 account，补发走原账号 |
| R1 热生效写入 | `patch_channel.py --api-url/--api-token` | 走核心既有 `PATCH /api/v1/agents/{a}/channels/{c}`（`update_channel` → 注销 → 重建，配置立即生效），Bearer JWT 鉴权；免 `--restart` |

注意事项：

- **多账号仅反向模式**；正向模式一条 `ws_url` 对应一个账号，多账号请开多条通道或多条反向连接。
- **探测循环修复**：`session.login_confirmed` 置位后必须 `_refresh_state()`，否则聚合计不更新（E2E 抓出过一次，勿回退）。
- **持久化是文件级而非事务级**：进程崩溃可能丢最后一批未落盘消息；去重缓存不持久化（真实 NapCat 断线不重推，mock 重推属测试夹具行为）。

## 13. v0.6.0 审计修复（R5）

对 v0.5.1 做了一次完整代码审计（含对部署环境的受控动态验证），发现的问题全部修复，并且每一条都有一个会在旧实现上失败的用例。下表的“用例”在 `tools/test_plugin_regression.py`（`core:` 前缀的在 `tools/test_plugin_core_integration.py`，`e2e:` 前缀的在 `tools/test_plugin_e2e.py`）。

| 方面 | 问题 | 修复 | 用例 |
|---|---|---|---|
| 收包 | 读循环在处理事件时等待 API 应答（如展开合并转发），而应答只能由它自己收 → 死锁到超时 | 读循环只收帧；echo/meta 就地处理，事件进会话队列由 worker 依次处理；连接一关就 `_fail_pending()`，已读入的事件限时排空 | `forward_receive_loop_never_blocks_on_api_calls` / `reverse_receive_loop_never_blocks_on_api_calls` / `drain_processes_events_already_read_when_peer_closes` |
| 收包 | 多账号时追问 `get_forward_msg` 走了主连接而不是收到事件的连接 | `_call_via(session)` | `multi_account_forward_lookup_uses_the_receiving_session` |
| 收包 | `get_forward_msg` 只发 `message_id`，且只认一种返回结构 | 同时发 `id` 与 `message_id`；兼容列表 / `{"messages"}` / `{"message":[node]}` | `get_forward_msg_accepts_real_world_shapes` |
| 反向 | 无 token 时对 `0.0.0.0` 开放监听：任何人可顶掉 NapCat 并伪造事件 | 非回环无 token 拒绝监听（`reverse_allow_no_token` 显式放开） | `reverse_listener_refuses_open_non_loopback_bind`；`core: reverse_listener_without_token_is_refused_not_crashed` |
| 反向 | 先升级再以 1008 关闭；非 ASCII token 令鉴权比较抛异常 | 握手前 404/401/503；UTF-8 字节比较 | `reverse_handshake_rejected_before_upgrade` / `non_ascii_token_never_raises` |
| 反向 | 多账号会话数无上限 | 上限 32，已知账号重连不受限 | `reverse_session_cap_only_blocks_new_accounts` |
| 重试 | 发送超时被当作“没发出”，对慢 NapCat 无限重发 | 超时占用一次尝试并至少等一个请求超时；总数受 `outbox_max_attempts` 限制 | `slow_ack_does_not_resend_forever`；`e2e: scenario_slow_ack` |
| 限速 | outbox 补发绕过限速，重连后一次倾泻整个积压 | 补发每条取一个限速槽；一次逻辑消息只取一个槽 | `outbox_replay_takes_a_rate_slot_per_message`；`core: a_long_reply_consumes_one_rate_slot` |
| 出站 | 长回复一条失败后仍继续发后面的分片；引用发送超时后补发无引用副本 | 分片失败即停；传输异常/超时不补发副本 | `passive_long_text_is_split_per_message_and_stops_on_failure` / `quoted_send_timeout_is_not_retried_unquoted` |
| 媒体 | 工具的 `image_url` 可让 Octop 读取服务器上任意文件 | 仅 `media_allowed_dirs` 内的普通文件；realpath、`O_NOFOLLOW`、大小与魔数检查 | `local_media_is_confined_to_allowed_dirs` |
| outbox | 状态文件权限宽松；加载时信任文件内容，可被改成任意 OneBot 动作 | 文件 `0600`；动作白名单、参数/字节预算校验 | `outbox_state_file_is_private_and_validated` |
| HTTP 回退 | 读取响应无上限；无应答超时被当成连接失败（等同“没发出”） | 响应上限 2 MiB；超时按 `TimeoutError`（送达未知）处理 | `http_fallback_has_response_cap_and_distinct_timeout` |
| 提示注入 | 巡检摘要把群友原文直接拼进给模型的提示 | 单行中和 + 随机围栏 + “围栏内是数据”声明 | `monitor_digest_neutralizes_untrusted_text` |
| 入站策略 | 戳一戳、自动通过好友/群邀请绕过发送人黑白名单；戳一戳可刷屏 | 共用 `_sender_allowed()`；戳一戳 10 秒冷却 | `poke_honours_sender_policy_and_cooldown` / `auto_accept_obeys_sender_policy` |
| 入站 | 去重键只含 message_id，跨账号/会话会误吞；CQ 字符串里的转义（`&#91;`、`&amp;`）原样进入文本；Unicode 数字被当 QQ 号 | 键含 `self_id\|类型\|会话\|message_id`；CQ 转义还原且只还原一次；ID 仅接受 ASCII 数字 | `message_dedupe_is_scoped_by_account_and_chat` / `cq_codes_are_unescaped_once` / `numeric_ids_ignore_unicode_digits` |
| 群上下文 | metadata 缺 `sender_id`，群历史发送者全是 `unknown`；`always` 被核心降级为 `mention` | 写入 `sender_id`；`always` 同时声明 `visibility=all` | `sender_policy_and_sender_id_metadata` / `always_mode_declares_full_visibility`；`core: mention_mode_buffers_chatter_with_the_real_group_policy` / `always_mode_is_not_degraded_by_the_core` |
| 配置 | 表单字符串（`"3002"`、`"true"`、空值）让构造抛 `ValueError` | `__post_init__` 统一转换，非法值回退默认并告警 | `config_values_are_normalized`；`core: dashboard_payload_builds_a_working_channel` |
| 日志 | URL 里的凭据进了日志和状态栏 | `_redact_url()` | `urls_are_redacted_in_logs_and_status` |
| 注册表 | 旧实例停止时把同 ID 的新实例踢出注册表 | 按实例身份注销 | `live_registry_is_identity_checked` |
| 工具 | 多通道/多 Agent 时按“第一个通道”发送，Agent 之间可串用；`group_id` 不校验 | 每 Agent `channel_id` 绑定（模型参数不能改道）；未绑定多通道拒绝；ID 仅接受 ASCII 数字 | `tools_are_bound_to_one_channel_per_agent` / `tools_register_a_channel_binding_field`；`core: tools_honour_the_per_agent_channel_binding` |
| 脚本 | 令牌写进命令行/远程 shell 引号拼接；`/tmp` 固定路径；示例里带现网 IP 与 ID | 令牌走环境变量或 stdin；`mktemp -d`；单引号校验；示例改占位符；`install.ps1` 参数化且不创建通道 | 手工/静态检查（无单测） |

### 13.1 取舍与注意

- **兼容性**：默认行为有三处收紧——反向监听非回环地址必须有 token；本地图片路径默认不可用；多通道且未绑定时工具拒绝发送。升级后若现网依赖旧行为，需要分别配置 `token` / `media_allowed_dirs` / 工具的 `channel_id`。
- **投递语义**：至少一次，不是恰好一次（见 design.md 7.1）。
- **未覆盖**：`install.ps1` 没有在 Windows 上实跑；`get_forward_msg` 的返回结构只对 mock 与单元用例验证过；没有接入真实 QQ 账号做端到端联调。

## 14. 发布与交接

### 发布

1. 更新 `plugins/*/plugin.yaml` 的 `version`，同时更新 README 徽章与本文件开头的“当前版本”（`test_plugin_regression.py` 的 `docs_and_manifests_agree_on_version` 会检查三者一致）。
2. 更新 [ROADMAP.md](../ROADMAP.md) 的里程碑与待办状态。
3. 打 tag：`git tag vX.Y.Z && git push origin vX.Y.Z`。
4. `gh release create vX.Y.Z --generate-notes`。
5. 部署验证：`plugin install --force` → `restart` → 日志确认 `applied ... onebot11`。

### 交接清单

- [ ] 仓库权限：至少 `write`，CI 可读。
- [ ] 部署环境：Octop 数据目录、容器名、Compose 文件位置（**不要写进仓库**）。
- [ ] NapCat 账号：由业务方持有，开发者只需要测试号。
- [ ] 现网通道 ID 与 Agent ID、服务器地址：单独记录，不提交到仓库（示例一律用占位符）。
- [ ] 已知问题：见第 7 节；未完成事项：见第 8 节路线图。
- [ ] 密钥：仓库内不得出现 token；配置只存 Octop DB。

## 15. 参考

- Octop：<https://github.com/TencentCloud/Octop>
- octop-gateway：<https://github.com/TencentCloud/octop-gateway>
- OneBot 11 协议：<https://github.com/botuniverse/onebot-11>
- NapCat：<https://github.com/NapNeko/NapCatQQ>
- 项目内文档：[design.md](design.md) · [napcat.md](napcat.md)
