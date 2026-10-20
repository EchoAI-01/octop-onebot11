# 开发与接手指南

> 面向接手本项目的开发者。目标：**1 小时跑起来，半天能改第一个需求**。
> 使用说明见 [README](../README.md)，NapCat 配置见 [napcat.md](napcat.md)，实现细节见 [design.md](design.md)。

## 1. 项目速览

| 项 | 值 |
|---|---|
| 定位 | Octop 的 OneBot 11 / NapCat 通道插件，让 Octop 接入个人 QQ |
| 组成 | 1 个 channel 插件 + 1 个 tool 插件 + 测试/部署脚本 |
| 代码量 | 约 1.3k 行 Python（通道 900+，工具 120+，测试 300+） |
| 运行环境 | Octop 进程内，由插件加载器动态导入 |
| 外部依赖 | 仅 `websockets`（Octop 已内置，验证 15.0.1） |
| 支持版本 | websockets 13–17，Python 3.12+ |
| 当前版本 | v0.2.0 |

### 仓库结构

```
octop-onebot11/
├─ plugins/
│  ├─ octop-onebot11/            # channel 插件，入口 setup(ctx)
│  │  ├─ plugin.yaml             # id / kind: channel / entry
│  │  └─ main.py                 # 配置类 + 通道实现 + 字段 schema
│  └─ octop-onebot11-tools/      # tool 插件，入口 setup(ctx)
│     ├─ plugin.yaml             # id / kind: tool
│     └─ main.py                 # onebot11_send_group / _private
├─ tools/
│  ├─ mock_napcat.py             # 假 NapCat：WS 服务端 + action 响应
│  ├─ test_plugin_offline.py     # 纯逻辑测试，不需要 Octop
│  └─ test_plugin_e2e.py         # 真实 WS 测试，含反向模式
├─ deploy/
│  ├─ install.ps1                # Windows 一键上传/安装/建通道
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

python tools/test_plugin_offline.py     # 不需要 Octop，秒级
python tools/test_plugin_e2e.py         # 起 mock NapCat，约 5 秒
```

`test_plugin_offline.py` 用轻量 stub 替换 `octop_gateway`，所以**不需要安装 Octop** 就能验证解析、路由、配置和鉴权逻辑。改动协议层时优先补这里的断言。

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
python -m py_compile plugins/octop-onebot11/main.py plugins/octop-onebot11-tools/main.py tools/mock_napcat.py
python tools/test_plugin_offline.py
python tools/test_plugin_e2e.py
```

CI 会跑 Python 3.12 / 3.13 两个版本，本地至少保证 3.12 通过。

## 3. 代码地图

### `plugins/octop-onebot11/main.py`

| 区块 | 关键成员 | 说明 |
|---|---|---|
| 工具函数 | `_to_bool` / `_to_int` / `_normalize_path` | Dashboard 表单值是字符串，统一归一化 |
| 模块级 | `_LIVE_CHANNELS` / `get_live_channel()` / `build_group_subject()` / `build_private_subject()` | 供 tool 插件取活实例、构造主动发送目标 |
| 配置 | `OneBot11Config` | `ChannelConfig` 子类，`from_dict` 由 Octop 调用；`missing_credentials()` 按模式校验 |
| 生命周期 | `start()` / `stop()` / `is_connected` / `runtime_error` | `start()` 永不因连接失败抛错 |
| 正向 | `_run_forever()` / `_connect_once()` / `_receiver()` | 拨号、退避重连、收帧 |
| 反向 | `_serve_forever()` / `_handle_reverse_client()` / `_attach_client()` / `_detach_client()` | 监听、鉴权、单连接替换 |
| 登录态 | `_login_probe_loop()` / `_handle_meta_event()` | `get_login_info` + heartbeat 双来源 |
| 入站 | `parse_inbound()` / `_parse_message_content()` / `_parse_cq_string()` | OneBot 事件 → `InboundMessage` |
| 触发 | `_parse_keywords()` / `_keyword_hit()` / `_handle_message_event()` | @ / 关键词 / 私聊策略过滤 |
| 出站 | `_send_text()` / `_send_content()` / `_send_media()` / `_build_send_action()` | 文本分片、base64 媒体、群/私聊路由 |
| 协议 | `_call_api()` / `_dispatch_payload()` / `_fail_pending()` | `echo` 关联、超时、断线失败 |
| 字段 schema | `_FIELDS` / `setup()` | Dashboard 表单声明 |

### `plugins/octop-onebot11-tools/main.py`

| 成员 | 说明 |
|---|---|
| `_channel_module()` | 从 `sys.modules` 取 channel 插件模块 |
| `_resolve_channel()` | 取活实例并检查 `is_connected` |
| `onebot11_send_group()` / `onebot11_send_private()` | 构造 subject → `push_text()`，失败返回 `发送失败: ...` |
| `setup()` | `ctx.tool()` 注册，description 会影响模型是否调用，修改时注意措辞 |

### `tools/mock_napcat.py`

| 参数 | 作用 |
|---|---|
| `--host` / `--port` | WS 监听地址，默认 `127.0.0.1:3001` |
| `--token` | 校验 `Authorization`，空表示不校验 |
| `--logged-in` | `get_login_info` 返回有效 `user_id` |
| `--send-test-after N` | 启动 N 秒后推一条群 @ 测试消息 |
| `--log-file` | 把收到的 action 写入文件，E2E 用它断言 |

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

`_handle_message_event()` 再按 `group_reply_mode` / `private_reply_mode` 过滤。**未触发的群消息不会入队**，只进 `GroupContextManager` 缓存。

### 4.3 出站路由

`_build_send_action()` 用多个信号判断群/私聊，原因是 Octop 的 `message_keys.chat_type_from_message()` 只读 `metadata["chat_type"]`。改动这块时必须保留“多信号兜底”，否则历史会话会误发私聊。

两种失败语义：

| 路径 | 行为 |
|---|---|
| `_send_text()`（被动回复） | 捕获异常只记日志，不影响 Agent |
| `push_text(strict_push=True)`（工具主动发送） | 抛错，返回给 Agent |

### 4.4 配置加载

```
_FIELDS（表单 schema） → Dashboard 表单 → channels.config_json
                                         ↓
                    OneBot11Config.from_dict()（只取 dataclass 字段）
                                         ↓
                                 __post_init__ 归一化
```

- 表单只覆盖 `_FIELDS`；高级字段只能通过 CLI/JSON 写入。
- `from_dict` 支持 `field_aliases`（`access_token` → `token`）。
- 反向模式通过 `missing_credentials()` 跳过 `ws_url` 必填校验。

## 5. 开发约定

1. **不引入新依赖**。插件必须能在 Octop 自带环境里直接跑。
2. **不改 Octop 核心**。需要核心能力时，先确认 `PluginContext` 是否已提供；没有再考虑上游 PR。
3. **状态文案中文，日志英文**。`runtime_error` 会显示在 Dashboard；日志给运维 grep。
4. **新增配置项三步**：dataclass 字段 → `_FIELDS` 条目 → 离线测试断言。缺一不可。
5. **新增行为必须带测试**。协议/传输相关加 E2E，纯逻辑加离线断言。
6. **兼容旧版本**。改 `websockets` 调用时保留 13–17 的兼容分支。
7. **不打印 token**。日志里只允许出现是否配置，不允许出现值。

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

## 8. 路线图

优先级：P0 阻塞使用 / P1 影响体验 / P2 增强。

| 优先级 | 事项 | 价值 | 预估 | 依赖 |
|---|---|---|---|---|
| P0 | **配置热更新接口（R1，见第 9 节）** | 不再手动改库 + 重启 | 6–10 人日 | Octop 核心 PATCH |
| P0 | 发送 outbox + 重试 | 断线期间主动消息不丢 | 3–5 人日 | 需要持久化选型 |
| P1 | 群白名单 / 发送配额 | 防止 Agent 误发、降低风控 | 1–2 人日 | — |
| P1 | 主动发送支持图片 / @ | 工具能力对齐回复路径 | 2–3 人日 | — |
| P1 | 多账号（反向多连接路由） | 一个通道接多个 QQ | 3–5 人日 | 需要 self_id 路由 |
| P2 | 指标与健康检查 | 可观测性 | 1–2 人日 | — |
| P2 | 配置向导 / 表单联动 | 降低配置门槛 | 2–3 人日 | Dashboard 支持 |

## 9. 新需求 R1：通道配置热更新接口

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
  "channel_id": "01M5...",
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
  "channel_id": "01M5...",
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
| Phase 1 | 文档 + 脚本：`scripts/patch_channel.py` 读取现有配置、合并后调用旧 PATCH | 本仓库 | 1–2 人日 |
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

## 10. 发布与交接

### 发布

1. 更新 `plugins/*/plugin.yaml` 的 `version`。
2. 更新 README 变更说明。
3. 打 tag：`git tag v0.3.0 && git push origin v0.3.0`。
4. `gh release create v0.3.0 --generate-notes`。
5. 部署验证：`plugin install --force` → `restart` → 日志确认 `applied ... onebot11`。

### 交接清单

- [ ] 仓库权限：至少 `write`，CI 可读。
- [ ] 部署环境：Octop 数据目录、容器名、Compose 文件位置（**不要写进仓库**）。
- [ ] NapCat 账号：由业务方持有，开发者只需要测试号。
- [ ] 现网通道 ID 与 Agent ID：单独记录，不提交到仓库。
- [ ] 已知问题：见第 7 节；未完成事项：见第 8 节路线图。
- [ ] 密钥：仓库内不得出现 token；配置只存 Octop DB。

## 11. 参考

- Octop：<https://github.com/TencentCloud/Octop>
- octop-gateway：<https://github.com/TencentCloud/octop-gateway>
- OneBot 11 协议：<https://github.com/botuniverse/onebot-11>
- NapCat：<https://github.com/NapNeko/NapCatQQ>
- 项目内文档：[design.md](design.md) · [napcat.md](napcat.md)
