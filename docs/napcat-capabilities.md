# NapCat / OneBot 11 能力全集与通道差距（R12）

> 本文回答三个问题：NapCat **能收到什么**、**能发出什么**、**能控制什么**；本插件**已经做了哪些**、**还差什么**、**为什么没做**。
> 它是后续扩展 Agent 能力的依据：加工具之前先在这里查“NapCat 是否支持、有什么坑、是否值得做”。
>
> 配置 NapCat 见 [napcat.md](napcat.md)；开发计划见 [ROADMAP.md](../ROADMAP.md)。

## 0. 资料来源与可信度

| 项 | 说明 |
|---|---|
| 调研对象 | NapCatQQ `main` 分支 = tag `v4.18.41`（commit `859eb53`，2026-10-21），2026-10-22 读取源码 |
| 对照资料 | OneBot 11 标准（`botuniverse/onebot-11`）、go-cqhttp `docs/cqhttp.md`、NapCat 官方文档（napneko.github.io） |
| **方法** | **读源码，没有对 NapCat 实机发包。** 标注“已验证”的指 *源码中确有该行为*，不等于实测；标注“未验证”的是文档/推理 |
| 已实测的部分 | 本插件对真实 NapCat 的联调（登录、群/私聊收发、合并转发出站、断线重连）；以及 `get_record` 对 ffmpeg 缺失的失败形态（见 §6） |
| 时效 | NapCat 迭代很快，版本相关结论请以你部署的版本为准；下面 §7 给出了在目标环境里自检的办法 |

规模：**178 个 action**（OneBot 标准 37 / go-cqhttp 33 / NapCat 扩展 108）、**23 类消息段**、**15 种 notice_type**。这里不逐个罗列——完整清单见源码 `packages/napcat-onebot/action/router.ts`——只记录对本插件有意义的部分。

## 1. 能收到什么（入站）

### 1.1 消息段

“收”指 NapCat 推给我们的事件里会出现的段；“现状”指本插件对它的处理。

| 段 | 收到时的数据 | 插件现状 |
|---|---|---|
| `text` | `{text}` | ✅ 原样 |
| `at` | `{qq}`（QQ 号或 `all`） | ✅ `@机器人` 触发；其余转成 `@昵称` 文本并记入 `metadata.mentions` |
| `reply` | `{id}`（短消息 ID） | ✅ 记入 `reply_to`；自主参与把“回复机器人的消息”视为强信号 |
| `image` | `{file, url, summary, sub_type, file_size}`；**商城表情 mface 也以 image 段到达**（多出 `key/emoji_id/emoji_package_id`） | ✅ 下载后交给多模态模型（R11）；mface 当普通图片处理 |
| `record` | `{file, path, url?}`；NT silk 格式，`url` 仅在 packet 可用时才是 HTTP，否则是 NapCat 本机路径 | ✅ 经 `get_record` 转 mp3/wav 再转写；或 `fetch_ptt_text`（§3.4） |
| `video` | `{file, url, file_size}` | ⚠️ 只有链接/占位符，不抽帧不理解 |
| `file` | `{file(文件名), file_id, file_size, url?}`（url 仅 packet） | ⚠️ 占位符 `[文件: 名]`；不下载 |
| `face` | `{id, raw}` | ⚠️ 统一显示为 `[表情]`，不区分哪个表情 |
| `forward` | `{id}`，默认**无内容**（`parseMultMsg=false`） | ✅ 可选展开（`resolve_forward`，调 `get_forward_msg`）；嵌套转发无法展开（NapCat 限制） |
| `json` | `{data: ark JSON 字符串}`；音乐/联系人/小程序/分享卡片都是它 | ⚠️ 占位符 `[卡片消息]`，不解析 |
| `markdown` | `{content}` | ❌ 未识别（NapCat 独有） |
| `dice` / `rps` | `{result}` | ❌ 未识别（骰子/猜拳结果丢失） |
| `poke`（段） | `{type, id}` | ❌ 未识别；戳一戳走 notice（见下） |
| `onlinefile` / `flashtransfer` | 在线文件 / 闪传 | ❌ 未识别（NapCat 独有） |
| `xml` / `miniapp` | **NapCat 不会产生**（枚举里有，收发都未实现） | — |

没有转换器的 NT 元素（灰条、日历、钱包、小程序卡等）NapCat 会**直接丢弃**，不报错——所以“消息为空”有时只是 NapCat 没能转换。

### 1.2 事件

事件字段：所有事件有 `time`、`self_id`、`post_type`；消息事件的 `sender` **只有** `{user_id, nickname, card}`（群里另有 `role`），**没有** 性别/年龄/地区/等级/头衔。要这些得另调 `get_group_member_info`。

| 事件 | 触发 | 插件现状 |
|---|---|---|
| `message` | 群/好友/群临时会话消息 | ✅ |
| `message_sent` | 自己发的消息的回显 | ❌ **收不到**——只有适配器配置 `reportSelfMessage:true` 才推（HTTP Server 没有该选项）。本插件靠“发送成功回执 + 本地历史库”记录机器人自己的发言，不依赖它 |
| `notice.group_recall` / `friend_recall` | 撤回 | ⚠️ 仅记日志 |
| `notice.notify.poke` | 戳一戳（群：`group_id/user_id/target_id`） | ✅ 可选触发回复（`poke_triggers_reply`，10 秒冷却） |
| `notice.group_increase` | 入群（含机器人自己入群） | ⚠️ 仅记日志 |
| `notice.bot_offline` | **被踢下线**（NapCat 自定义） | ✅ 通道状态转“需要重新登录”，不再假装在线（0.7.0 新增） |
| `notice.group_ban` / `group_admin` / `group_decrease` / `group_upload` / `group_card` / `essence` | 禁言、管理员变更、退群/被踢、群文件、改名片、设精华 | ❌ 未处理（可转成 Agent 可见的系统提示，见 §5） |
| `notice.group_msg_emoji_like` | 有人给消息贴表情 | ❌ 未处理 |
| `notice.notify.title` / `group_name` / `profile_like` / `input_status` / `gray_tip` | 头衔、群名变更、资料点赞、对方正在输入、未知灰条 | ❌ 未处理 |
| `notice.friend_add` / `online_file_*` | 新好友、在线文件传输 | ❌ 未处理 |
| `request.friend` / `request.group(add/invite)` | 好友申请、加群申请、被邀请入群 | ✅ 可选自动通过（`auto_accept_*`），同样遵守发送人黑白名单；不自动通过时只记日志 |
| `meta_event.heartbeat` / `lifecycle` | 心跳（`status.online`）、连接 | ✅ 登录态判定的来源之一 |

NapCat **没有实现**的标准事件：`notify.lucky_king`、`notify.honor`、`offline_file`、`client_status`；群消息也没有 `anonymous`。

## 2. 能发出什么（出站）

| 段 | NapCat 支持 | 插件现状 |
|---|---|---|
| `text` / `at` / `reply` / `image` / `record` / `video` / `file` | ✅ | ✅ 主动/被动均可；图片、语音、视频走 `base64://` 或 http(s)，本地路径受 `media_allowed_dirs` 约束 |
| `node`（合并转发） | ✅，**必须整条消息全是 node**；node 的 `nickname` **必填**（`name` 只是别名，缺 `nickname` 会失败）。packet 可用时伪造转发，否则退化为“先把节点消息真发到机器人自己的私聊再转发”（会在机器人私聊里留下真实消息） | ✅ 仅群；节点同时带 `nickname/name/uin`（0.7.0 修复了缺 `nickname` 的问题） |
| `face` | ✅，id 必须在 NapCat 的表情表内，否则被丢弃 | ❌ 不发 |
| `mface` | ✅ 需要 `emoji_package_id/emoji_id/key/summary` | ❌ 不发 |
| `json` / `music` / `contact` | ✅ 音乐卡片依赖第三方签名服务 | ❌ 不发 |
| `dice` / `rps` | ✅ | ❌ 不发 |
| `markdown` | 源码注释“Need signing”，效果**未验证** | ❌ 不发 |
| `location` | **发送是桩**（固定发“测试”文本） | 不要用 |
| `xml` / `miniapp` / `poke` / `onlinefile` / `flashtransfer` | 不可发 | — |

`file` 参数的来源（image/record/video/file 通用）：本地绝对路径、`file://`、`http(s)://`、`base64://…`、`data:…;base64,…`、NapCat 的文件 token。**URL 与 base64 不能直接塞进 `get_image/get_file/get_record` 的 `file`**——那三个只认 NapCat 自己的 token 和本地缓存（见 §3.1）。

## 3. 关键语义与踩坑

### 3.1 取回媒体：`get_image` / `get_record` / `get_file`

| 点 | 结论 |
|---|---|
| 入参 | `file`（或 `file_id`）必须是入站段里的 `data.file`——NapCat 的**内存 token**（24 小时滑动过期、容量 5000、**重启即失**）。传 URL/base64 会被当成文件名去搜索，得到 `file not found` |
| `get_record` | `out_format` **必填**（mp3/amr/wma/m4a/spx/ogg/wav/flac），**恒返回 base64**；需要 NapCat 侧有可用的 ffmpeg 才能转码 |
| `get_image` / `get_file` | **只有** OneBot 配置里 `enableLocalFile2Url=true`（默认 false）时才带 `base64`；否则只有 NapCat 本机路径——跨机/跨容器部署时我们读不到那个文件 |
| 入站图片 `url` | 形如 `https://multimedia.nt.qq.com.cn/download?appid=…&fileid=…&rkey=…`，约 2 小时过期；`get_image`/`get_msg`/`nc_get_rkey` 可刷新 |
| 本插件的对策 | 图片：先用事件自带 `url` 直接下载（带浏览器 UA，仅限 QQ 官方域名与 NapCat 自身地址），失败再 `get_image`；语音：`get_record`（mp3→wav→不带格式）→ 下载 → ASR |

> **部署建议**：NapCat 与 Octop 不在同一台机器/容器时，在 NapCat 的 OneBot 网络配置里打开 `enableLocalFile2Url`，`get_image` 才能返回可用的 base64。

### 3.2 消息 ID

- `message_id` 是 NapCat 用 `md5(msgId|chatType|peerUid)` 派生的**短整数**，存在**进程内 LRU（5000 条，所有会话共用）**，**不落盘，重启即失**。
- `delete_msg`、`set_msg_emoji_like`、`set_essence_msg`、`forward_*_single_msg` **只认短 ID**；`get_msg`、`fetch_ptt_text`、`mark_*_as_read` 也认 NT 长 ID。
- ID 不在 LRU 里时：撤回报 `Recall failed`、贴表情报 `msg not found`；**`reply` 段会被静默丢弃（消息照发，只是不带引用）**。
- 由此推出两条设计原则（已落实）：① 插件把**自己发出消息的 ID** 与全部入站消息一起落入本地 SQLite，撤回/贴表情前先在本地库核对“这条消息是否属于该会话、是否机器人自己发的”；② 对“消息不存在”类错误统一降级为可读提示，不重试。

### 3.3 `retcode`：**1200 不是限流**

| 通道 | 值 | 含义 |
|---|---|---|
| HTTP | 状态码 400 + `Invalid JSON` | 请求体不合法 |
| HTTP | 状态码 403 | token 不对（缺 token 也是 403） |
| HTTP | body `retcode:400` | 入参校验失败 |
| HTTP | body `retcode:200, status:failed` | 处理器抛异常 / 未知 action（**HTTP 状态仍是 200**，必须同时看 `status/retcode`） |
| WS | `1400` | 入参校验失败，或帧不是合法 JSON |
| WS | **`1200`** | **处理器抛异常（通用异常）**，`message` 是错误文本——权限不足、消息不存在、风控拒绝都可能是它 |
| WS | `1403` | 握手 token 错误（随后断开） |
| WS | `1404` | 未知 action（`不支持的API <name>`） |

源码里**没有限流机制，也没有 retcode 表**（`_async`/`_rate_limited` 后缀只是同步别名，不排队不限速）；风控由 QQ 服务端决定，错误文本由 NT 层透传。

> 早先文档曾把私聊时的 `retcode=1200` 解读为“私聊短时频率限制”，这是**误读**：1200 只表示“这次调用抛了异常”，具体原因看 `message`。本插件按 `message` 翻译成人话（见 `_action_failure`），不按 retcode 猜原因。

### 3.4 语音转文字：`fetch_ptt_text`

`fetch_ptt_text{message_id}` → `{text}`：调用 QQ（NT）自带的语音转文字，**不依赖 packet**，不用先下载音频。内部分三步（查消息 → 请求转写 → 再查消息），每步最多等 NapCat 的 `baseTimeout`（默认 10 秒）。

- 消息已不在 NapCat 缓存里 → `消息不存在`；消息不含语音 → `消息中不包含语音`；QQ 没给出文字 → `获取语音转文字结果失败`。
- 旧版 NapCat 可能没有这个 action（返回 1404 / `不支持的API`）。

**本插件的用法（0.7.0）**：配置了外部转写服务（`asr_base_url`）时先用它；未配置，或它失败时，若 `asr_napcat_fallback`（默认开）则用 `fetch_ptt_text`。好处是零配置、免费、语音不出本机；代价是识别质量取决于 QQ，且需要消息仍在 NapCat 缓存。遇到“不支持的API”会记住并在本次连接内不再询问。

### 3.5 其他容易踩的坑

| 坑 | 说明 | 本插件的处理 |
|---|---|---|
| `set_group_admin` 省略 `enable` | **等于取消管理员**（与 OneBot 默认相反） | 不暴露这个 action |
| `get_group_member_info` 的 `no_cache` | 缺省为 **true**（慢）；`get_group_member_list` 缺省 false | 群成员查询用 list，不带 `no_cache` |
| `set_group_ban` 预检 | 目标是群主 → `cannot ban owner`；目标是管理员而机器人不是群主 → `cannot ban admin` | 翻译成可读提示 |
| `set_group_card` | 设置后最多轮询 3 次校验缓存，未生效抛“设置群名片未生效” | 同上 |
| 非好友私聊 | 无共同群时可能“无法获取用户信息”或发不出；带 `group_id` 才能走临时会话 | 主动私聊失败时如实返回 |
| `delete_friend` | 失败也返回 ok（data 里 `valid:false`） | 未使用 |
| 恒定值 | `get_online_clients` 恒 `[]`、`check_url_safely` 恒 level 1、`can_send_*` 恒 true、`get_status.good` 恒 true | **不要用它们判断健康**；本插件用 `get_login_info` + heartbeat + `bot_offline` |
| 同步别名 | `<action>_async` 不会异步，`<action>_rate_limited` 不会限速 | 不使用 |
| `bot_exit` | 直接 `process.exit(0)` | 不使用 |
| `ocr_image` | 仅 Windows QQ | 不使用 |

### 3.6 packet 依赖

下面这些能力依赖 NapCat 的 packet 后端，**先用 `nc_get_packet_status` 探测**，不可用时返回的错误里带原因：

- 戳一戳（`send_poke`/`group_poke`/`friend_poke`）、群签到（`set_group_sign`）、设置头衔（`set_group_special_title`）
- 群文件下载链接（`get_group_file_url`/`get_private_file_url`）、群文件移动/重命名/转存
- `get_rkey` 系、`nc_get_user_status`、`send_packet`、群待办、小程序 Ark
- AI 声聊（`get_ai_characters`/`get_ai_record`/`send_group_ai_record`，**仅群**，且 `send_group_ai_record` 只返回 `message_id:0`，无法撤回）

packet 可用要同时满足：QQ 版本在 NapCat 的 `napi2native.json` 白名单内、`napcat.json` 的 `packetBackend` 不是 `disable`、后端自检通过。官方文档里的“支持版本表”已过期，以源码为准。

### 3.7 超时与大小

- 发消息的超时 = `min(10 秒 + 媒体字节 ÷ 256KB/s, 30 分钟)`，可用 `send_*` 的 `timeout`（毫秒）覆盖。**插件侧的 API 超时不应小于它**，否则会出现“我们超时了、消息其实发出去了”——这正是本插件把超时当作“送达未知”、占用重试预算而不盲目重发的原因。
- WS 单帧上限 50 MB；大文件走本地路径或分块上传，不要塞 base64。视频文档上限 100 MB。
- NapCat 的 `host` 默认 `127.0.0.1`：Docker 里要改成 `0.0.0.0` 才能被容器外访问；`token` 为空即不鉴权。

## 4. 能控制什么（动作）

按“对 Agent 有没有用、风险多大”分类。**已提供**指本插件的 Agent 工具；**内部使用**指通道自己调用、不暴露给 Agent。

### 4.1 已提供给 Agent 的工具（13 个，0.7.0）

| 工具 | 用到的 NapCat action | 备注 |
|---|---|---|
| `onebot11_send_group` / `send_private` | `send_group_msg` / `send_private_msg` | 文本、图片、@、引用；断线进队列；返回 `message_id` 回执 |
| `onebot11_send_forward_group` | `send_group_forward_msg` | 仅群 |
| `onebot11_send_voice` | `send_*_msg`（record 段） | 语音条 |
| `onebot11_react` | `set_msg_emoji_like` | 默认回应“当前正在回复的消息”；表情名→QQ 表情 ID 映射；校验消息属于该会话 |
| `onebot11_recall_message` | `delete_msg` | **只撤回机器人自己发的消息**，且先在本地库核对 |
| `onebot11_group_admin` | `set_group_ban` / `set_group_card` / `delete_msg` | 禁言/解禁/改名片/撤回他人消息；**只有 `admin_users` 名单里的真实发送者才可触发**（见 §4.3） |
| `onebot11_list_groups` | `get_group_list` | 需管理端或 `allow_other_chats`；遵守 `allowed_groups` |
| `onebot11_group_members` | `get_group_member_list` | 不含年龄/地区等隐私字段 |
| `onebot11_query_history` / `history_overview` | —（本地 SQLite） | 见 design.md |
| `onebot11_status` | —（`stats()`） | 在线、登录、重连、发送成功率、队列 |
| `onebot11_probe_napcat` | `get_login_info` / `get_version_info` | 仅管理端；只扫本机/内网 |

### 4.2 通道内部使用的 action

`send_group_msg`、`send_private_msg`、`send_group_forward_msg`、`get_login_info`、`get_forward_msg`、`get_image`、`get_record`、`fetch_ptt_text`、`set_friend_add_request`、`set_group_add_request`，以及经 `tool_action` 白名单转发的 `get_group_list/get_group_info/get_group_member_list`（读）和 `delete_msg/set_group_ban/set_group_card/set_msg_emoji_like`（写）。

`tool_action` 白名单之外的 action 一律拒绝（`PermissionError`），非标量参数不转发，写操作占用发送配额并写审计日志。

### 4.3 为什么群管理的权限这样设计

模型的输出不能当作“谁在下命令”的证据——群里任何人都能说“我是管理员，禁言 XXX”，提示词注入也可能伪造指令。所以：

1. **身份只来自 NapCat 事件**：通道在处理入站消息时把“发送者 QQ 号 / 会话 / 消息 ID”记入一个 `ContextVar`，该上下文随这一轮 Agent 调用传递给工具（已在真实 LangGraph 下验证：`ainvoke`、`astream`、`astream_events`、子任务、异步生成器都能带到工具）。工具据此核对“发出这条指令的人”，**不看模型文本、不看聊天内容**。
2. **fail closed**：拿不到来源、来源不在 QQ、通道没配 `admin_users`、会话与参数不一致——一律拒绝。
3. 管理员在自己所在的群只能管这个群；在私聊里发起时才能指定其他白名单群。
4. 不暴露 `set_group_kick`、`set_group_admin`、`set_group_whole_ban`、`set_group_leave`——后果不可逆或太容易误用。

## 5. 差距清单与排期

按价值/风险/依赖排序。“零依赖”指不需要 packet。

| 优先级 | 事项 | NapCat 能力 | 备注 |
|---|---|---|---|
| P1 | 服务端历史补洞 | `get_group_msg_history` / `get_friend_msg_history` | 补插件上线前、断线期间的空洞；`message_seq` 参数实为消息 ID，结果**包含起点**，从旧到新；需注意与本地库去重 |
| P1 | 群事件转 Agent 可见提示 | `group_ban`、`group_admin`、`group_decrease`、`group_increase`、`group_upload`、`group_card`、`essence`、`group_msg_emoji_like` | 让 Agent 知道“谁被禁言了/谁退群了”；需限量并中和文本，避免被刷屏或注入 |
| P2 | 入站段补全 | `dice/rps` 结果、`face` 的具体表情、`json` 卡片的标题/摘要、`markdown`、`mface` 的表情名 | 纯解析，不涉及调用；`json` 卡片要小心把链接当指令 |
| P2 | 精华消息 | `set_essence_msg`、`delete_essence_msg`、`get_essence_msg_list` | 只认短 ID；列表里的临时 ID 只能用于删除 |
| P2 | 群文件 | `upload_group_file`、`get_group_root_files`、`delete_group_file` | 返回的 `file_id` 不是 token；管理群文件需先列目录；下载链接依赖 packet |
| P2 | 公告 | `_send_group_notice` / `_get_group_notice` / `_del_group_notice` | 需要管理员权限语义 |
| P3 | 戳一戳 | `group_poke` / `friend_poke` | 依赖 packet；先探测，沿用现有冷却 |
| P3 | 点赞/已读 | `send_like`、`mark_msg_as_read` | 价值低 |
| P3 | AI 声聊 | `get_ai_characters` + `send_group_ai_record` | 仅群、依赖 packet、无法撤回 |
| 不做 | 踢人、设管理员、全员禁言、退群、`bot_exit`、`set_restart`、`clean_cache`、`send_packet` | — | 不可逆或风险过大；需要时由人在 QQ/NapCat 里操作 |
| 不做 | `ocr_image` | 仅 Windows | 平台受限；多模态模型能直接看图时无需要 |

> 本表的“P1/P2/P3”是**建议**；是否排期以 [ROADMAP.md](../ROADMAP.md) 为准。

## 6. 已经由这次调研改掉的问题

| 问题 | 来源 | 处理 |
|---|---|---|
| 合并转发的 node 只带 `name/uin`，缺必填的 `nickname`，在 NapCat 上会发送失败 | §2 | 节点同时带 `nickname/name/uin`；入站展开读取 `nickname`，回退 `name` |
| 把 `retcode=1200` 当作“私聊频率限制” | §3.3 | 订正文档；错误提示按 `message` 翻译 |
| 机器人被踢下线后通道仍显示“已连接” | §1.2 | 处理 `bot_offline` notice，状态转为“需要重新登录”，之后收到上线心跳自动恢复 |
| `get_image` 返回的是 NapCat 本机路径，Octop 读不到 | §3.1 | 优先用事件自带 URL 下载；文档里写明 `enableLocalFile2Url` |
| 语音转文字必须自己配第三方 ASR | §3.4 | 增加 `fetch_ptt_text` 兜底（`asr_napcat_fallback`） |
| 取消的 action 请求在会话里留下等待槽 | 复核时发现 | 取消/超时/异常统一在 `finally` 清理 |

`get_record` 在没有可用 ffmpeg 的 NapCat 上会失败（实测形态：处理器抛异常，`retcode=1200`）。插件按 `mp3 → wav → 不带格式` 依次尝试，仍失败则落到 `fetch_ptt_text`。

## 7. 在你的环境里验证

调研来自源码，部署差异很大。上线前建议在目标 NapCat 上自检：

1. `python scripts/probe_napcat.py`（或管理端对话里调用 `onebot11_probe_napcat`）：确认地址、令牌、登录账号与 NapCat 版本。
2. 对 NapCat 发 `nc_get_packet_status`：判断戳一戳/头衔/群文件链接等 packet 能力是否可用。
3. 给机器人发一条语音：看日志里是 `fetch_ptt_text` 还是外部 ASR 在工作；`onebot11_status` 里的 `voice_transcribed_napcat` 计数会增加。
4. 给机器人发一张图：`media_images_fetched` 增加即成功；失败看日志里的具体原因（域名不可信 / 过期 / NapCat 只返回本机路径）。
5. 尝试 `onebot11_react`、`onebot11_recall_message`：确认 NapCat 版本支持 `set_msg_emoji_like` / `delete_msg`，且 ID 仍在 LRU 里。

## 8. 未验证清单

以下结论来自推理或文档，**没有**源码或实机证据；动手做之前请先验证：

- 各 `Any` 返回（相册、闪传、在线文件、自定义表情、收藏、QQ 空间、`set_msg_emoji_like` 的原始结果）的字段
- 私聊里 `set_msg_emoji_like` 是否可用
- API 自发消息与 `message_sent` 是否会重复上报
- `group_msg_emoji_like` 对他人消息的覆盖面
- rkey 的精确有效期
- 服务端的发送频率风控阈值
- `get_forward_msg` 走 resId 时是否强依赖 packet
- `set_group_member_invite_policy` 的 policy 结构
- 多数能力的引入版本号
