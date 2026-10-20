# NapCat 配置指南

本插件只依赖 OneBot 11 协议，NapCat 是最常用的实现。本文给出与本插件配合的最小配置；不同 NapCat 版本的字段名可能略有差异，**优先使用 WebUI 创建，不要手写 JSON**。

## 1. 部署与登录

Docker 方式（示例）：

```yaml
services:
  napcat:
    image: mlikiowa/napcat-docker:latest
    container_name: napcat
    restart: unless-stopped
    ports:
      - "127.0.0.1:6099:6099"   # WebUI 只绑本机；远程管理请走 SSH 隧道或带访问控制的反向代理
    volumes:
      - ./napcat/config:/app/napcat/config
      - ./napcat/qq:/app/.config/QQ
    networks:
      - octop
```

首次启动后：

1. 打开 `http://<host>:6099`，使用启动日志里的临时 token 登录 WebUI；
2. 用手机 QQ 扫码登录机器人账号；
3. 确认 `docker logs napcat` 中出现登录成功的账号信息。

> 登录态保存在挂载出来的 QQ 配置目录中，重新创建容器不需要重复扫码。

> **安全提示**：WebUI 能登录、改配置、读到账号上的聊天，不要把 6099 暴露到公网；OneBot 的 3001（正向）同理，只在容器网络 / 内网里开放，**不要写进 `ports:`**（同一 Docker 网络里的容器按名字访问即可）。两个端口一旦公网可达，任何人都可以冒用该 QQ 号收发消息。

## 2. 与 Octop 的网络关系

| 部署方式 | 插件 `ws_url` / NapCat URL |
|---|---|
| 同一 Docker 网络 | `ws://napcat:3001`（正向）/ `ws://octop:3002/onebot/v11/ws`（反向） |
| 同机不同网络 | 使用宿主机 IP + 映射端口 |
| 跨主机 | 使用内网可达 IP，防火墙只放行 Octop 所在主机；必须配置 token，并优先考虑 VPN / 内网隧道 |

容器名能否解析，取决于两者是否在同一个自定义网络里：

```bash
docker network connect <octop_network> napcat   # 临时接入
# 想持久化，写进 docker-compose.override.yml
```

在 Octop 容器内验证连通性：

```bash
docker exec octop python -c "import socket; s=socket.create_connection(('napcat',3001),3); print('ok')"
```

## 3. 正向：NapCat 作为服务端

WebUI → 网络配置 → 新建 → **WebSocket 服务器**

| 项 | 建议值 |
|---|---|
| 启用 | 是 |
| Host | `0.0.0.0` |
| Port | `3001` |
| Token | 与 Octop 通道 `token` 一致（必填，使用强随机值） |
| 消息格式 | `array` |
| 上报自身消息 | 关闭 |
| 心跳间隔 | `30000` |

等价的 `onebot11_<uin>.json` 片段：

```json
{
  "network": {
    "websocketServers": [
      {
        "name": "octop",
        "enable": true,
        "host": "0.0.0.0",
        "port": 3001,
        "messagePostFormat": "array",
        "reportSelfMessage": false,
        "token": "<token>",
        "enableForcePushEvent": true,
        "debug": false,
        "heartInterval": 30000
      }
    ]
  }
}
```

Octop 侧配置：

```json
{
  "connection_mode": "forward",
  "ws_url": "ws://napcat:3001",
  "token": "<token>"
}
```

## 4. 反向：NapCat 作为客户端

WebUI → 网络配置 → 新建 → **WebSocket 客户端**

| 项 | 建议值 |
|---|---|
| 启用 | 是 |
| URL | `ws://octop:3002/onebot/v11/ws` |
| Token | 与 Octop 通道 `token` 一致 |
| 消息格式 | `array` |
| 上报自身消息 | 关闭 |
| 心跳间隔 | `30000` |
| 重连间隔 | `3000` |

等价 JSON：

```json
{
  "network": {
    "websocketClients": [
      {
        "name": "octop-reverse",
        "enable": true,
        "url": "ws://octop:3002/onebot/v11/ws",
        "messagePostFormat": "array",
        "reportSelfMessage": false,
        "token": "<token>",
        "debug": false,
        "heartInterval": 30000,
        "reconnectInterval": 3000
      }
    ]
  }
}
```

Octop 侧配置：

```json
{
  "connection_mode": "reverse",
  "listen_host": "0.0.0.0",
  "listen_port": 3002,
  "listen_path": "/onebot/v11/ws",
  "token": "<token>"
}
```

反向模式适合 NapCat 位于 NAT 后、或不想在 NapCat 侧开放入站端口的场景。NapCat 断开后会自动按 `reconnectInterval` 重连，Octop 侧监听器始终在线。

**反向监听的安全规则**：

- `listen_host` 不是回环地址（如默认的 `0.0.0.0`）时，通道**必须配置 `token`**，否则拒绝监听，状态栏显示“反向模式未配置 token，已拒绝监听 …”。原因：无 token 的开放端口会让任何能连到它的人顶掉真实 NapCat 的连接，并向 Octop 伪造聊天事件。
- 只在本机使用时可以把 `listen_host` 设成 `127.0.0.1`，此时可以不配 token。
- 网络完全可信、又确实不想配 token 时，才显式开启 `reverse_allow_no_token`（不推荐）。
- 握手不合格的连接在升级为 WebSocket 之前就会被 HTTP 拒绝：路径不符 `404`、token 不符 `401`、多账号连接数达上限（32）`503`。

## 5. 消息格式必须用 array

插件按 OneBot 11 的**数组格式**解析 `message`：

```json
[{"type": "text", "data": {"text": "你好"}}]
```

如果 NapCat 配置成 `string`，事件里的 `message` 会变成 CQ 码字符串。插件虽然保留了 CQ 解析回退，但 `array` 更稳定，也避免 CQ 转义带来的歧义。

## 6. 验证清单

1. NapCat WebUI 显示账号在线；
2. Octop 通道状态为“已连接”（或日志出现 `OneBot11 WS connected` / `reverse WS client connected`）；
3. 私聊机器人发送“你好”，日志出现入站事件；
4. 群里 @ 机器人，确认走 `send_group_msg`；
5. 断开 NapCat 再恢复，确认 Octop 自动重连且无需重启。

## 7. 常见问题

| 现象 | 原因 | 处理 |
|---|---|---|
| Octop 报 401 / 403 | 两端 token 不一致 | 重新对齐 token |
| NapCat 反向连接被拒（HTTP 401 / 404 / 503） | token 不匹配 / 路径不符 / 多账号连接数已满 | 检查 token、`listen_path`（NapCat URL 的路径要与之相同）和连接数 |
| Octop 状态栏“反向模式未配置 token，已拒绝监听” | 在非本机地址监听却没有 token | 配置 `token`；仅本机使用可改 `listen_host=127.0.0.1` |
| 群消息只收到一部分 | `reportSelfMessage` 打开导致自回环 | 关闭上报自身消息 |
| 发送成功但 QQ 没显示 | 账号被禁言 / 风控 / 不在群 | 检查账号状态与群权限 |
| 图片发送失败 | 超过 10 MB，或给的是服务器本地路径但没有授权目录 | 压缩图片；优先用 http(s) URL；本地文件需要管理员在通道里配置 `media_allowed_dirs` |

> 个人 QQ 的自动化存在风控风险，避免高频群发、短时间大量加群/退群。插件内置的 20 条/60 秒限速只能约束单通道，不能替代账号侧的风控策略。
