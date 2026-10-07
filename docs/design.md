# jetcast 设计文档（第一期）

> 状态：第三版，已吸收 codex 与 pi 两轮评审，并按第一期实现同步。发布前整理为英文文档。

## 1. 定位

jetcast 是一个通用的 Go 库加 TypeScript SDK，把服务端事件实时推送给浏览器和其他客户端。浏览器经 WebSocket **直连 NATS**。心智模型参考 Laravel Broadcasting 与 Echo：服务端向频道广播事件，客户端监听频道；私有频道由应用的授权回调决定。

**目标**

- Laravel 式的频道、事件、授权回调、`toOthers`，Echo 式的客户端 API。
- 不假设部署形态：NATS 可以嵌入、独立或集群；WebSocket 可以由 Go、nginx 或负载均衡反代。库只接收 `*nats.Conn`。
- 授权由 NATS 服务端强制执行。
- 借助 JetStream：频道事件短期留存，客户端能发现漏收并补齐；无法证明完整时明确告知应用。

**非目标（第一期）**：长期消息存储与历史查询、presence、whisper、React hooks、模型广播约定、Go 反代组件与配置生成器（只提供示例）。

## 2. NATS 事实与对应决策（v2.15，已对照源码核实）

| 事实 | 决策 |
|---|---|
| 权限在连接时确定，不能在连接上更新；没有订阅时授权的钩子 | 私有频道默认走服务端中继；连接时授予的频道只是应用显式选择的优化 |
| callout 配置文件模式可用，能拿到 token、连接名、连接类型、客户端地址；配置模式下用户 JWT 的 `aud` 必须是目标账号名 | 凭据作为 token 传入；callout 按配置的账号签发 |
| callout 签发 JWT 中的用户级 `limits` 不生效；权限与 `exp` 生效 | 资源限制由账号级配置加库内业务级限制承担 |
| 订阅权限 deny `"> >"` 可禁止所有队列订阅 | 浏览器 JWT 带上该 deny |
| 一个 stream 只能有一条 RePublish；RePublish 只在 leader 执行，附加 `Nats-Sequence` 与该主题在转发时刻上一条已存消息的序号 `Nats-Last-Sequence` | 单一入口与出口命名空间；补发读取时由服务端重建序号 |
| nats.go 创建的 KV 默认开启 AllowDirect，读取可能由落后的副本应答 | KV 与事件 stream 都关闭 AllowDirect，读取走 leader |
| KV 每键 TTL 只能在 Create 时设置，Update 不能改 | KV 只用桶级 TTL 做垃圾回收，记录里保存绝对过期时间 |
| stream 只用按年龄、总字节淘汰时，总是先淘汰全局最旧的消息 | 补发完整性用“stream 最早序号是否越过游标”证明 |
| nats.js 在 WebSocket 下支持 no responders | 中继节点失联可立即发现 |

NATS 服务端最低 2.14.4，推荐 2.15，必须开启 JetStream。callout 的认证超时（`authorization.timeout`，默认 2 秒）需要容纳 `Authenticate`、`Grants` 与三次 KV 操作，建议设为 5 秒。

## 3. 架构

```
浏览器 / 客户端 ── jetcast SDK（nats.js v3）
      │ WebSocket，name=socket，token=应用凭据
NATS（嵌入 / 独立 / 集群，开启 JetStream）
      │ auth_callout
应用节点 × N（jetcast.Server）
      · callout 服务（队列组）
      · 请求处理（队列组）与节点专属请求（renew / leave）
      · 中继：订阅频道事件，转发给本节点登记的连接
JetStream
      · stream <S>：频道事件短期留存（RePublish 投递）
      · KV <S>_CONN：连接登记、用户索引、吊销标记
只发布事件的进程 ── jetcast.Publisher
```

## 4. 连接与身份

### 4.1 一次连接，一个 socket

- SDK 每次建立连接（包括断线重连）都生成**新的** socket id（22 位 base62），作为连接名传入，并以它作为该连接的主题命名空间。SDK 自己管理重连，不使用 nats.js 的自动重连。
- socket 登记只创建、不覆盖、不复用：同一个 socket 只能认证一次，之后再用它连接一律拒绝。登记在桶级 TTL 到期（大于最长连接寿命）后才被回收，因此吊销后的 socket 也不能被他人重新认领。

### 4.2 callout 流程

1. 校验 socket id 格式。
2. 调用 `Authenticate(ctx, AuthRequest{Token, Type, Host})`，得到 `User{ID, Session, ExpiresAt, Info, ConnectionTypes}`；`ID`、`Session` 只允许 `[A-Za-z0-9_-]`，长度 1–64。
3. 调用 `Grants(ctx, user)`。
4. 写入登记：`Create(s.<socket>)`（已存在则拒绝），记录用户、会话、`Info`、授予、绝对过期时间、server id、cid；再 `Put(u.<user>.<socket>)` 作为用户索引。
5. 读取吊销标记 `x.<user>` 与 `x.<user>.<session>`。标记的写入时间（JetStream 服务端时间）不早于 callout 请求的签发时间 `iat`（NATS 服务端时间，留 2 秒余量）时，说明认证进行期间发生了吊销：把登记标为已吊销并拒绝连接，客户端重试时会重新认证。
6. 签发 JWT 并回复。

与 `Disconnect` 的竞态由“先写后查”闭合：`Disconnect` 先写吊销标记、再按索引吊销登记；callout 先写登记与索引、再查吊销标记。两者交错时至少有一方能看到另一方的写入。

签发的 JWT：

| 字段 | 值 |
|---|---|
| `name` | `<user>|<socket>` |
| `aud` | 配置的账号 |
| `exp` | 与登记中的绝对过期时间相同：`min(User.ExpiresAt, now + MaxConnectionTTL)`，默认上限 1 小时 |
| `allowed_connection_types` | `User.ConnectionTypes`，默认只有 WEBSOCKET |
| 订阅 | 允许 `<p>.ev.pub.>`、授予模式对应的 `<p>.ev.prv.<模式>`、`<p>.c.<socket>.>`；deny `"> >"` |
| 发布 | 允许 `<p>.rq.<socket>.>` |

所有主题与权限由库集中生成，单元测试断言浏览器的发布权限只有本连接的请求前缀。

### 4.3 请求身份

服务端处理 `<p>.rq.<socket>.…` 上的请求时：

- 回复主题必须位于 `<p>.c.<socket>.r.>` 下，否则丢弃；
- 从登记读取 `s.<socket>`（leader 读，节点内缓存最多 5 秒，吊销时广播失效）；登记不存在、已吊销或已过期时回复 `denied`；KV 暂时不可用时回复 `unavailable`（SDK 重试）。

### 4.4 hello 与过期刷新

连接后 SDK 发 `hello`，回复包含本连接的 `Info`、授予模式、过期时间、stream epoch 与留存配置。

- SDK 在过期前（剩余 10%，至少 30 秒）调用 `getToken()`，新建连接（新 socket）并切换，旧连接在切换完成后关闭。页面从后台恢复时立即检查。
- `getToken()` 抛出 `UnauthorizedError` 时进入 `stopped`；其他错误带抖动退避重试。

## 5. 主题布局

前缀 `<p>` 默认 `jetcast`，只允许 `[a-z0-9_]`。

| 主题 | 用途 |
|---|---|
| `<p>.in.<pub|prv>.<频道>` | stream 入口，只有应用节点发布 |
| `<p>.ev.<pub|prv>.<频道>` | 频道事件（RePublish 出口；Ephemeral 频道直接发布到这里） |
| `<p>.c.<socket>.ev` | 中继投递给该连接的事件 |
| `<p>.c.<socket>.ctl` | 发给该连接的控制消息 |
| `<p>.c.<socket>.r.>` | 请求回复与补发投递（nats.js `inboxPrefix`） |
| `<p>.rq.<socket>.<op>` | 由任意节点处理的请求：`hello`、`sub`、`heads`、`recover` |
| `<p>.rq.<socket>.n.<node>.<op>` | 发给指定节点的请求：`renew`、`leave`，以及中继频道的 `heads` |
| `<p>.sys.>` | 节点之间的控制广播 |

**频道名**：一到八段，每段只允许 `[A-Za-z0-9_-]`，总长不超过 200。客户端提交的永远是字面频道名；`*`、`>` 只出现在服务端的授予模式和频道模式中。

## 6. 频道与订阅

### 6.1 类型与路径

| 类型 | 客户端 | 服务端 | 授权 |
|---|---|---|---|
| 公开 | `echo.channel("news")` | `jetcast.Public("news")` | 无，直接订阅 |
| 私有 | `echo.private("orders.42")` | `jetcast.Private("orders.42")` | 命中授予模式时直接订阅；否则由 `Channel` 回调授权后中继 |

- **中继（默认）**：节点订阅 `<p>.ev.prv.<频道>`，把事件转发到 `<p>.c.<socket>.ev`，加上 `Jetcast-Channel`、`Jetcast-Sid` 头。
- **直接（显式优化）**：`Grants` 返回的模式写进连接权限。SDK 依据 hello 中的授予模式选择路径；服务端在 `sub` 时以自己的判定为准，并在回复中给出路径。授予模式必须是安全前缀：前缀下所有频道都允许该用户看到。

| 操作 | 直接路径 | 中继路径 |
|---|---|---|
| 新增授权 | `Refresh`：通知客户端在随机延迟后换新连接 | 下次订阅即生效 |
| 移除当前订阅 | `Leave`：通知 SDK 退订（协作式） | `Leave`：节点立即停止转发 |
| 强制撤销 | `Disconnect` 或等待 JWT 过期 | 授权回调拒绝（节点每 `ReauthorizeInterval` 重新授权，默认 1 分钟），或 `Leave` |

### 6.2 订阅流程

1. 建立接收：直接路径订阅频道主题并 flush；中继路径使用连接级的 `ev` 订阅。之后到达的事件先进入该频道缓冲区。
2. `sub {channel, sid, path}`，`sid` 是本次订阅尝试的新编号。服务端校验授权；中继路径订阅频道事件并 flush、登记中继；然后回复 `{path, node, epoch, recoverable, head, position}`，其中 `head` 为该频道最新事件序号（没有则为 0），`position` 为 stream 当前最后序号。
3. 首次订阅以回复作为游标起点。重新订阅（断线、节点失联、续约丢失）时带着原游标执行补发（第 7.3 节），补完后再处理缓冲区。
4. 进入 `subscribed`，报告 `recovered` 与原因。

jetcast 的序号只保证传输层连续，不是业务数据的版本；业务快照与增量的衔接由应用自己的版本号负责。

### 6.3 中继租约

- SDK 只接受当前 `sid` 的中继事件与控制消息。旧 `sid`（例如请求超时后在另一节点重试留下的中继）的事件被丢弃，所以跨节点重复中继不会造成重复交付。
- SDK 按节点合并续约：每 20 秒（带抖动）向持有中继的每个节点发 `n.<node>.renew`，携带该节点上的全部 `sid`；节点回复其中已不存在的 `sid`，SDK 对它们重新订阅。
- 续约 no responders 或超时：该节点上的频道标为 `interrupted`，带游标重新订阅。
- 节点 3 个续约周期收不到续约就移除中继；续约时登记已吊销或过期，或重新授权被拒绝，移除中继并向 `ctl` 发送带 `sid` 的 `denied`。
- 节点自身 NATS 连接断开恢复后，向本节点全部中继发送带 `sid` 的 `interrupted`，SDK 重新订阅并补发。
- 退订：SDK 向持有节点发送 `n.<node>.leave`。
- `Server.Close()`：停止接收请求，向本节点中继发送 `interrupted`，然后取消订阅。
- 中继数与并发请求数上限按节点计算（默认每连接 100 个中继、8 个并发请求）。

## 7. 留存、缺口检测与补发

### 7.1 stream

- 默认名称 `JETCAST`，主题 `<p>.in.>`，RePublish `<p>.in.>` → `<p>.ev.>`。
- 只用按年龄与总字节淘汰：默认 `MaxAge` 5 分钟、`MaxBytes` 1 GB、`DiscardOld`；不设每主题条数上限，不允许 rollup、单条删除、按消息 TTL；不开启 AllowDirect；`Duplicates` 2 分钟；文件存储，副本数可配置。
- epoch 为 stream 创建时间。
- `Server.Start` 默认只校验 stream 与 KV 配置，不一致时报错；`ManageStreams: true` 时创建或更新。

### 7.2 游标与实时检测

SDK 为每个可补发频道保存游标：`epoch`、`pos`（已确认收齐到的 stream 全局序号）、`last`（最后交付的本频道事件序号，0 表示没有）。

收到序号 `S`、上一条序号 `L` 的事件：

1. `S <= pos`：重复，丢弃。如果 `S < last`（序号回退，可能是 stream 重建），立即触发一次 heads 检查（有节流）。
2. `last != 0` 且 `L == last`：可证明连续（若两者之间有事件被淘汰，那么更旧的 `last` 也必然已被淘汰，与 `L == last` 矛盾），交付，`pos = last = S`。
3. 其他情况：无法证明连续，先补发 `(pos, S)`，再交付 `S`。

### 7.3 补发

SDK 用新的回复主题发送 `recover {channel, epoch, pos, upTo}`；服务端向回复主题依次发送事件消息（头 `Jetcast-Status: event`），最后发送结果消息（头 `Jetcast-Status: done`，正文为 JSON 结果）。处理步骤：

1. 校验授权（公开频道放行；直接路径看登记中的授予；中继路径看本连接在本节点是否有当前中继，否则执行授权回调）。
2. epoch 不同：回复 `{recovered: false, reason: "epoch"}`。
3. 从 `pos + 1` 起按主题逐条读取（leader 路径），每条作为独立消息发到回复主题，带 `Jetcast-Event`、`Jetcast-Id`、`Jetcast-Origin`、`Nats-Sequence`、`Nats-Last-Sequence`（由服务端按读取顺序重建）；直到 `upTo`、本批条数（默认 100）或字节（默认 512 KB，至少一条）上限。
4. 读完后读取 stream 状态：最早序号 `first > pos + 1` 说明 `pos` 之后已有事件被淘汰，结果不完整，回复 `{recovered: false, reason: "expired"}`；否则回复 `{recovered: true, more, next}`，`next` 是本批最后一条的序号（没有事件时为 `upTo - 1` 或当前最后序号）。
5. 单次补发累计超过 10000 条或 16 MB 时回复 `{recovered: false, reason: "too_far"}`。

SDK 串行处理同一频道的补发与实时事件：补发期间实时事件进入缓冲（上限 1000 条，超出视为 `too_far`）；按批推进游标，`more` 时以 `next` 继续；完成后交付缓冲中序号大于游标的事件。补发失败（`recovered: false`）时，游标重置到服务端给出的当前位置，频道状态报告 `recovered: false` 与原因，由应用重新拉取数据。

### 7.4 heads：尾部丢失与静默频道

SDK 每 30 秒（带抖动；页面从后台恢复时立即）对本周期没有收到实时事件的可补发频道发一次 `heads {channels, epoch}`：直接订阅的频道发给任意节点，中继频道发给持有中继的节点（该节点只回答本连接在本节点有中继的频道）。服务端按授权回复每个频道的最新序号，以及 stream 的 `first`、`last`、epoch。SDK：

- 频道最新序号大于 `last`：补发 `(pos, 最新序号]`；
- 相等且 `first <= pos + 1`：没有遗漏，把 `pos` 推进到 stream 的 `last`，使游标不会随时间变旧；
- `first > pos + 1`：`pos` 之后有事件已被淘汰，无法证明完整，按 `recovered: false, reason: "expired"` 处理；
- epoch 变化：按 `reason: "epoch"` 处理。

服务端对同一频道的最新序号查询做 1 秒合并。

### 7.5 Ephemeral 频道

按模式声明为 Ephemeral 的频道直接发布到 `<p>.ev...`，不进入 stream，没有序号；`sub` 回复 `recoverable: false`，SDK 不做缺口检测与补发。

## 8. 广播

```go
pub := jetcast.NewPublisher(nc, cfg)
res, err := pub.Broadcast(ctx, jetcast.Event{
    Name:     "order.shipped",
    Channels: []jetcast.Channel{jetcast.Private("orders.42"), jetcast.Private("users.7.feed")},
    Data:     v,                // []byte 原样；其他值 JSON 编码
    Origin:   socketID,         // 可选，即 toOthers
})
```

- `jetcast.Config`（前缀、stream 名、Ephemeral 模式）由 `Server` 与 `Publisher` 共用。
- 事件类型也可以实现 `BroadcastOn() []Channel`，可选 `BroadcastAs()`、`BroadcastWith()`、`BroadcastWhen()`。
- 每个频道一次 JetStream 发布，`Nats-Msg-Id` 为 `<事件 ID>:<pub|prv>:<频道>`，去重窗口内重试不会重复。多频道广播不是原子的，结果逐频道列出成功与失败。
- 消息头只由库设置；应用数据不能覆盖 `Jetcast-*`、`Nats-*` 头。事件大小不能超过连接的 `max_payload` 减去头部开销，超出时 `Broadcast` 直接返回错误。
- **toOthers**：照常投递，SDK 推进游标但不触发来源连接的回调。它不是保密手段。

## 9. 撤销与连接管理

| API | 行为 |
|---|---|
| `Refresh(ctx, ByUser / BySession)` | 新增授予后使用：通知相关连接在随机延迟（0–5 秒）后换新连接 |
| `Leave(ctx, channel, ByUser / BySession)` | 移除当前订阅：中继立即停止转发并通知 SDK；直接订阅的 SDK 收到通知后退订。不撤销授权 |
| `Disconnect(ctx, ByUser / BySession)` | 吊销：先写吊销标记（使在途 callout 失败），再按索引把相关登记标为已吊销，广播缓存失效与中继移除，向连接发 `disconnect` 控制消息；配置了 `ConnectionAdmin` 时按登记中的 server id 与 cid 踢出。结果报告各项成功与失败数量 |

调用顺序：应用必须先让会话失效（`Authenticate` 不再通过），再调用 `Disconnect`。

```go
type ConnectionAdmin interface {
    Kick(ctx context.Context, serverID string, cid uint64) error
}
```

提供两个实现：系统账号连接发送 `$SYS.REQ.SERVER.<id>.KICK`；嵌入式服务端调用 `DisconnectClientByID`。不配置时只做协作式断开，结果中注明。

## 10. 可用性、限制与可观测性

| 故障 | 影响 |
|---|---|
| KV 不可用 | 新连接认证失败；依赖登记的请求回复 `unavailable`；已建立的直接订阅与中继转发继续工作，续约无法读登记时中继在 3 个周期后停止 |
| 事件 stream 不可用 | 留存频道的广播、heads、补发失败；Ephemeral 广播继续工作 |
| 应用节点全部不可用 | 新连接认证失败；直接订阅继续收到事件 |

- 每连接、每节点的资源上限见第 6.3 节；账号级的连接数、订阅数、载荷上限在 NATS 配置中设置。
- `Server.Stats()`：callout 通过与拒绝数及延迟、中继数、转发条数、补发请求与失败数。日志用 `slog`，凭据永不写入日志。

## 11. Go API

```go
cfg := jetcast.Config{Prefix: "jetcast", Stream: "JETCAST", Ephemeral: []string{"typing.>"}}

srv, err := jetcast.NewServer(nc, jetcast.ServerOptions{
    Config:        cfg,
    Account:       "APP",
    CalloutSigner: issuerKey,
    CalloutXKey:   xkey,                         // 可选
    Admin:         jetcast.SystemAdmin(sysConn), // 可选
    History:       jetcast.History{MaxAge: 5 * time.Minute, Replicas: 1},
    ManageStreams: true,
})
srv.Authenticate(func(ctx context.Context, r jetcast.AuthRequest) (jetcast.User, error) { ... })
srv.Grants(func(ctx context.Context, u jetcast.User) ([]string, error) { ... })
srv.Channel("orders.{id}", func(ctx context.Context, u jetcast.User, p jetcast.Params) (bool, error) { ... })
err = srv.Start(ctx)
defer srv.Close()

srv.Broadcast(ctx, event)
srv.Refresh(ctx, jetcast.ByUser(id))
srv.Leave(ctx, jetcast.Private("orders.42"), jetcast.ByUser(id))
res, err := srv.Disconnect(ctx, jetcast.BySession(userID, sessionID))
jetcast.SocketID(r)  // 从 HTTP 请求读取 X-Socket-ID
```

## 12. TypeScript SDK（`js/`，包名 `@runforyou/jetcast`）

```ts
const echo = await connect({
  servers: "wss://example.com/nats",
  getToken: async () => api.realtimeToken(),
})

echo.channel("news").listen("article.published", (data, meta) => {})

const ch = echo.private("orders.42").listen("order.shipped", (data, meta) => {})
await ch.ready()                       // 首次进入 subscribed；被拒绝时 reject
ch.onState(({ state, recovered, reason }) => {
  // state: subscribing | recovering | subscribed | interrupted | denied
  if (state === "subscribed" && recovered === false) reload()
})
ch.leave()

echo.socketId                          // 当前连接的 socket，调 API 时放在 X-Socket-ID
echo.onStatus((s) => {})               // connecting | connected | reconnecting | stopped
await echo.close()
```

- `recovered: false` 的 `reason`：`initial`、`expired`、`epoch`、`too_far`、`ephemeral`。
- `socketId` 在换新连接后变化；`toOthers` 依赖应用在每次 API 请求时读取当前值。
- 依赖 `@nats-io/nats-core` v3。

## 13. Go 客户端

`jetcast/client` 提供与 TS SDK 对应的 API，供执行器等非浏览器客户端使用。应用在 `User.ConnectionTypes` 中允许 `STANDARD`。

## 14. 部署

- 应用节点以 callout 的 `auth_users` 身份连接，需要：发布 `<p>.in.>`、`<p>.ev.>`、`<p>.c.>`、`<p>.sys.>`；订阅 `<p>.rq.>`、`<p>.ev.prv.>`、`<p>.sys.>`、`$SYS.REQ.USER.AUTH`；访问 stream 与 KV 的 JetStream API。
- 第一期提供示例：嵌入式服务端 Go 示例、独立部署 `nats-server.conf`、nginx 与负载均衡反代配置、Go `httputil.ReverseProxy` 反代示例。
- 文档注意事项：`same_origin` 在终止 TLS 的代理后会误判，应使用 `allowed_origins`；集群设置 `websocket.advertise` 或在客户端忽略集群地址推送；代理读超时大于 NATS ping 周期；凭据长度受 `max_control_line`（默认 4096 字节）限制；可用 `token_cookie` 改用 HttpOnly cookie 传凭据。

## 15. 分期

- **第一期**：本文全部内容。
- **第二期**：presence、whisper、React hooks、与 jetq 衔接（排队广播、事务提交后广播）、模型广播约定、配置生成器与 Go 反代组件。
- **第三期**：按需查询历史、浏览器用户独立账号与 `Nats-Request-Info`、无 callout 模式、OpenTelemetry、多 stream 分片。

## 16. 验收用例

1. 公开频道多客户端收发。
2. 授予频道：hello 返回本连接的授予；授予范围外的直接订阅被拒绝。
3. 中继频道：授权允许与拒绝；`ready` 后发布的事件送达；`leave` 后不再收到；同一频道新旧 `sid` 并存时只交付当前 `sid`。
4. 多节点：任意节点广播都能送达；中继节点退出后在其他节点重建，期间事件经补发补齐。
5. 缺口：丢弃一条实时事件后下一条触发补发；最后一条丢失由 heads 发现；频道静默且 `pos` 之后有淘汰时报告 `expired`；空频道首条漏收后过期报告 `expired`；补发进行中发生淘汰报告 `expired`；stream 重建报告 `epoch`。
6. toOthers：来源连接不触发回调，游标正常推进。
7. 安全：重复使用已登记 socket 的连接被拒绝；吊销期间的在途 callout 被拒绝；回复主题不属于本连接时请求被丢弃；浏览器不能发布到频道、不能做队列订阅、不能补发未授权频道。
8. `Refresh` 后新授予可用；`Disconnect` 标记登记、踢出连接，不配合的客户端也被断开；JWT 到期前 SDK 无感换新连接。
9. 嵌入式单机、独立 NATS、三节点集群（含 stream leader 切换）分别走通。

## 17. 待实测

- 三节点集群 stream leader 切换期间 RePublish 的丢失与重复，以及 heads 的覆盖。
- 单 stream 写入吞吐、heads 与补发的读负载（基准测试）。
- callout 中 KV 操作带来的连接延迟。
