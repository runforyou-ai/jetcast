# jetcast

[English](README.md)

jetcast 把服务端事件实时推送给经 WebSocket **直连 NATS** 的浏览器和其他客户端。熟悉 Laravel Broadcasting 和 Echo 的话，jetcast 的用法几乎一样：服务端向频道广播事件，客户端监听频道，私有频道由你写的回调授权。

- **浏览器直连 NATS，权限由 NATS 强制执行。** 客户端带着你的令牌，经 NATS auth callout 认证；每个连接拿到的 JWT 只允许订阅它有权看的频道和它自己的主题。
- **Laravel 式频道授权。** `srv.Channel("orders.{id}", authorize)` 在订阅时授权，相当于 `routes/channels.php`。稳定、范围较大的授权（用户自己的通知、团队频道）也可以在连接时授予，由 NATS 直接路由，不经过应用服务器。
- **漏掉的事件可以补回。** 事件在 JetStream stream 中保留几分钟。客户端能发现漏收，包括最后一条丢失的情况，并在重连后补齐；补不齐时明确告诉应用，由应用重新加载数据，不会默默显示过期内容。
- **`toOthers`、吊销与刷新。** 可以不推给操作发起者自己；可以吊销某个用户或会话的全部连接，不配合的客户端会被强制踢下线；授权增加后可以让客户端重连拿到新授权。
- **不限定部署形态。** 库只接收 `*nats.Conn`。NATS 可以嵌入应用、独立部署或集群；WebSocket 可以由 Go、nginx 或负载均衡反代。

要求 nats-server **2.14.4+**（推荐 2.15）并开启 JetStream，Go 1.26+。

## 安装

```sh
go get github.com/runforyou-ai/jetcast
npm install @runforyou/jetcast
```

## 用法

服务端、浏览器端的完整示例见 [README](README.md#server)，部署说明见 [docs/deployment.md](docs/deployment.md)，设计与协议见 [docs/design.md](docs/design.md)。

```go
srv.Authenticate(func(ctx context.Context, r jetcast.AuthRequest) (jetcast.User, error) { ... })
srv.Grants(func(ctx context.Context, u jetcast.User) ([]string, error) {
	return []string{"users." + u.ID + ".>"}, nil
})
srv.Channel("orders.{id}", func(ctx context.Context, u jetcast.User, p jetcast.Params) (bool, error) {
	return orders.CanView(ctx, u.ID, p["id"])
})
srv.Broadcast(ctx, jetcast.Event{
	Name:     "order.shipped",
	Channels: []jetcast.Channel{jetcast.Private("orders.42")},
	Data:     order,
	Origin:   jetcast.SocketID(r),
})
```

```ts
const echo = await connect({ servers: "wss://example.com/nats", getToken })
echo.private("orders.42")
  .listen("order.shipped", (order) => update(order))
  .onState(({ state, recovered }) => {
    if (state === "subscribed" && recovered === false) reloadOrder()
  })
```

## 投递语义

- 每个连接至多投递一次，留存期内（默认 5 分钟）可以补回。业务数据以你自己的存储为准：订阅报告 `recovered: false` 时重新加载。
- 授权在建立连接时（授予）和订阅频道时（授权回调）检查，中继频道还会定期重新检查。需要立即切断访问时使用 `Disconnect`，配置 `ConnectionAdmin` 执行强制断开。`Disconnect` 返回错误时，由应用重试以完成撤销，重试包含已标记撤销的连接；需要跨进程重启继续执行时，应用持久化重试任务。
- `toOthers` 只是不触发发起者的回调，不是保密手段。

## 路线图

- presence 频道（`here`、`joining`、`leaving`）与客户端事件（whisper）。
- React hooks。
- 借助 [jetq](https://github.com/runforyou-ai/jetq) 排队广播、事务提交后广播。
- 历史查询、浏览器独立账号、无 callout 模式、OpenTelemetry。

## 许可证

[MIT](LICENSE)
