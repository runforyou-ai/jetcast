# Migrating to 0.3

Version 0.3 changes the Go publication API and the TypeScript connection type. The NATS wire protocol, channel names, authorization and recovery behavior are unchanged.

## Go publication

`Dispatch`, `Broadcastable` and `BroadcastOption` are removed. Publish an `Event` through `Broadcast`; application code supplies every part of the publication explicitly:

```go
if shouldPublish {
    result, err := publisher.Broadcast(ctx, jetcast.Event{
        Name:     "order.shipped",
        Channels: []jetcast.Channel{jetcast.Private("orders.42")},
        Data:     OrderShipped{ID: 42},
        ID:       eventID,
        Origin:   jetcast.SocketID(request),
    })
    // Handle err and inspect result.Channels for partial failures.
}
```

- `Name` is required. Keep the event name used by existing subscribers, including the former Go type name if that was the deployed name.
- Set `Channels` directly instead of supplying a channel method on the payload.
- Set `Data` to the published value instead of a payload-selection hook.
- Evaluate publication conditions before calling `Broadcast`.
- Set `ID` and `Origin` on the event instead of passing dispatch options. Empty `ID` generates a random identifier; empty `Origin` delivers callbacks to every subscriber.

Go payload types need no publication methods. Changes to their Go names do not change an event's explicitly supplied protocol name.

## TypeScript connection

Update connection type imports and annotations to `Client`. `connect` and the returned object's methods keep their behavior:

```ts
import { connect, type Client } from "@runforyou/jetcast";

const client: Client = await connect({ servers, getToken });
const channel = client.private("orders.42");
channel.listen("order.shipped", (data) => render(data));
await channel.ready();
```

`Client` is the only exported connection class. Existing calls to `connect`, `channel`, `private`, `listen`, `onState`, `ready` and `close` require no behavioral migration. The Go client already uses the name `Client` and is unchanged.
