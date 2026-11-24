# 10 · Queue features

Eight small demos of queue arguments, each in its own method in `Program.cs`.

| Demo | Arguments | Result |
|------|-----------|--------|
| Queue TTL | `x-message-ttl = 1000` | message gone after 1.5 s |
| Message expiration | `Expiration = "300"` on one message | only that message expires |
| Max length, drop-head | `x-max-length = 3` | publish 1..5 → keeps 3, 4, 5 |
| Max length, reject-publish | `x-max-length = 2`, `x-overflow = reject-publish` | 3rd publish is nacked → `PublishException` |
| Priority | `x-max-priority = 10` | 9 > 5 > 1 > 0 |
| Queue expiry | `x-expires = 1000` | unused queue deletes itself → 404 on passive declare |
| Quorum delivery limit | `x-queue-type = quorum`, `x-delivery-limit = 2` | requeued 3 times, then dead-lettered |
| Single active consumer | `x-single-active-consumer = true` | primary gets 1–3, standby takes over for 4–6 |

## Run

```bash
dotnet run --project samples/10-QueueFeatures
```

Theory: [06 Queue types and arguments](../../docs/06-queue-types-and-arguments.md),
[04 Consumers](../../docs/04-consumers-acks-prefetch.md).
