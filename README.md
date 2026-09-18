<div align="center">

# 📬 Go-Bot-Queue-NATS

**go-bot 削峰填谷队列的 NATS JetStream 适配器**

*消息先进队列按节奏消费、多实例负载均衡，不再瞬时调用打爆平台限流*

<br>

[![Go 版本](https://img.shields.io/badge/Go-1.26+-00ADD8.svg)](https://golang.org/)
[![许可协议](https://img.shields.io/badge/license-MIT-green.svg)]()
[![go-bot](https://img.shields.io/badge/依赖-go--bot-blue.svg)](https://github.com/kamalyes/go-bot)

</div>

---

## ✨ 特性

- 🌊 **削峰填谷** - `QueuedBot.Send` 入队直返，消费循环按 `Interval` 节流排干队列，钉钉 20 条/分钟这类风控从此只是配置数字
- 🌐 **分布式** - durable pull consumer 竞争消费：一条任务只被一个实例投递，崩溃实例的未确认消息在 `AckWait` 后自动转移给存活实例
- 🔄 **可靠投递** - 失败任务不确认、由 JetStream 超时重投，投递次数达到 `MaxDeliver` 终结丢弃并记录日志，脏载荷直接隔离
- 🧩 **通用连接** - 注入 `*nats.Conn` 复用既有连接，或按 `URL` 自建（无限自动重连），`URL` 支持逗号分隔集群地址

## 🚀 快速开始

```go
package main

import (
	"context"
	"time"

	gobot "github.com/kamalyes/go-bot"
	"github.com/kamalyes/go-bot/dingtalk"
	"github.com/kamalyes/go-bot-queue-nats"
)

func main() {
	adapter, err := dingtalk.New(dingtalk.Config{
		Token:  "access_token",          // webhook 地址中的 access_token
		Secret: "SEC...",                // 安全设置为加签时必填
	})
	if err != nil {
		panic(err)
	}
	bot, err := gobot.NewBot(adapter).Build()
	if err != nil {
		panic(err)
	}

	q, err := queue.New(queue.Config{
		URL: "nats://127.0.0.1:4222",  // 集群逗号分隔：nats://host1:4222,host2:4222；或注入 Conn: *nats.Conn
	})
	if err != nil {
		panic(err)
	}
	defer q.Close(context.Background())

	qb, err := gobot.NewQueuedBot(bot, q).
		WithInterval(3 * time.Second). // 削峰节奏：钉钉 20 条/分钟对应 3s
		Build()
	if err != nil {
		panic(err)
	}

	// 生产端：突发 60 条告警瞬间入队、立即返回，平台侧按节奏收到
	for i := 0; i < 60; i++ {
		_, _ = qb.Send(context.Background(), gobot.Text("【告警】CPU 92%"), gobot.Chat(""))
	}

	// 消费端：常驻进程按节奏排干队列；静音/重试/熔断/统计全链路生效
	if err := qb.Run(context.Background()); err != nil {
		panic(err)
	}
}
```

## 🏭 多实例部署

多个进程实例使用相同的 `Stream` + `Durable`：

- 负载均衡：JetStream 把流内消息分发给共享 durable consumer 的实例，一条任务只被一个实例投递
- 故障转移：实例崩溃后其未确认消息在 `AckWait` 后由存活实例接管重投
- 吞吐扩容：单实例 `Interval` 限制单实例速率，集群总吞吐 = 实例数 × 单实例速率

先入队、后起消费者不丢任务：任务落在持久化流里，durable consumer 从流头全量消费。

## ⚙️ 配置速查

| 配置 | 缺省 | 说明 |
|------|------|------|
| `URL` / `Conn` | `nats://127.0.0.1:4222` | NATS 地址（支持逗号分隔集群，自动故障转移），或注入 `*nats.Conn` |
| `Stream` | `GOBOT` | 持久化流名，多实例共享 |
| `Subject` | `gobot.tasks` | 任务主题，绑定到流 |
| `Durable` | `gobot` | durable consumer 名，跨实例共享实现竞争消费 |
| `AckWait` | `30s` | 未确认消息的重投等待，应大于单条任务最长投递耗时 |
| `MaxDeliver` | `3` | 单条任务最大投递次数，耗尽终结丢弃 |
| `FetchTimeout` | `5s` | 单次拉取的等待时长 |

## 🧪 测试

```bash
go test ./... -race
```

基于内嵌 nats-server 端到端：入队落流、确认清零、失败重投、耗尽终结、脏载荷隔离与双实例负载均衡恰好一次。
