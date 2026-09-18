/*
* @Author: kamalyes 501893067@qq.com
* @Date: 2026-09-19 17:21:07
* @LastEditors: kamalyes 501893067@qq.com
* @LastEditTime: 2026-09-19 19:21:07
* @FilePath: \go-bot-queue-nats\queue.go
* @Description: NATS JetStream 队列适配器：构造与配置
*
* Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */

// Package queue 提供基于 NATS JetStream 的 gobot.Queue 实现：
// 任务经 Publish 落到持久化 Stream、durable pull consumer
// 竞争消费（一条任务只被一个实例投递）、成功 Ack 确认删除；
// 投递失败的消息不确认，AckWait 超时后由 JetStream 重投
// （含其他实例），达到 MaxDeliver 次仍失败则 Term 终结丢弃
//
// 分布式部署：多个进程实例共享同一 Stream 与 durable consumer，
// 每条任务只会被一个实例投递，实例崩溃后其未确认消息
// 在 AckWait 后自动转移给其余实例
package queue

import (
	"context"
	"strings"
	"sync"
	"time"

	gobot "github.com/kamalyes/go-bot"
	gologger "github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/mathx"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// DefaultStream 是任务流的缺省名
	DefaultStream = "GOBOT"
	// DefaultSubject 是任务的缺省主题
	DefaultSubject = "gobot.tasks"
	// DefaultDurable 是 durable pull consumer 的缺省名，跨实例共享
	DefaultDurable = "gobot"
	// DefaultAckWait 是未确认消息的重投等待
	DefaultAckWait = 30 * time.Second
	// DefaultFetchTimeout 是单次拉取的等待时长
	DefaultFetchTimeout = 5 * time.Second
)

// DefaultMaxDeliver 是单条任务的缺省最大投递次数，耗尽即终结丢弃
const DefaultMaxDeliver = 3

// Config 配置 NATS JetStream 队列适配器
type Config struct {
	// Conn 是注入的 NATS 连接；nil 时按 URL 拨号
	Conn *nats.Conn
	// URL 是自建连接时的 NATS 地址，支持逗号分隔的集群地址列表
	// （客户端在服务器间自动故障转移），缺省 nats://127.0.0.1:4222
	URL string
	// Stream 是持久化流名，缺省 DefaultStream
	Stream string
	// Subject 是任务主题，缺省 DefaultSubject
	Subject string
	// Durable 是 durable consumer 名，跨实例共享实现竞争消费，
	// 缺省 DefaultDurable
	Durable string
	// AckWait 是未确认消息的重投等待，缺省 DefaultAckWait
	AckWait time.Duration
	// MaxDeliver 是单条任务的最大投递次数（含首次），
	// 缺省 DefaultMaxDeliver；JetStream 侧同值兜底
	MaxDeliver int
	// FetchTimeout 是单次拉取的等待时长，缺省 DefaultFetchTimeout
	FetchTimeout time.Duration
	// Logger 可选，缺省 go-logger 的 EmptyLogger
	Logger gologger.ILogger
}

// Queue 实现 gobot.Queue：把投递任务中转到 NATS JetStream。
// 多个 Queue 实例（不同进程）共享同一 Stream + durable consumer 时，
// 每条任务只被一个实例投递，实例崩溃后其未确认消息
// 在 AckWait 后由其余实例接管
// Queue 可安全并发使用
type Queue struct {
	cfg      Config
	conn     *nats.Conn
	js       jetstream.JetStream
	ownClose bool // 自建连接时 Close 需要一并释放

	mu          sync.Mutex
	streamReady bool // 流已确保创建，后续 Publish 跳过检查
}

// New 按配置创建队列适配器并探活连接，连接失败时返回错误
func New(cfg Config) (*Queue, error) {
	const op = "New"
	if cfg.Conn == nil && strings.TrimSpace(cfg.URL) == "" {
		cfg.URL = nats.DefaultURL
	}
	conn := cfg.Conn
	if conn == nil {
		var err error
		conn, err = nats.Connect(cfg.URL,
			nats.Timeout(DefaultFetchTimeout),
			nats.ReconnectWait(time.Second),
			nats.MaxReconnects(-1))
		if err != nil {
			return nil, gobot.NewTransportError(gobot.Platform(""), op, err)
		}
	}
	js, err := jetstream.New(conn)
	if err != nil {
		if cfg.Conn == nil {
			_ = conn.Drain()
		}
		return nil, gobot.NewTransportError(gobot.Platform(""), op, err)
	}
	cfg.Stream = mathx.IfNotEmpty(cfg.Stream, DefaultStream)
	cfg.Subject = mathx.IfNotEmpty(cfg.Subject, DefaultSubject)
	cfg.Durable = mathx.IfNotEmpty(cfg.Durable, DefaultDurable)
	if cfg.AckWait <= 0 {
		cfg.AckWait = DefaultAckWait
	}
	if cfg.MaxDeliver <= 0 {
		cfg.MaxDeliver = DefaultMaxDeliver
	}
	if cfg.FetchTimeout <= 0 {
		cfg.FetchTimeout = DefaultFetchTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = &gologger.EmptyLogger{}
	}
	return &Queue{cfg: cfg, conn: conn, js: js, ownClose: cfg.Conn == nil}, nil
}

// Close 释放队列持有的资源；注入的连接归调用方管理，
// 仅自建连接时由本方法释放
func (q *Queue) Close(ctx context.Context) error {
	if !q.ownClose {
		return nil
	}
	if err := q.conn.Drain(); err != nil {
		return gobot.NewTransportError(gobot.Platform(""), "Close", err)
	}
	return nil
}

// logger 返回配置的日志实现
func (q *Queue) logger() gologger.ILogger { return q.cfg.Logger }
