/*
* @Author: kamalyes 501893067@qq.com
* @Date: 2026-09-19 21:16:23
* @LastEditors: kamalyes 501893067@qq.com
* @LastEditTime: 2026-09-19 21:36:07
* @FilePath: \go-bot-queue-nats\queue_test.go
* @Description: queue 构造与配置测试
*
* Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */

package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	gobot "github.com/kamalyes/go-bot"
)

// TestNewConnectionRefused 验证自建连接拨不通时返回传输错误
func TestNewConnectionRefused(t *testing.T) {
	_, err := New(Config{URL: "nats://127.0.0.1:1", FetchTimeout: 100 * time.Millisecond})
	var e *gobot.Error
	if !errors.As(err, &e) || e.Kind != gobot.KindTransport {
		t.Fatalf("New() 错误 = %v, 期望 transport", err)
	}
}

// TestNewDefaults 验证缺省值填充：流、主题、durable、
// 重投等待、投递上限与拉取超时
func TestNewDefaults(t *testing.T) {
	srv := newJSServer(t)
	q, err := New(Config{URL: srv.ClientURL()})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = q.Close(context.Background()) }()

	if q.cfg.Stream != DefaultStream {
		t.Fatalf("Stream = %q, 期望 %q", q.cfg.Stream, DefaultStream)
	}
	if q.cfg.Subject != DefaultSubject {
		t.Fatalf("Subject = %q, 期望 %q", q.cfg.Subject, DefaultSubject)
	}
	if q.cfg.Durable != DefaultDurable {
		t.Fatalf("Durable = %q, 期望 %q", q.cfg.Durable, DefaultDurable)
	}
	if q.cfg.AckWait != DefaultAckWait {
		t.Fatalf("AckWait = %v, 期望 %v", q.cfg.AckWait, DefaultAckWait)
	}
	if q.cfg.MaxDeliver != DefaultMaxDeliver {
		t.Fatalf("MaxDeliver = %d, 期望 %d", q.cfg.MaxDeliver, DefaultMaxDeliver)
	}
	if q.cfg.FetchTimeout != DefaultFetchTimeout {
		t.Fatalf("FetchTimeout = %v, 期望 %v", q.cfg.FetchTimeout, DefaultFetchTimeout)
	}
	if !q.ownClose {
		t.Fatal("自建连接时 ownClose 应为 true")
	}
}

// TestNewInjectedConnNotOwned 验证注入连接时 Close 不释放连接
func TestNewInjectedConnNotOwned(t *testing.T) {
	srv := newJSServer(t)
	conn, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}
	defer func() { _ = conn.Drain() }()

	q, err := New(Config{Conn: conn})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if q.ownClose {
		t.Fatal("注入连接时 ownClose 应为 false")
	}
	if err := q.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !conn.IsConnected() {
		t.Fatal("注入的连接不应被释放")
	}
}
