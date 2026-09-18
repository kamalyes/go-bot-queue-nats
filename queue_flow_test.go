/*
* @Author: kamalyes 501893067@qq.com
* @Date: 2026-09-19 21:17:36
* @LastEditors: kamalyes 501893067@qq.com
* @LastEditTime: 2026-09-19 22:11:23
* @FilePath: \go-bot-queue-nats\queue_flow_test.go
* @Description: queue 消费全流程测试：入队、确认、重投、终结丢弃与多实例负载均衡
*
* Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */

package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	gobot "github.com/kamalyes/go-bot"
)

// newJSServer 启动开启 JetStream 的内嵌 NATS server，
// 存储落在临时目录，用例结束自动关闭
func newJSServer(t *testing.T) *server.Server {
	t.Helper()
	srv, err := server.NewServer(&server.Options{
		JetStream: true,
		StoreDir:  t.TempDir(),
		Port:      -1,
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("NATS server 启动超时")
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

// flowConfig 返回加速用例的消费参数：重投等待与拉取超时压到毫秒级
func flowConfig(url string) Config {
	return Config{
		URL:          url,
		Stream:       "GOBOT_FLOW",
		Subject:      "gobot.flow",
		AckWait:      200 * time.Millisecond,
		MaxDeliver:   2,
		FetchTimeout: 50 * time.Millisecond,
	}
}

// newFlowQueue 按流名与 durable 建队列实例（多实例共享流与 durable）
func newFlowQueue(t *testing.T, url string, durable string) *Queue {
	t.Helper()
	cfg := flowConfig(url)
	cfg.Durable = durable
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = q.Close(context.Background()) })
	return q
}

// runConsume 在后台 goroutine 起消费循环，返回停止函数
func runConsume(t *testing.T, q *Queue, handler gobot.QueueHandler) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- q.Consume(ctx, handler) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("Consume 未随 ctx 取消退出")
		}
	})
	return cancel
}

// waitUntil 轮询断言直至条件满足或超时
func waitUntil(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("等待条件超时")
}

// inspectTask 构造生产级巡检任务快照
func inspectTask() *gobot.QueueTask {
	return &gobot.QueueTask{
		ID: "task-inspect-001",
		Message: gobot.Message{
			Type:      gobot.MsgTypeMarkdown,
			Title:     "【巡检报告】cn-east-1",
			Text:      "| 服务 | 实例 | QPS | P99 |\n| --- | --- | ---: | ---: |\n| user-api | 12 | 8600 | 120ms |\n",
			AtUserIDs: []string{"ou_7dab8ce3"},
		},
		Targets:     []gobot.Target{gobot.Chat("-1001234567890")},
		PublishedAt: time.Date(2026, 9, 19, 3, 11, 23, 0, time.Local),
	}
}

// streamState 查询流内消息数，校验 Publish 持久化
func streamState(t *testing.T, url string) func() uint64 {
	t.Helper()
	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Drain() })
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}
	return func() uint64 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		// 流可能尚未创建（Publish 前查询），查不到按 0 条等待重试
		stream, err := js.Stream(ctx, flowConfig(url).Stream)
		if err != nil {
			return 0
		}
		info, err := stream.Info(ctx)
		if err != nil {
			return 0
		}
		return info.State.Msgs
	}
}

// TestPublishPersistsToStream 验证任务发布即持久化落流且载荷完整
func TestPublishPersistsToStream(t *testing.T) {
	srv := newJSServer(t)
	q := newFlowQueue(t, srv.ClientURL(), "flow-publisher")
	state := streamState(t, srv.ClientURL())

	task := inspectTask()
	if err := q.Publish(context.Background(), task); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	waitUntil(t, 3*time.Second, func() bool { return state() == 1 })
}

// TestPublishRejectsNil 验证空任务在本地被拒绝
func TestPublishRejectsNil(t *testing.T) {
	srv := newJSServer(t)
	q := newFlowQueue(t, srv.ClientURL(), "flow-publisher")

	err := q.Publish(context.Background(), nil)
	var e *gobot.Error
	if !errors.As(err, &e) || e.Kind != gobot.KindValidation {
		t.Fatalf("Publish(nil) 错误 = %v, 期望 validation", err)
	}
}

// TestConsumeDeliversAndAcks 验证任务经竞争消费投递后 Ack 确认
func TestConsumeDeliversAndAcks(t *testing.T) {
	srv := newJSServer(t)
	q := newFlowQueue(t, srv.ClientURL(), "flow-deliver")

	var mu sync.Mutex
	var received []*gobot.QueueTask
	runConsume(t, q, func(_ context.Context, task *gobot.QueueTask) error {
		mu.Lock()
		received = append(received, task)
		mu.Unlock()
		return nil
	})

	task := inspectTask()
	if err := q.Publish(context.Background(), task); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	waitUntil(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) == 1
	})

	mu.Lock()
	defer mu.Unlock()
	if received[0].ID != task.ID {
		t.Fatalf("收到任务 ID = %q, 期望 %q", received[0].ID, task.ID)
	}
	if received[0].Message.Title != task.Message.Title || len(received[0].Targets) != 1 {
		t.Fatalf("收到任务载荷不完整: %+v", received[0])
	}
}

// TestConsumeRedeliversFailure 验证失败任务不确认，
// AckWait 后由 JetStream 重投直至成功
func TestConsumeRedeliversFailure(t *testing.T) {
	srv := newJSServer(t)
	q := newFlowQueue(t, srv.ClientURL(), "flow-redeliver")

	var calls atomic.Int32
	runConsume(t, q, func(_ context.Context, _ *gobot.QueueTask) error {
		if calls.Add(1) == 1 {
			return errors.New("网络抖动，投递失败")
		}
		return nil
	})

	if err := q.Publish(context.Background(), inspectTask()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	waitUntil(t, 3*time.Second, func() bool { return calls.Load() >= 2 })

	if got := calls.Load(); got != 2 {
		t.Fatalf("投递次数 = %d, 期望重投后成功共 2 次", got)
	}
}

// TestConsumeDropsExhausted 验证持续失败的任务达到
// MaxDeliver 后被 Term 终结，不再重投
func TestConsumeDropsExhausted(t *testing.T) {
	srv := newJSServer(t)
	q := newFlowQueue(t, srv.ClientURL(), "flow-exhausted")

	var calls atomic.Int32
	runConsume(t, q, func(_ context.Context, _ *gobot.QueueTask) error {
		calls.Add(1)
		return errors.New("会话不存在")
	})

	if err := q.Publish(context.Background(), inspectTask()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	// MaxDeliver=2：首次 + AckWait 重投各一次后终结
	waitUntil(t, 3*time.Second, func() bool { return calls.Load() >= 2 })
	time.Sleep(300 * time.Millisecond)

	if got := calls.Load(); got != 2 {
		t.Fatalf("投递次数 = %d, 期望终结于 2 次", got)
	}
}

// TestConsumeDiscardsCorruptPayload 验证损坏载荷被直接终结，
// 不回调 handler 也不无限重投
func TestConsumeDiscardsCorruptPayload(t *testing.T) {
	srv := newJSServer(t)
	q := newFlowQueue(t, srv.ClientURL(), "flow-corrupt")

	var calls atomic.Int32
	runConsume(t, q, func(_ context.Context, _ *gobot.QueueTask) error {
		calls.Add(1)
		return nil
	})
	// 先同步确保流已建，再直接发布一条损坏载荷
	// （不经过 Queue.Publish 的校验，且不与 Consume 建流竞态）
	if err := q.ensureStream(context.Background()); err != nil {
		t.Fatalf("ensureStream() error = %v", err)
	}
	if _, err := q.js.Publish(context.Background(), q.cfg.Subject, []byte("{not-json")); err != nil {
		t.Fatalf("js.Publish() error = %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	if got := calls.Load(); got != 0 {
		t.Fatalf("损坏载荷不应回调 handler, 调用次数 = %d", got)
	}
}

// TestConsumeLoadBalances 验证两个实例共享 durable consumer 时
// 每条任务只被一个实例投递，无重复无遗漏
func TestConsumeLoadBalances(t *testing.T) {
	srv := newJSServer(t)
	url := srv.ClientURL()
	q1 := newFlowQueue(t, url, "flow-balance")
	q2 := newFlowQueue(t, url, "flow-balance")

	var mu sync.Mutex
	delivered := make(map[string]int)
	record := func(_ context.Context, task *gobot.QueueTask) error {
		mu.Lock()
		delivered[task.ID]++
		mu.Unlock()
		return nil
	}
	runConsume(t, q1, record)
	runConsume(t, q2, record)

	ids := []string{"bal-1", "bal-2", "bal-3", "bal-4", "bal-5"}
	for _, id := range ids {
		task := inspectTask()
		task.ID = id
		if err := q1.Publish(context.Background(), task); err != nil {
			t.Fatalf("Publish(%s) error = %v", id, err)
		}
	}
	waitUntil(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(delivered) == len(ids)
	})

	mu.Lock()
	defer mu.Unlock()
	for _, id := range ids {
		if got := delivered[id]; got != 1 {
			t.Fatalf("任务 %s 投递次数 = %d, 期望恰好一次（负载均衡不重复）", id, got)
		}
	}
}

// TestConsumeStopsOnCancel 验证 ctx 取消后消费循环退出并返回 ctx.Err
func TestConsumeStopsOnCancel(t *testing.T) {
	srv := newJSServer(t)
	q := newFlowQueue(t, srv.ClientURL(), "flow-cancel")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- q.Consume(ctx, func(context.Context, *gobot.QueueTask) error { return nil }) }()
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Consume() 返回 %v, 期望 context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Consume 未随 ctx 取消退出")
	}
}
