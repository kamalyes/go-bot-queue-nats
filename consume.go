/*
* @Author: kamalyes 501893067@qq.com
* @Date: 2026-09-19 13:11:26
* @LastEditors: kamalyes 501893067@qq.com
* @LastEditTime: 2026-09-19 13:21:07
* @FilePath: \go-bot-queue-nats\consume.go
* @Description: NATS JetStream 队列适配器：竞争消费、确认与终结
*
* Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */

package queue

import (
	"context"
	"encoding/json"

	gobot "github.com/kamalyes/go-bot"
	"github.com/nats-io/nats.go/jetstream"
)

// Consume 阻塞消费直到 ctx 取消：durable pull consumer 拉取任务，
// handler 返回 nil 即 Ack 确认删除，返回 error 则不确认，
// AckWait 超时后由 JetStream 重投（含其他实例，故障转移）；
// 投递次数达到 MaxDeliver 时 Term 终结丢弃并记录日志。
// 返回 ctx.Err() 表示正常停机
func (q *Queue) Consume(ctx context.Context, handler gobot.QueueHandler) error {
	const op = "Consume"
	if handler == nil {
		return gobot.NewValidationError(op, "handler is required")
	}
	if err := q.ensureStream(ctx); err != nil {
		return err
	}
	consumer, err := q.ensureConsumer(ctx)
	if err != nil {
		return err
	}
	for ctx.Err() == nil {
		batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(q.fetchTimeout(ctx)))
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return gobot.NewTransportError(gobot.Platform(""), "Fetch", err)
		}
		if err := q.drain(ctx, batch, handler); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// drain 处理一次拉取批次中的全部消息
func (q *Queue) drain(ctx context.Context, batch jetstream.MessageBatch, handler gobot.QueueHandler) error {
	for msg := range batch.Messages() {
		q.dispatch(ctx, msg, handler)
	}
	if err := batch.Error(); err != nil {
		return gobot.NewTransportError(gobot.Platform(""), "Fetch", err)
	}
	return nil
}

// dispatch 反序列化并投递一条消息：成功 Ack 确认删除；
// 失败且投递次数未耗尽则不确认，等待 AckWait 重投；
// 失败且已达到 MaxDeliver 则 Term 终结丢弃
func (q *Queue) dispatch(ctx context.Context, msg jetstream.Msg, handler gobot.QueueHandler) {
	task, err := unmarshalTask(msg)
	if err != nil {
		// 脏数据无法投递，终结删除避免无限重投
		q.logger().ErrorContext(ctx, "queue: task payload is corrupt, terminated",
			"subject", q.cfg.Subject, "error", err)
		_ = msg.Term()
		return
	}
	meta, err := msg.Metadata()
	if err != nil {
		_ = msg.Term()
		return
	}
	if err := handler(ctx, task); err != nil {
		if meta.NumDelivered >= uint64(q.cfg.MaxDeliver) {
			q.logger().ErrorContext(ctx, "queue: task exhausted max deliver, terminated",
				"subject", q.cfg.Subject, "task", task.ID,
				"attempts", meta.NumDelivered, "error", err)
			_ = msg.Term()
			return
		}
		q.logger().WarnContext(ctx, "queue: deliver failed, left unacked for redelivery",
			"subject", q.cfg.Subject, "task", task.ID,
			"attempt", meta.NumDelivered, "error", err)
		return
	}
	if err := msg.Ack(); err != nil {
		q.logger().WarnContext(ctx, "queue: ack failed",
			"subject", q.cfg.Subject, "task", task.ID, "error", err)
	}
}

// marshalTask 把任务序列化为消息载荷
func marshalTask(task *gobot.QueueTask) ([]byte, error) {
	return json.Marshal(task)
}

// unmarshalTask 从消息载荷还原任务，载荷损坏时返回错误
func unmarshalTask(msg jetstream.Msg) (*gobot.QueueTask, error) {
	var task gobot.QueueTask
	if err := json.Unmarshal(msg.Data(), &task); err != nil {
		return nil, err
	}
	return &task, nil
}
