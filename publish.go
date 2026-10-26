/*
* @Author: kamalyes 501893067@qq.com
* @Date: 2026-09-19 15:10:26
* @LastEditors: kamalyes 501893067@qq.com
* @LastEditTime: 2026-10-26 09:41:52
* @FilePath: \go-bot-queue-nats\publish.go
* @Description: NATS JetStream 队列适配器：任务入队与流保障
*
* Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */

package queue

import (
	"context"
	"time"

	gobot "github.com/kamalyes/go-bot"
	"github.com/nats-io/nats.go/jetstream"
)

// Publish 把一条任务发布到主题：JetStream 收到即持久化落流，
// 入队成功返回 PublishAck；多实例共享同一流时全局 FIFO
func (q *Queue) Publish(ctx context.Context, task *gobot.QueueTask) error {
	const op = "Publish"
	if task == nil {
		return gobot.NewValidationError(op, "task is required")
	}
	data, err := marshalTask(task)
	if err != nil {
		return gobot.NewValidationError(op, "task is not serializable: "+err.Error())
	}
	if err := q.ensureStreamOnce(ctx); err != nil {
		return err
	}
	if _, err := q.js.Publish(ctx, q.cfg.Subject, data); err != nil {
		return gobot.NewTransportError(gobot.Platform(""), op, err)
	}
	return nil
}

// ensureStreamOnce 对齐 Redis XADD 的 MKSTREAM 语义：
// 流在首个任务入队前幂等创建，成功后跳过检查，
// 失败时保留待下次 Publish 重试
func (q *Queue) ensureStreamOnce(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.streamReady {
		return nil
	}
	if err := q.ensureStream(ctx); err != nil {
		return err
	}
	q.streamReady = true
	return nil
}

// ensureStream 幂等创建或复用持久化流：绑定配置的主题集合，
// 历史任务保留（削峰场景的"先入队、后起消费者"不丢任务）；
// 配置了保留限值时显式收紧，防止削峰队列无限增长
func (q *Queue) ensureStream(ctx context.Context) error {
	const op = "ensureStream"
	subjects := q.cfg.StreamSubjects
	if len(subjects) == 0 {
		subjects = []string{q.cfg.Subject}
	}
	config := jetstream.StreamConfig{
		Name:     q.cfg.Stream,
		Subjects: subjects,
	}
	if q.cfg.MaxBytes > 0 {
		config.MaxBytes = q.cfg.MaxBytes
	}
	if q.cfg.MaxAge > 0 {
		config.MaxAge = q.cfg.MaxAge
	}
	if _, err := q.js.CreateOrUpdateStream(ctx, config); err != nil {
		return gobot.NewTransportError(gobot.Platform(""), op, err)
	}
	return nil
}

// ensureConsumer 幂等创建或复用 durable pull consumer：
// 过滤配置的主题、AckWait 内未确认的消息由 JetStream 重投，
// MaxDeliver 由 JetStream 侧兜底限制
func (q *Queue) ensureConsumer(ctx context.Context) (jetstream.Consumer, error) {
	const op = "ensureConsumer"
	consumer, err := q.js.CreateOrUpdateConsumer(ctx, q.cfg.Stream, jetstream.ConsumerConfig{
		Durable:       q.cfg.Durable,
		FilterSubject: q.cfg.Subject,
		AckWait:       q.cfg.AckWait,
		MaxDeliver:    q.cfg.MaxDeliver,
	})
	if err != nil {
		return nil, gobot.NewTransportError(gobot.Platform(""), op, err)
	}
	return consumer, nil
}

// fetchTimeout 剥离 ctx 截止的拉取等待时长，避免 ctx 已超时仍长时间阻塞
func (q *Queue) fetchTimeout(ctx context.Context) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		if remain := time.Until(deadline); remain < q.cfg.FetchTimeout {
			return remain
		}
	}
	return q.cfg.FetchTimeout
}
