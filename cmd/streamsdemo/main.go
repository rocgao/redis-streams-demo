package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"os"
	"sync"
	"time"
)

const (
	StreamKey = "redis_streams"
	GroupName = "redis-streams-group"
)

var (
	workerWait = sync.WaitGroup{}
	client     *redis.Client
)

func main() {
	// 连接redis
	client = redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	// 启动worker
	workerWait.Add(2)
	consumerName := getName()
	go consume(consumerName)
	go checkHistory(consumerName)

	// 等待所有worker退出
	workerWait.Wait()
}

func consume(name string) {
	for {
		// 打印提示信息
		fmt.Printf("%s waiting...", name)
		println()

		// 等待新消息
		data, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
			Streams:  []string{StreamKey, ">"},
			Group:    GroupName,
			Consumer: name,
			Count:    1,
			Block:    0,
		}).Result()
		if err != nil {
			println(err.Error())
			break
		}

		// 处理新消息
		messages := data[0].Messages
		printXMessages(true, messages)
		ackXMessages(messages)
	}
	workerWait.Done()
}

func checkHistory(name string) {
	for {
		t := <-time.After(time.Second * 5)
		fmt.Printf("%s %s 正在检查历史消息...", formatTime(t), name)
		println()

		// 检查新消息
		doCheckHistory(name)
	}
	workerWait.Done()
}

func doCheckHistory(name string) {
	lastId := "0-0"
	for {
		data, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
			Streams:  []string{StreamKey, lastId},
			Group:    GroupName,
			Consumer: name,
			Count:    1,
			Block:    time.Second * 2,
		}).Result()
		if err != nil {
			println(err.Error())
			break
		}

		// 处理新消息
		messages := data[0].Messages
		if len(messages) == 0 {
			println("没有历史消息")
			break
		}
		printXMessages(false, messages)
		ackXMessages(messages)
	}
}

func printXMessages(isNew bool, messages []redis.XMessage) {
	print(formatTime(time.Now()))
	print(" ")
	if isNew {
		print("收到新消息 -> ")
	} else {
		print("收到历史消息 ->> ")
	}

	for _, msg := range messages {
		print("ID:")
		print(msg.ID)

		for k, v := range msg.Values {
			if sv, ok := v.(string); ok {
				print(" ")
				print(k)
				print("=")
				print(sv)
			}
		}

		println()
	}
}

func ackXMessages(messages []redis.XMessage) {
	var idList []string
	for _, msg := range messages {
		idList = append(idList, msg.ID)
	}

	if err := client.XAck(context.Background(), StreamKey, GroupName, idList...).Err(); err != nil {
		fmt.Printf("确认消息出错：%s", err.Error())
		return
	}

	println("消息已确认")
}

func getName() string {
	args := os.Args
	if len(args) < 2 {
		panic("未指定消费者名称")
	}
	return args[1]
}

func formatTime(t time.Time) string {
	return t.Format("2006-01-02T15:04:05.999")
}
