package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"os"
	"strings"
)

func main() {
	// 打印消费者名称
	consumerName := getName()
	fmt.Printf("消费者-%s", consumerName)
	fmt.Println()

	// 连接redis
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	// 消费数据
	for true {
		data, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
			Streams:  []string{"redis_streams", ">"},
			Group:    "redis-streams-group",
			Consumer: consumerName,
			Block:    0,
		}).Result()
		if err != nil {
			println(err.Error())
			break
		}

		for _, dataValue := range data {
			outputs := []string{"收到消息 -> "}
			outputs = append(outputs, " stream:", dataValue.Stream)
			for _, msg := range dataValue.Messages {
				outputs = append(outputs, " ID:", msg.ID)
				for k, v := range msg.Values {
					if sv, ok := v.(string); ok {
						outputs = append(outputs, " ", k, "=", sv)
					}
				}
			}
			println(strings.Join(outputs, ""))
		}
	}

}

func getName() string {
	args := os.Args
	if len(args) < 2 {
		panic("未指定消费者名称")
	}
	return args[1]
}
