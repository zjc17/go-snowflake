package snowflake

import (
	"context"
	"fmt"
	"time"
)

type HeartBeatExecutor func(node *Node)

func (f HeartBeatExecutor) HeartBeat(node *Node) {
	f(node)
}

func defaultHeartBeat(node *Node) {
	nodeId := node.NodeId
	if node.Identifier == "" {
		fmt.Println("Snowflake ID generated in debug mode. Heart beat auto-disabled")
		return
	}
	fmt.Println("Snowflake heartbeat starts")
	for {
		time.Sleep(HeartbeatInterval)
		if res, err := RedisClient.Exists(context.Background(), getPreemptRedisKey(nodeId)).Result(); err != nil {
			fmt.Printf("Snowflake node id occupied failed. %+v\n", err)
			continue
		} else if res != 1 {
			panic("should not happen")
		}
		if _, err := RedisClient.SetEX(context.Background(), getPreemptRedisKey(nodeId), node.Identifier, OccupiedInterval).Result(); err != nil {
			fmt.Printf("Snowflake node_id = %d occupied failed. %+v\n", nodeId, err)
			continue
		}
		lastOccupiedTimeMillis = time.Now().UnixMilli()
	}
}
