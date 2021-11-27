package snowflake

import (
	"context"
	"fmt"
	"hash/fnv"
)

type DistributeIdGenerator func(key string) (id uint8, err error)

func (f DistributeIdGenerator) Produce(key string) (id uint8, err error) {
	return f(key)
}

func defaultGenerateDistributedNodeId(identifier string) (nodeId uint8, err error) {
	// 生成 NODE ID
	if identifier == "" {
		fmt.Println("Snowflake node identifier not set. Node id always 1")
		nodeId = 1
	} else {
		nodeId = hash(identifier)
		//fmt.Printf("Snowflake node identifier set. node identifier = %s, node id = %d\n", identifier, nodeId)
		// 抢占 Node ID
		if err = preemptNodeID(identifier, nodeId); err != nil {
			return
		}
	}
	return
}

func preemptNodeID(identifier string, nodeId uint8) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), HeartbeatInterval)
	defer cancel()
	if success, err := RedisClient.SetNX(ctx, getPreemptRedisKey(nodeId), identifier, OccupiedInterval).Result(); err != nil {
		return fmt.Errorf("cannot occupy snowflake node. nodeId=%d, podId=%s, err=%+v", nodeId, identifier, err)
	} else if !success {
		return fmt.Errorf("cannot occupy snowflake node. nodeId=%d, podId=%s. node id in used", nodeId, identifier)
	}
	if success, err := RedisClient.Expire(ctx, getPreemptRedisKey(nodeId), OccupiedInterval).Result(); err != nil {
		return fmt.Errorf("cannot occupy snowflake node. nodeId=%d, podId=%s, err=%+v", nodeId, identifier, err)
	} else if !success {
		return fmt.Errorf("cannot occupy snowflake node. nodeId=%d, podId=%s. should not happen", nodeId, identifier)
	}
	return
}

func hash(s string) uint8 {
	h := fnv.New32a()
	if _, err := h.Write([]byte(s)); err != nil {
		panic("Hash to get snowflake node id failed")
	}
	return uint8(h.Sum32())
}

func getPreemptRedisKey(nodeId uint8) string {
	return fmt.Sprintf(NodeKeyTemplate, nodeId)
}
