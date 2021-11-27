package snowflake

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"hash/fnv"
	"time"

	sf "github.com/zjc17/snowflake"
)

var (
	RedisClient redis.UniversalClient

	FailCallbackFunc = func() {}

	NodeKeyTemplate = "snowflake:node:id:%d"

	HeartbeatInterval  = 3 * time.Second
	OccupiedInterval   = 1 * time.Minute
	FastFailedInterval = 45 * time.Second // 必须小于 OccupiedInterval

	lastOccupiedTimeMillis int64
)

type Node struct {
	node       *sf.Node
	NodeId     uint8
	Identifier string
}

func NewNode(identifier string) (node *Node, nodeId uint8, err error) {
	// 自定义 snowflake 格式
	customSnowflake()
	nodeId = generateDistributedNodeId(identifier)
	lastOccupiedTimeMillis = time.Now().UnixMilli()
	// 创建 snowflake node
	var sNode *sf.Node
	if sNode, err = sf.NewNode(int64(nodeId)); err != nil {
		fmt.Printf("Cannot create snowflake node. nodeId=%d, identifer=%s", nodeId, identifier)
		return
	}
	fmt.Printf("Success assign snowflake worker. node_id = %d, pod_ip = %s\n", nodeId, identifier)
	node = &Node{
		node:       sNode,
		NodeId:     nodeId,
		Identifier: identifier,
	}
	go func(node *Node) {
		nodeId := node.NodeId
		for {
			time.Sleep(HeartbeatInterval)
			if res, err := RedisClient.Exists(nil, getPreemptRedisKey(nodeId)).Result(); err != nil {
				fmt.Printf("Snowflake node id occupied failed. %+v\n", err)
				continue
			} else if res != 1 {
				fmt.Println("should not happen")
				fmt.Printf("snowflake node stop occupy node_id = %d\n", nodeId)
				return
			}
			if _, err := RedisClient.SetEX(nil, getPreemptRedisKey(nodeId), identifier, OccupiedInterval).Result(); err != nil {
				fmt.Printf("Snowflake node_id = %d occupied failed. %+v\n", nodeId, err)
				continue
			}
			lastOccupiedTimeMillis = time.Now().UnixMilli()
		}
	}(node)
	return
}

func (n *Node) Generate() (sf.ID, error) {
	if time.Now().UnixMilli()-lastOccupiedTimeMillis > FastFailedInterval.Milliseconds() {
		FailCallbackFunc()
		fmt.Println("Snowflake node id occupied timeout.")
		return 0, errors.New("snowflake error. wait for restarting")
	}
	return n.node.Generate(), nil
}

func generateDistributedNodeId(identifier string) (nodeId uint8) {
	// 生成 NODE ID
	if identifier == "" {
		fmt.Println("Snowflake node identifier not set. Node id always 1")
		nodeId = 1
	} else {
		fmt.Printf("snowflake node identifier set. node identifier = %s", identifier)
		nodeId = hash(identifier)
		// 抢占 Node ID
		preemptNodeID(identifier, nodeId)
	}
	return
}

func preemptNodeID(identifier string, nodeId uint8) {
	ctx, cancel := context.WithTimeout(context.Background(), HeartbeatInterval)
	defer cancel()
	if success, err := RedisClient.SetNX(ctx, getPreemptRedisKey(nodeId), identifier, OccupiedInterval).Result(); err != nil {
		panic(fmt.Sprintf("Cannot occupy snowflake node. nodeId=%d, podId=%s, err=%+v", nodeId, identifier, err))
	} else {
		if !success {
			panic(fmt.Sprintf("Cannot occupy snowflake node. nodeId=%d, podId=%s", nodeId, identifier))
		}
	}
}

func getPreemptRedisKey(nodeId uint8) string {
	return fmt.Sprintf(NodeKeyTemplate, nodeId)
}

func customSnowflake() {
	sf.Epoch = 1577836800000 // 设置 Epoch 为 2020/1/1 8:00
	sf.NodeBits = 8          // 256 nodes
	sf.StepBits = 14         // 16384 per millisecond
}

func hash(s string) uint8 {
	h := fnv.New32a()
	if _, err := h.Write([]byte(s)); err != nil {
		panic("Hash to get snowflake node id failed")
	}
	return uint8(h.Sum32())
}
