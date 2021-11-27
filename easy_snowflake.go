package snowflake

import (
	"context"
	"errors"
	"fmt"
	sf "github.com/bwmarrin/snowflake"
	"github.com/go-redis/redis/v8"
	"hash/fnv"
	"time"
)

var (
	RedisClient redis.UniversalClient

	FailCallbackFunc = func() {}

	NodeKeyTemplate = "snowflake:node:id:%d"

	HeartbeatInterval = 5 * time.Second
	OccupiedInterval  = 40 * time.Second
	FailThreshold     = 20 * time.Second // 必须小于 OccupiedInterval

	lastOccupiedTimeMillis int64
)

var (
	Epoch    int64 = 1577836800000 // 设置 Epoch 为 2020/1/1 8:00
	NodeBits uint8 = 8             // 256 nodes
	StepBits uint8 = 14            // 16384 per millisecond
)

type Node struct {
	node       *sf.Node
	NodeId     uint8
	Identifier string
}

func NewNode(identifier string) (node *Node, nodeId uint8, err error) {
	// 自定义 snowflake 格式
	customSnowflake()
	if nodeId, err = generateDistributedNodeId(identifier); err != nil {
		return
	}
	lastOccupiedTimeMillis = time.Now().UnixMilli()
	// 创建 snowflake node
	var sNode *sf.Node
	if sNode, err = sf.NewNode(int64(nodeId)); err != nil {
		fmt.Printf("Cannot create snowflake node. nodeId=%d, identifer=%s\n", nodeId, identifier)
		return
	}
	fmt.Printf("Success assign snowflake worker node. node_id = [%d], pod_ip = [%s]\n", nodeId, identifier)
	node = &Node{
		node:       sNode,
		NodeId:     nodeId,
		Identifier: identifier,
	}
	go func(node *Node) {
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
			if _, err := RedisClient.SetEX(context.Background(), getPreemptRedisKey(nodeId), identifier, OccupiedInterval).Result(); err != nil {
				fmt.Printf("Snowflake node_id = %d occupied failed. %+v\n", nodeId, err)
				continue
			}
			lastOccupiedTimeMillis = time.Now().UnixMilli()
		}
	}(node)
	return
}

func (n *Node) Generate() (sf.ID, error) {
	if time.Now().UnixMilli()-lastOccupiedTimeMillis > FailThreshold.Milliseconds() {
		FailCallbackFunc()
		fmt.Println("Snowflake node id occupied timeout.")
		return 0, errors.New("snowflake error. wait for restarting")
	}
	return n.node.Generate(), nil
}

func generateDistributedNodeId(identifier string) (nodeId uint8, err error) {
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

func getPreemptRedisKey(nodeId uint8) string {
	return fmt.Sprintf(NodeKeyTemplate, nodeId)
}

func customSnowflake() {
	sf.Epoch = Epoch
	sf.NodeBits = NodeBits
	sf.StepBits = StepBits
}

func hash(s string) uint8 {
	h := fnv.New32a()
	if _, err := h.Write([]byte(s)); err != nil {
		panic("Hash to get snowflake node id failed")
	}
	return uint8(h.Sum32())
}
