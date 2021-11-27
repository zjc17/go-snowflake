package snowflake

import (
	"errors"
	"fmt"
	sf "github.com/bwmarrin/snowflake"
	"github.com/go-redis/redis/v8"
	"time"
)

var (
	// FailCallbackFunc node id 失效后的回调函数，一般用于更新健康检查重启 pod
	FailCallbackFunc = func() {}
	// RedisClient 默认使用 redis 进行分布式 node id 配置及心跳机制
	// 使用默认 NodeIdGeneratorFunc 和 HeartBeatFunc 时必须设置
	RedisClient redis.UniversalClient
	// NodeIdGeneratorFunc 允许自定义的分布式ID生成器
	NodeIdGeneratorFunc DistributeIdGenerator = defaultGenerateDistributedNodeId
	// HeartBeatFunc 允许自定义的保持占有 node id 的心跳函数
	HeartBeatFunc HeartBeatExecutor = defaultHeartBeat

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

	if nodeId, err = NodeIdGeneratorFunc(identifier); err != nil {
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
	go HeartBeatFunc(node)
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

func customSnowflake() {
	sf.Epoch = Epoch
	sf.NodeBits = NodeBits
	sf.StepBits = StepBits
}
