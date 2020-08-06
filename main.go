package main

import(
	"os"
	"strconv"
	"fmt"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"net/http"
	"math/rand"
	"time"
	"sync"
	//"runtime"
)
type nodeInfo struct{
	id string
	port string
}
//声明节点对象类型Raft
type Raft struct {
	node nodeInfo
	mu   sync.Mutex
	//当前节点编号
	myId          int
	//currentTerm   int
	//votedFor      int
	//0是follower, 1是candidate，2是leader
	//state         int
	//多少时间内没接收到心跳则重新发起投票
	//timeout       int
	//currentLeader int
	//该节点最后一次处理数据的时间
	//lastMessageTime int64

	//message   chan bool
	//electCh  chan bool
	//heartbeat chan bool
	//子节点给主节点返回心跳信号
	//heartbeatRe chan bool
}

//声明leader对象
type Leader struct {
	//任期
	//Term int
	//leader 编号
	//LeaderId int
}
//设置发送参数的数据类型
type Param struct {
	//Msg   string
	//MsgId string
	//Arg   Leader
}

//设置节点个数
const raftCount = 3
//var leader = Leader{0,-1}
//存储缓存信息
//var bufferMessage = make(map[string]string)
//处理数据库信息
//var mysqlMessage =  make(map[string]string)
//操作消息数组下标
//var messageId =1
// 用nodeTable 存储每个节点中的键值对
var nodeTable map[string]string

var electCh = make(chan bool,2)

func main(){
	if  len(os.Args) > 0 {
		userId := os.Args[1]
		//转为int类型
		id,_ := strconv.Atoi(userId)
		fmt.Println(id)
		nodeTable = map[string]string{
			"1": ":8000",
			"2": ":8001",
			"3": ":8002",
		}
		//封装nodeInfo对象,本ID, id:1,port:8000
		node := nodeInfo{id:userId,port:nodeTable[userId]}
		//创建节点对象
		rf := Make(id)
		//确保每个新建节点都有端口对应
		
		rf.node = node
		
		//注册rpc
		go func(){
			//注册rpc,为了实现远程连接
			//rf.raftRegisterRPC(node.port)
			rf.raftRegisterJsonRPC(node.port)
		}()
	}
	for {;
	}
}
//初始化本节点
func Make(me int) *Raft {
	rf := &Raft{}
	rf.myId = me
	//rf.votedFor = -1
	//rf.state = 0
	//rf.timeout = 0 
	//rf.currentLeader = -1
	//rf.setTerm(0)
	//初始化通道
	//rf.message = make(chan bool)
	//rf.heartbeat = make(chan bool)
	//rf.heartbeatRe = make(chan bool)
	//rf.electCh = make(chan bool,2)
	//每个节点都有选举权，启动之初就进行选举
	go rf.election()
	//每个节点都有心跳功能
	//go rf.sendLeaderHeartBeat()
	
	return rf
}

func (node *Raft) raftRegisterRPC(port string){
	//注册一个服务器
	rpc.Register(node)
	//把服务绑定到http协议上
	rpc.HandleHTTP()
	err:=http.ListenAndServe(port,nil)
	if err!= nil {
		fmt.Println("注册rpc服务失败:",err)
	}
}

func  (node *Raft) raftRegisterJsonRPC(port string) {
    rpc.Register(node)
    l, err := net.Listen("tcp", port)
    if err != nil {
        fmt.Printf("Listener tcp err: %s", err)
        return
    }
    for {
        fmt.Println("waiting...")
        conn, err := l.Accept()
        if err != nil {
            fmt.Printf("accept connection err: %s\n", conn)
            continue
        }
        go jsonrpc.ServeConn(conn)
    }
}

func (rf *Raft) election() {
	var result bool
	//每隔一段时间发一次心跳
	for {
		//发起拉票之前，设置延时时间，设置随机值
		timeout := randomRange(1500, 3000)
		//timeout := 2000
		fmt.Println("发起拉票之前，延时时间为：", timeout)
		//设置该节点最有一次处理消息的时间
		//rf.lastMessageTime = milliseconds()
		
		select {
		//间隔时间为1500-3000ms的随机值
		case <-time.After(time.Duration(timeout) * time.Millisecond):
		}
		result = rf.election_one_round()
		if result {
			fmt.Println("一共收到两个投票，完成一轮完整投票过程")
		}else{
			fmt.Println("没有完成一轮完整投票过程")
		}
		/* result = false
		for !result {
			//选择leader
			result = rf.election_one_round()
			fmt.Println("完成投票！")
		} */
		//目前有多少个协程
		//fmt.Println("目前系统在跑的协程数量是：",runtime.NumGoroutine())
	}
}

func (rf *Raft) election_one_round() bool {
	//已经有了leader，并且不是自己，那么return
	//if args.LeaderId > -1 && args.LeaderId != rf.me{
	//	fmt.Printf("%d 已是leader，终止%d选举\n",args.LeaderId,rf.me)
	//	return true
	//}
	//var timeout int64
	var vote int
	//var triggerHeartbeat bool
	//timeout = 1000
	//last := milliseconds()
	success := false
	//rf.mu.Lock()
	//rf.becomeCandidate()
	//rf.mu.Unlock()
	//fmt.Printf("candidate=%d start electing leader\n",rf.me)
	//fmt.Println("发送投票请求")
	param := rf.myId

	
	vote = rf.broadcast(param,"Raft.ElectingLeader")
	/* go func(){
		fmt.Println("投票进程开始")
		rf.broadcast(param,"Raft.ElectingLeader")
		fmt.Println("投票进程结束")
	}() */
	//vote = 0
	//triggerHeartbeat = false

	/* timeout := make (chan bool, 1)
	go func() {
		time.Sleep(1e9) // sleep one second
		timeout <- true
	}()
 */
/* 	for i := 0; i < raftCount-1;i++{
		fmt.Printf("发起者=%d 正在等待选举\n",rf.myId)
		select{
		//case ok := <- electCh:
		//	//fmt.Println(ok)
		//	if ok {
		//		vote++
		//		fmt.Println("收到一个节点的投票")
				//success =  rf.currentLeader == -1 && vote >= raftCount/2
		//	}
		case <-time.After(time.Duration(timeout) * time.Millisecond):
		}
		fmt.Printf("一共接收：%d 个投票\n",vote)
	} */
	// timeout + last < milliseconds() 表示没有超时
	//fmt.Println(timeout + last < milliseconds())
/* 	if timeout + last < milliseconds() && vote == 2 {
		fmt.Println("退出")
		break
	}else{
		fmt.Println("继续等待")
		select{
		case <- time.After(time.Duration(5000) * time.Millisecond):
		}
	} */
		
	
	//fmt.Printf("candidate=%d receive votes status=%t\n",rf.me,success)
	//fmt.Println(vote)
	if vote == 2 {
		success = true
	}
	return success
}

//获得当前时间（毫秒）
func milliseconds() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func randomRange(min, max int64) int64 {
	//设置随机时间
	rand.Seed(time.Now().UnixNano())
	return rand.Int63n(max-min) + min
}
func (rf *Raft) broadcast(msg int, path string) int {

	//设置不要自己给自己广播
	vote :=0
	for nodeID, port := range nodeTable {
		//如果是本机，跳过
		
		id := rf.node.id
	
		if nodeID == id {
			continue;
		}
		//rf.mu.Unlock()
		//链接远程rpc
		/* fmt.Println("打点2")
		rf.mu.Lock()
		rp, err := rpc.DialHTTP("tcp", "127.0.0.1"+port)
		rf.mu.Unlock()
		fmt.Println("打点3")
		if err != nil {
			fmt.Println("连接rpc服务器失败")
			fun(false)
			continue
		}
		fmt.Println("打点4")
		defer rp.Close()
		var bo = false
		fmt.Println("打点5")
		err = rp.Call(path, msg, &bo)
		fmt.Println("打点6")
		if err != nil {
			fmt.Println("调用rpc函数失败")
			fun(false)
			continue
		} */
		
		
		//设置连接1000毫秒超时
		timeout := time.Millisecond * 1000
		conn, err := net.DialTimeout("tcp", "127.0.0.1"+port, timeout)
		if err != nil {
			fmt.Println("连接rpc服务器失败")
			//electCh <- false
			continue
		}
		defer conn.Close()
		// timeout 设置读写500毫秒超时。
		readAndWriteTimeout := 500 * time.Millisecond
		err = conn.SetDeadline(time.Now().Add(readAndWriteTimeout))
		if err != nil {
			fmt.Println("SetDeadline failed:", err)
			//electCh <- false
			continue
		}
		client := jsonrpc.NewClient(conn)
		var bo = false
		defer client.Close()
		err = client.Call(path, msg, &bo)
		if err != nil {
			fmt.Println("调用rpc函数失败")
			//electCh <- false
			continue
		}
		//electCh <- bo
		vote++
		fmt.Printf("收到子节点：%s的回复\n",port)
	}

	return vote
}
func (rf *Raft) ElectingLeader(param int, a *bool) error {
	//给leader投票
	*a = true
	fmt.Println("把票投给：",param)
	//if param.Arg.Term < rf.currentTerm {
	//	*a = false
	//	rf.lastMessageTime = milliseconds()
	//
	//}
	return nil
}
