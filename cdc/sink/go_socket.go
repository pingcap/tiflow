package sink

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

)



//发送信息
func sender(jsonStr *RowJson) {

	fmt.Println(">>>>>>>>>>>>>>>>>>jsonStr:",jsonStr)
	server := "127.0.0.1:9300"
	tcpAddr, err := net.ResolveTCPAddr("tcp4", server)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
	fmt.Println("connection success")

	words, _ := json.Marshal(jsonStr)
	fmt.Println(string(words))
	fmt.Println("words:::::::::::::::",words)
	conn.Write(words)
	fmt.Println("send over")

	//接收服务端反馈
	buffer := make([]byte, 2048)

	n, err := conn.Read(buffer)
	if err != nil {
		Log(conn.RemoteAddr().String(), "waiting server back msg error: ", err)
		return
	}
	Log(conn.RemoteAddr().String(), "receive server back msg: ", string(buffer[:n]))

}
//日志
func Log(v ...interface{}) {
	log.Println(v...)
}

//func main() {
	/*server := "127.0.0.1:1024"
	tcpAddr, err := net.ResolveTCPAddr("tcp4", server)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}*/

//	sender()
//}
