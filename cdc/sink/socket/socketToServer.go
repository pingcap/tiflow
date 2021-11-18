package socket

import (
	"bytes"
	"fmt"
	"github.com/pingcap/ticdc/cdc/sink/publicUtils"
	"github.com/pingcap/ticdc/cdc/sink/vo"
	"log"
	"net"
	"os"
	"time"
	//"unsafe"
)

var conn net.Conn
var ConnMap map[string]net.Conn = make(map[string]net.Conn)

var tcpconn *net.TCPConn
var timeout = time.Duration(60)*time.Second

func CheckError(err error) {
    if err != nil {
        fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
        os.Exit(1)
    }
}

func getLocalIp(){

	addres, err := net.InterfaceAddrs()
	 if err != nil {
		fmt.Println(err)
		return
	}

	for _, value := range addres{

		if ipnet , ok := value.(*net.IPNet); ok && ! ipnet.IP.IsLoopback(){

			if ipnet.IP.To4() != nil {

				fmt.Println("---> IP ::"+ipnet.IP.String())
			}
		}
	}


}

type SliceMock struct {
	addr uintptr
	len  int
}

func chkError(err error) {
    if err != nil {
        log.Fatal(err);
    }
}

func JddmDDLClient_ByResolveTcp(host string,ddlInfos *vo.DDLInfos){


	if tcpconn != nil {

	}else{

		//ResolveTCPAddr用于获取一个TCPAddr
	    //net参数是"tcp4"、"tcp6"、"tcp"
	    //addr表示域名或IP地址加端口号
	    tcpaddr, err := net.ResolveTCPAddr("tcp", host);
	    chkError(err);

	    //DialTCP建立一个TCP连接
	    //net参数是"tcp4"、"tcp6"、"tcp"
	    //laddr表示本机地址，一般设为nil
	    //raddr表示远程地址
	    tempTcp, err2 := net.DialTCP("tcp", nil, tcpaddr)
		tcpconn = tempTcp
	    chkError(err2)
	}

}

func JddmDDLClient(host string,ddlInfos *vo.DDLInfos){


	_, ok := ConnMap [host]
	if(!ok){
		tempConn, err := net.DialTimeout("tcp", host,timeout)
		//conn = tempConn
		ConnMap [host] = tempConn
		if err != nil {
			delete(ConnMap, host)
			//conn = nil
			fmt.Println("Error dialing", err.Error())
			return
		}
	}

	/*if(conn ==nil){

		//fmt.Printf(" Go Engine Set Socket Server ::[%s] \n",host)
		tempConn, err := net.DialTimeout("tcp", host,timeout)
		conn = tempConn
	    if err != nil {
			conn = nil
	        fmt.Println("Error dialing", err.Error())
	        return
	    }
	}*/
    //serviceNum := 1742
	//// 前4bytes消息长度
	//lengthArr := make([]byte,4)


	//serviceNumArr := publicUtils.IntTo2Bytes(serviceNum)
	//tradCodeArr := publicUtils.IntTo2Bytes(113)
	// 4-8bytes服务号和主次命令
	//verifyArr := make([]byte,4)
	verifyArr := make([]byte,4)
	//serviceNumArr := make([]byte,4)

	verifyArr[0] = 0x06
	verifyArr[1] = 0xce
	verifyArr[2] = 0x01
	verifyArr[3] = 0x13
	//serviceNumArr = intTo4Bytes(serviceNum)
	//fmt.Printf(" %s \n",publicUtils.BytestoHex(verifyArr))
	//verifyArr[0] := 0x06;
	//verifyArr[1] := 0xCE;

	//校验码
	//verifyArr[2] := 0x01;
	//verifyArr[3] := 0x13;

    //defer conn.Close()


    sendMsg :=" Connect Server Test  !";
	clientSendArr := make([]byte,8+len(sendMsg))
	lengthArr := publicUtils.IntegerToBytes(len(sendMsg));
	//fmt.Printf(" %s \n",publicUtils.BytestoHex(lengthArr))
	publicUtils.BlockByteArrCopy([]byte(lengthArr),0,clientSendArr,0,len(lengthArr))
	publicUtils.BlockByteArrCopy([]byte(verifyArr),0,clientSendArr,4,len(verifyArr))
	publicUtils.BlockByteArrCopy([]byte(sendMsg),0,clientSendArr,8,len(sendMsg))
	//fmt.Printf(" SendByte[]Arr %s \n",publicUtils.BytestoHex(clientSendArr))



	//fmt.Println("rowInfos：：：：：：：：：：：：：：：：：：：：：：：：",ddlInfos)
	//=============================================
	/***
	row1 := new(vo.RowInfos)
	row1.SchemaName = "dsg_test1123456789"
	row1.TableName = "tbl_pk"
	row1.ColumnNo=2

	columnInfos2 :=make([]*vo.ColumnVo,0);
	columnVo2 := new(vo.ColumnVo)
	columnVo2.ColumnName =" column_1"
	columnVo2.ColumnValue = "row_2_column_1 -> Value_01"

	columnInfos2 = append(columnInfos2,columnVo2)

	columnVo3 := new(vo.ColumnVo)
	columnVo3.ColumnName =" column_2"
	columnVo3.ColumnValue = "row_2_column_2 -> Value_02"
	columnInfos2 = append(columnInfos2,columnVo3)

	row1.ColumnList = columnInfos2;
	rowInfos = append(rowInfos,row1)
	//createBytesFromRowInfoList(rowInfos);
	***/

	//_, err := conn.Write(createBytes_FromDdlInfoVo(ddlInfos))
	_, err := ConnMap [host].Write(createBytes_FromDdlInfoVo(ddlInfos))
    if err != nil {
		fmt.Println("Error dialing", err.Error())
		return
	}


}


func JddmClient(host string, rowInfos []*vo.RowInfos){

	/*fmt.Printf(" Go Engine Input Host Port: [%d]-- ToString(%s)\n",hostPort,strconv.Itoa(hostPort))
	serverAddress := hostIpAddress+":"+strconv.Itoa(hostPort)*/

	fmt.Printf(" Go Engine Set Socket Server ::[%s] \n",conn)

	_, ok := ConnMap [host]
	if(!ok){
		tempConn, err := net.DialTimeout("tcp", host,timeout)
		//conn = tempConn
		ConnMap [host] = tempConn
		if err != nil {
			delete(ConnMap, host)
			//conn = nil
			fmt.Println("Error dialing", err.Error())
			return
		}
	}



	// 4-8bytes服务号和主次命令
	verifyArr := make([]byte,4)

	verifyArr[0] = 0x06
	verifyArr[1] = 0xce
	verifyArr[2] = 0x01
	verifyArr[3] = 0x13



	sendMsg :=" Connect Server Test  !";
	clientSendArr := make([]byte,8+len(sendMsg))
	lengthArr := publicUtils.IntegerToBytes(len(sendMsg));
	//fmt.Printf(" %s \n",publicUtils.BytestoHex(lengthArr))
	publicUtils.BlockByteArrCopy([]byte(lengthArr),0,clientSendArr,0,len(lengthArr))
	publicUtils.BlockByteArrCopy([]byte(verifyArr),0,clientSendArr,4,len(verifyArr))
	publicUtils.BlockByteArrCopy([]byte(sendMsg),0,clientSendArr,8,len(sendMsg))
	fmt.Printf(" SendByte[]Arr %s \n",publicUtils.BytestoHex(clientSendArr))

	fmt.Println("rowInfos：：：：：：：：：：：：：：：：：：：：：：：：",rowInfos)

	ConnMap [host].SetWriteDeadline(time.Now().Add(timeout))
	_, err := ConnMap [host].Write(createBytesFromRowInfo(rowInfos))


	if err != nil {
		delete(ConnMap, host)
		log.Println("setReadDeadline failed:", err)

		return
	}


}

func createBytesFromRowInfo(rowInfos []*vo.RowInfos) []byte{

	//var buffer bytes.Buffer
	//colsArr = make([]byte,0)
	fmt.Printf(" rowCount = %d\n", len(rowInfos))


	verifyArr := make([]byte,4)
	//serviceNumArr := make([]byte,4)

	verifyArr[0] = 0x06
	verifyArr[1] = 0xce
	verifyArr[2] = 0x01
	verifyArr[3] = 0x13

	buffer := new(bytes.Buffer)   //直接使用 new 初始化，可以直接使用
	sendBatchRowsArr :=new(bytes.Buffer)
	for _, rowInfo := range rowInfos {

		//当前时间戳
	    t1 := time.Now().Unix()  //1564552562
	    fmt.Println(t1)

		fmt.Println(rowInfo.TableName+"::::"+rowInfo.SchemaName)

		buffer.Write(publicUtils.LongToBytes(rowInfo.StartTimer))
		buffer.Write(publicUtils.LongToBytes(rowInfo.CommitTimer))
		buffer.Write(publicUtils.LongToBytes(rowInfo.ObjnNo))
		buffer.Write(publicUtils.LongToBytes(rowInfo.RowID))
		buffer.Write(publicUtils.Int32ToBytes(rowInfo.ColumnNo))

		operTypeArr := make([]byte,4)
		if rowInfo.OperType==2{
			operTypeArr[3]=byte('I')
			//publicUtils.BlockByteArrCopy([]byte("I"),0,operTypeArr,0,len(rowInfo.SchemaName))

		}else if rowInfo.OperType == 4{
			operTypeArr[3]=byte('D')
		}else if rowInfo.OperType == 3{
			operTypeArr[3]=byte('U')
		}

		buffer.Write(operTypeArr)
		schemaNameArr := make([]byte,1+len(rowInfo.SchemaName))
		publicUtils.BlockByteArrCopy([]byte(rowInfo.SchemaName),0,schemaNameArr,0,len(rowInfo.SchemaName))
		buffer.Write(schemaNameArr)
		tableNameArr := make([]byte,1+len(rowInfo.TableName))
		publicUtils.BlockByteArrCopy([]byte(rowInfo.TableName),0,tableNameArr,0,len(rowInfo.TableName))
		buffer.Write(tableNameArr)



		for _,col2 := range rowInfo.ColumnList{
			//fmt.Println("value:"+col2.ColumnValue+"::name::"+col2.ColumnName)
			//fmt.Println（"%s",col.ColumnValue)

			//allColumnArrByRow = append(allColumnArrByRow,columnInfoVoToByte(col2))
			//colsArr = append(colsArr)
			buffer.Write(columnInfoVoToByte(col2))
		}

		fmt.Printf(" allColumnArrByRow[%d]Arr %s \n",len(buffer.Bytes()),publicUtils.BytestoHex(buffer.Bytes()))
	}
	lengthArr := publicUtils.IntegerToBytes(len(buffer.Bytes())+4)
	sendBatchRowsArr.Write(lengthArr)
	sendBatchRowsArr.Write(verifyArr)
	//增加行数
	sendBatchRowsArr.Write(publicUtils.IntegerToBytes(len(rowInfos)))
	fmt.Printf(" rowCount = %d\n", len(rowInfos))

	sendBatchRowsArr.Write(buffer.Bytes())
	fmt.Printf(" allColumnArrByRow[]Arr %s \n",publicUtils.BytestoHex(sendBatchRowsArr.Bytes()))
	//fmt.Printf(" allColumnArrByRow[%d]Arr %s \n",len(buffer.Bytes()),publicUtils.BytestoHex(buffer.Bytes()))



	return sendBatchRowsArr.Bytes()

}


func createBytes_FromDdlInfoVo(ddlInfos *vo.DDLInfos) []byte{

	verifyArr := make([]byte,4)
	//serviceNumArr := make([]byte,4)

	verifyArr[0] = 0x06
	verifyArr[1] = 0xce
	verifyArr[2] = 0x01
	verifyArr[3] = 0x12

	buffer := new(bytes.Buffer)   //直接使用 new 初始化，可以直接使用
	sendBatchDDLArr :=new(bytes.Buffer)
	//当前时间戳
	t1 := time.Now().Unix()  //1564552562
	fmt.Println(t1)
	fmt.Println(ddlInfos.TableName+"::::"+ddlInfos.SchemaName)
	buffer.Write(publicUtils.LongToBytes(ddlInfos.StartTimer))
	buffer.Write(publicUtils.LongToBytes(ddlInfos.CommitTimer))
	buffer.Write(publicUtils.LongToBytes(ddlInfos.ObjnNo))

	//operTypeArr := make([]byte,4)
	//operTypeArr[3]=byte(12)
	buffer.Write(publicUtils.Int32ToBytes(ddlInfos.DDLType))
	fmt.Printf(" ddl Type:%s\n",buffer.Bytes())
	schemaNameArr := make([]byte,1+len(ddlInfos.SchemaName))
	publicUtils.BlockByteArrCopy([]byte(ddlInfos.SchemaName),0,schemaNameArr,0,len(ddlInfos.SchemaName))
	buffer.Write(schemaNameArr)
	tableNameArr := make([]byte,1+len(ddlInfos.TableName))
	publicUtils.BlockByteArrCopy([]byte(ddlInfos.TableName),0,tableNameArr,0,len(ddlInfos.TableName))
	buffer.Write(tableNameArr)
	buffer.Write(publicUtils.IntegerToBytes(len(ddlInfos.TableInfoList)))
	for _,colInfo := range ddlInfos.TableInfoList{
		fmt.Printf(" nowCol Type:%d ::name::%s\n",colInfo.ColumnType,colInfo.ColumnName)
		buffer.Write(ddlColumnInfoVoToByte(colInfo))
	}
	if ddlInfos.PreTableInfoList!=nil{
		buffer.Write(publicUtils.IntegerToBytes(len(ddlInfos.PreTableInfoList)))
		for _,preColInfo := range ddlInfos.PreTableInfoList{
			fmt.Printf(" preCol Type:%d ::name::%s\n",preColInfo.ColumnType,preColInfo.ColumnName)
			buffer.Write(ddlColumnInfoVoToByte(preColInfo))
		}
	}else{
		//preTableZeroArr := make([]byte,4)
		//preTableZeroArr[3]=0x00
		buffer.Write(make([]byte,4))
	}

	querySqlArr := make([]byte,1+len(ddlInfos.QuerySql))
	publicUtils.BlockByteArrCopy([]byte(ddlInfos.QuerySql),0,querySqlArr,0,len(ddlInfos.QuerySql))
	buffer.Write(querySqlArr)

	fmt.Printf(" DDL querySqlArr[]Arr %s \n",publicUtils.BytestoHex(querySqlArr))

	lengthArr := publicUtils.IntegerToBytes(len(buffer.Bytes()));
	sendBatchDDLArr.Write(lengthArr)
	sendBatchDDLArr.Write(verifyArr)
	sendBatchDDLArr.Write(buffer.Bytes())
	//fmt.Printf(" allColumnArrByRow[]Arr %s \n",publicUtils.BytestoHex(sendBatchDDLArr.Bytes()))


	return sendBatchDDLArr.Bytes()

}


func ddlColumnInfoVoToByte(columnInfo *vo.ColVo) []byte{

	colPos:=0;

	columnTypeArr := publicUtils.IntegerToBytes(columnInfo.ColumnType);

	thisColLen := len(columnInfo.ColumnName)+1+len(columnTypeArr);
	columnInfoArr := make([]byte,thisColLen)

	columnNameArr := make([]byte,1+len(columnInfo.ColumnName))
	publicUtils.BlockByteArrCopy([]byte(columnInfo.ColumnName),0,columnNameArr,0,len(columnInfo.ColumnName))

	publicUtils.BlockByteArrCopy([]byte(columnNameArr),0,columnInfoArr,colPos,len(columnNameArr))
	colPos = colPos+len(columnNameArr);
	publicUtils.BlockByteArrCopy([]byte(columnTypeArr),0,columnInfoArr,colPos,len(columnTypeArr))
	//fmt.Printf(" pos : %d  namArrLen : %s -> %d \n", colPos,columnInfo.columnNameArr,len(columnTypeArr))

	fmt.Printf(" DDL columnInfoArr[]Arr %s \n",publicUtils.BytestoHex(columnInfoArr))
	return columnInfoArr;
}




func columnInfoVoToByte(columnInfo *vo.ColumnVo) []byte{

	colPos:=0;
    //thisColLen := 4+len(columnInfo.ColumnName)+1+len(publicUtils.BytesToHexString(columnInfo.ColumnValue))+1+4
    thisColLen := 4+len(columnInfo.ColumnName)+1+len(columnInfo.ColumnValue)+5
	fmt.Printf(columnInfo.ColumnName," ColumnValue length %s \n",len(columnInfo.ColumnValue))
    columnInfoArr := make([]byte,thisColLen)


    //lengthArr := publicUtils.IntegerToBytes(thisColLen);
    columnNameArr := make([]byte,1+len(columnInfo.ColumnName))
    publicUtils.BlockByteArrCopy([]byte(columnInfo.ColumnName),0,columnNameArr,0,len(columnInfo.ColumnName))

    columnValueArr := columnInfo.ColumnValue

    //Create byte[] Array
    //publicUtils.BlockByteArrCopy([]byte(lengthArr),0,columnInfoArr,colPos,len(lengthArr))
    //colPos = colPos+len(lengthArr);
    if(columnInfo.IsPkFlag){
    	columnInfoArr[colPos]=0x31
    }else{
    	columnInfoArr[colPos]=0x30
    }
	colPos = colPos+1
    if(columnInfo.IsBinary==true){
		columnInfoArr[colPos]=0x31
	}else{
		columnInfoArr[colPos]=0x30
	}
    colPos = colPos+1

	if(columnInfo.IsNullFlag==true){
		columnInfoArr[colPos]=0x31
	}else{
		columnInfoArr[colPos]=0x30
	}
	colPos = colPos+1

    columnInfoArr[colPos]=columnInfo.ColumnType
    colPos = colPos+1
	columnInfoArr[colPos]=columnInfo.CFlag
	colPos = colPos+1
    publicUtils.BlockByteArrCopy([]byte(columnNameArr),0,columnInfoArr,colPos,len(columnNameArr))
	//fmt.Printf(" columnInfoArr[]Arr %s \n",publicUtils.BytestoHex(columnInfoArr))

	colPos = colPos+len(columnNameArr)

	publicUtils.BlockByteArrCopy(publicUtils.Int32ToBytes(columnInfo.ColumnLen),0,columnInfoArr,colPos,4)
	colPos = colPos+4

	//fmt.Printf("column info:::::",columnInfo.ColumnName," length:",columnInfo.ColumnLen," value:",columnInfo.ColumnValue," isBinary",columnInfo.IsBinary)
	//publicUtils.BlockByteArrCopy(publicUtils.Int32ToBytes(columnInfo.ColumnLen),0,columnInfoArr,colPos, int(columnInfo.ColumnLen))
	//fmt.Printf(string(columnInfo.ColumnLen)," columnValueArr[] %s \n",publicUtils.Int32ToBytes(columnInfo.ColumnLen))

	//colPos = colPos+4
	//fmt.Printf(" columnValueArr[] %s \n",publicUtils.BytestoHex(columnValueArr))

	publicUtils.BlockByteArrCopy(columnValueArr,0,columnInfoArr,colPos,len(columnValueArr))
    //fmt.Printf(" pos : %d  namArrLen : %s -> %d \n", colPos,columnInfo.ColumnValue,len(ColumnValueArr))

	//fmt.Printf(" columnInfoArr[]Arr %s \n",publicUtils.BytestoHex(columnInfoArr))
	return columnInfoArr;
}


//处理连接
func handleConnection(conn net.Conn) {

	buffer := make([]byte,2048)


	fmt.Printf("Operation client %s \n", conn.RemoteAddr().String())
	for{

		n,err := conn.Read(buffer)

		if err != nil{
			Log(conn.RemoteAddr().String()," connection error : ",err)
			return
		}
		// 将 byte 装换为 16进制的字符串
	    hex_string_data := publicUtils.BytestoHex(buffer[:n])
		Log(conn.RemoteAddr().String()," recveive data string :\n",hex_string_data)

	}

}


func Log(v ...interface{}) {
    log.Println(v...)
}

func JddmClientByCheckPoint(host string,resolvedTs uint64) (uint64, error){

	_, ok := ConnMap [host]
	if(!ok){
		tempConn, err := net.DialTimeout("tcp", host,timeout)
		//conn = tempConn
		ConnMap [host] = tempConn
		if err != nil {
			delete(ConnMap, host)
			//conn = nil
			fmt.Println("Error dialing", err.Error())
			return 0, nil
		}
	}

	//defer conn.Close()

	//fmt.Println("resolvedTs：：：：：：：：：：：：：：：：：：：：：：：：",resolvedTs)

	//=============================================
	if ConnMap [host]!=nil {
		ConnMap [host].SetWriteDeadline(time.Now().Add(timeout))
		_, err := ConnMap [host].Write(createBytesFromResolvedTs(resolvedTs))
		if err != nil {
			delete(ConnMap, host)
			return 0, nil
		}
	}



	//创建切片
	buf := make([]byte, 1024)

	//1 等待客户端通过conn发送信息
	//2 如果没有writer发送就一直阻塞在这
	if ConnMap [host]==nil{
		return 0, nil
	}
	ConnMap [host].SetReadDeadline(time.Now().Add(timeout))
	re, err := ConnMap [host].Read(buf)
	if err != nil {
		delete(ConnMap, host)
		fmt.Println("服务器read err=", err) //出错退出
		return 0, nil
	}
	//3. 显示读取内容到终端

	//fmt.Print("read:::::::::::::::",string(buf[:re]))
	_ = buf[:re]
	//fmt.Print("\nread:::::::::::::::",tt)
	//fmt.Print("\n")
	_ = buf[:4]
	//fmt.Print("\n",publicUtils.BytestoHex(tt1))
	result := buf[4:re]
	commitTs := publicUtils.BytesToLong(result)
	fmt.Print("\nCommitTs:::::::::",commitTs)

	return uint64(commitTs), err

}


func JddmClientFlush(host string,resolvedTs uint64) (uint64, error){


	_, ok := ConnMap [host]
	if(!ok){
		tempConn, err := net.DialTimeout("tcp", host,timeout)
		//conn = tempConn
		ConnMap [host] = tempConn
		if err != nil {
			delete(ConnMap, host)
			//conn = nil
			fmt.Println("Error dialing", err.Error())
			return 0, nil
		}
	}

	//defer conn.Close()

	//fmt.Println("resolvedTs：：：：：：：：：：：：：：：：：：：：：：：：",resolvedTs)

	//=============================================
	if ConnMap [host]==nil {
		fmt.Println("Error dialing")
		return 0, nil
	}

	ConnMap [host].SetWriteDeadline(time.Now().Add(timeout))
	_, err := ConnMap [host].Write(createFlushBytesFromResolvedTs(resolvedTs))
	if err != nil {
		delete(ConnMap, host)
		return 0, nil
	}


	// 遍历
	/*for e := l.Front(); e != nil; e = e.Next() {
		fmt.Printf("%v\n", e.Value)
	}*/

	//创建切片
	buf := make([]byte, 1024)

	//1 等待客户端通过conn发送信息
	//2 如果没有writer发送就一直阻塞在这
	ConnMap [host].SetReadDeadline(time.Now().Add(timeout))

	re, err := ConnMap [host].Read(buf)


	if err != nil {
		delete(ConnMap, host)
		fmt.Println("服务器read err=", err) //出错退出
		return 0, nil
	}
	//3. 显示读取内容到终端

	//fmt.Print("read:::::::::::::::",string(buf[:re]))
	_ = buf[:re]
	//fmt.Print("\nread:::::::::::::::",tt)
	//fmt.Print("\n")
	_ = buf[:4]
	//fmt.Print("\n",publicUtils.BytestoHex(tt1))
	result := buf[4:re]
	commitTs := publicUtils.BytesToLong(result)
	//fmt.Print("\nCommitTs:::::::::",commitTs)

	return uint64(commitTs), err

}

func createFlushBytesFromResolvedTs(resolvedTs uint64)  []byte{

	//var buffer bytes.Buffer
	//colsArr = make([]byte,0)
	//fmt.Printf(" resolvedTs   = %d\n", resolvedTs)


	verifyArr := make([]byte,4)
	//serviceNumArr := make([]byte,4)

	verifyArr[0] = 0x06
	verifyArr[1] = 0xce
	verifyArr[2] = 0x01
	//verifyArr[3] = 0x23
	verifyArr[3] = 0x15

	buffer := new(bytes.Buffer)   //直接使用 new 初始化，可以直接使用
	sendBatchRowsArr :=new(bytes.Buffer)

	//当前时间戳
	/*t1 := time.Now().Unix()  //1564552562
	fmt.Println(t1)*/

	//	buffer.Write(publicUtils.LongToBytes(rowInfo.StartTimer))
	buffer.Write(publicUtils.LongToBytes(int64(resolvedTs)))

	/*operTypeArr := make([]byte,4)
	operTypeArr[3]=byte('I')
	buffer.Write(operTypeArr)
	fmt.Printf(" allColumnArrByRow[%d]Arr %s \n",len(buffer.Bytes()),publicUtils.BytestoHex(buffer.Bytes()))*/


	lengthArr := publicUtils.IntegerToBytes(len(buffer.Bytes()));
	sendBatchRowsArr.Write(lengthArr)
	sendBatchRowsArr.Write(verifyArr)
	sendBatchRowsArr.Write(buffer.Bytes())
	//fmt.Printf(" allColumnArrByRow[]Arr %s \n",publicUtils.BytestoHex(sendBatchRowsArr.Bytes()))


	return sendBatchRowsArr.Bytes()

}

func createBytesFromResolvedTs(resolvedTs uint64)  []byte{

	//var buffer bytes.Buffer
	//colsArr = make([]byte,0)
	//fmt.Printf(" resolvedTs   = %d\n", resolvedTs)


	verifyArr := make([]byte,4)
	//serviceNumArr := make([]byte,4)

	verifyArr[0] = 0x06
	verifyArr[1] = 0xce
	verifyArr[2] = 0x01
	verifyArr[3] = 0x23
	//verifyArr[3] = 0x15

	buffer := new(bytes.Buffer)   //直接使用 new 初始化，可以直接使用
	sendBatchRowsArr :=new(bytes.Buffer)

	//当前时间戳
	/*t1 := time.Now().Unix()  //1564552562
	fmt.Println(t1)*/

	//	buffer.Write(publicUtils.LongToBytes(rowInfo.StartTimer))
	buffer.Write(publicUtils.LongToBytes(int64(resolvedTs)))

	/*operTypeArr := make([]byte,4)
	operTypeArr[3]=byte('I')
	buffer.Write(operTypeArr)
	fmt.Printf(" allColumnArrByRow[%d]Arr %s \n",len(buffer.Bytes()),publicUtils.BytestoHex(buffer.Bytes()))*/


	lengthArr := publicUtils.IntegerToBytes(len(buffer.Bytes()));
	sendBatchRowsArr.Write(lengthArr)
	sendBatchRowsArr.Write(verifyArr)
	sendBatchRowsArr.Write(buffer.Bytes())
	//fmt.Printf(" allColumnArrByRow[]Arr %s \n",publicUtils.BytestoHex(sendBatchRowsArr.Bytes()))


	return sendBatchRowsArr.Bytes()

}
