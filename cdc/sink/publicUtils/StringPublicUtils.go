
/*** *** *** *** *** *** ***
 @Title  StringPublicUtils.go -> package publicUtils
 @Description  define Strings/ byte/ byte[]/int Operation
 @Author  JiangHao  2021/04/23 22:56
 @Update  JiangHao  2021/04/23 22:56
*/
package publicUtils

import (
	"fmt"
	"bytes"
	"strconv"
    "encoding/binary"
    "errors"
    "container/list"
)

/*** *** *** *** *** *** ***
@author:JiangHao
@Description: int64 to Byte[8]
@Date:2021-09-01 07:33 PM
***/
func LongToBytes(i int64) []byte {
    var int64Arr = make([]byte, 8)
    binary.BigEndian.PutUint64(int64Arr, uint64(i))
    return int64Arr
}

/*** *** *** *** *** *** ***
@author:JiangHao
@Description: Byte[8] to int64
@Date:2021-09-01 07:33 PM
***/
func BytesToLong(int64Arr []byte) int64 {
    return int64(binary.BigEndian.Uint64(int64Arr))
}

/*** *** *** *** *** *** ***
@author:JiangHao
@Description: int to Byte[4]
@Date:2021-04-23 19:33
***/
func IntegerToBytes(n int) []byte {
  x := int32(n)
  bytesBuffer := bytes.NewBuffer([]byte{})
  binary.Write(bytesBuffer, binary.BigEndian, x)
  return bytesBuffer.Bytes()
}

// int to 4 bytes
func intTo4Bytes(i int) []byte {
	buf := bytes.NewBuffer([]byte{})
	tmp := uint32(i)
	binary.Write(buf, binary.BigEndian, tmp)
	return buf.Bytes()
}

// int to 2 bytes
func IntTo2Bytes(i int) []byte {
	buf := bytes.NewBuffer([]byte{})
	tmp := uint16(i)
	binary.Write(buf, binary.BigEndian, tmp)
	return buf.Bytes()
}

/*** *** *** *** *** *** ***
@title :   BytesToInteger(b []byte) int
@Description: Byte[4] to int
@author:JiangHao
@Date:2021-04-23 19:33
***/
func BytesToInteger(b []byte) int {
  bytesBuffer := bytes.NewBuffer(b)

  var x int32
  binary.Read(bytesBuffer, binary.BigEndian, &x)

  return int(x)
}

// bytes to int 32
func bytesTo32Int(b []byte) int {
	buf := bytes.NewBuffer(b)
	var tmp uint32
	binary.Read(buf, binary.BigEndian, &tmp)
	return int(tmp)
}
// bytes to int 16
func bytesTo16Int(b []byte) int {
	buf := bytes.NewBuffer(b)
	var tmp uint16
	binary.Read(buf, binary.BigEndian, &tmp)
	return int(tmp)
}


/*** *** *** *** *** *** ***
@title :  ShortIntegerToBytes(b []byte) int
@Description: src byte[] copy to Dest byte[]
@author:JiangHao
@param     b            []byte       " two byte []"
@return                 int
@Date:2021-04-25 17:35
***/
func ShortIntegerToBytes(b []byte) int{

	outArr := make([]byte,4)

	outArr[2]=b[0]
	outArr[3]=b[1]
	var x int32
	binary.Read(bytes.NewBuffer(outArr), binary.BigEndian, &x)

	return int(x)

}


/*** *** *** *** *** *** ***
@title :  BlockByteArrCopy(src []byte, srcOffset int, dst []byte, dstOffset, count int) (bool, error)
@Description: src byte[] copy to Dest byte[]
@author:JiangHao
@param     src          []byte       "Source Byte[]"
@param     srcOffset    int          "source byte[] length"
@param     dst          []byte       "Target byte[]"
@param     dstOffset    int          "Target byte[] offset Point"
@param     count        int
@return                 bool
@return                 error
@Date:2021-04-23 19:33
***/
func BlockByteArrCopy(src []byte, srcOffset int, dst []byte, dstOffset, count int) (bool, error) {

    srcLen := len(src)
    if srcOffset > srcLen || count > srcLen || srcOffset+count > srcLen {
        return false, errors.New(" Source buffer Index out of range ! ")
    }
    dstLen := len(dst)
    if dstOffset > dstLen || count > dstLen || dstOffset+count > dstLen {
        return false, errors.New(" Target Buffer Index out of range ! ")
    }
    index := 0
    for i := srcOffset; i < srcOffset+count; i++ {
        dst[dstOffset+index] = src[srcOffset+index]
        index++
    }
    return true, nil
}

/*** *** *** *** *** *** ***
@title :  QueryByteArray(src []byte) interface{}
@Description: input byte Array split By 0x00
@author:JiangHao
@param     src          []byte       "Source Byte[]"
@return                 interface{}  "list.New()"
@Date:2021-04-25 19:33
***/
func QueryByteArray(src []byte) interface{} {
	var posLen int =0

	returnList := list.New() //创建一个新的list
	for i:=0;i<len(src); {

		testArr,reLen :=GetEndArrByHex(src,posLen)
		if testArr[0] != 0x00  {
			fmt.Printf("------> %s %s (%d) \n",BytestoHex(testArr), string(testArr),len(testArr))
			returnList.PushBack(testArr)
		}

		posLen = i+reLen
		i=posLen
	}

	return returnList
}

/*** *** *** *** *** *** *** *** ***
@title :  GetEndArrByHex(src []byte, srcOffset int)([]byte)
@Description: Read bit by bit from the specified position of the input byte array until it is equal to 0x00(byte)
@author:JiangHao
@param     src          []byte       "Source Byte[]"
@param     srcOffset    int          "source byte[] length"
@return                 []byte
@Date:2021-04-25 21:33
***/
func GetEndArrByHex(src []byte, srcOffset int)([]byte,int){

	index := 0
	arrLen := srcOffset
	for i:=srcOffset;i<len(src);i++ {

		if src[arrLen] == 0x00{
			break
		}
		index++
		arrLen++
	}
	if index == 0 {
		index++
		allCountArr := make([]byte,index)
		return allCountArr,index
	}else{
		//index++
		allCountArr := make([]byte,index)
		BlockByteArrCopy(src,srcOffset,allCountArr,0,index)
		return allCountArr,(index+1)
	}

}


/*** *** *** *** *** *** *** *** *** *** *** *** *** ***
@title :  BytestoHex
@Description : 字节数组转16进制可以直接使用 用fmt就能转换
@author : JiangHao
@param     b      []byte       "输入的二进制数组"
@return    H      string       "十六进制的字符串"
@Date : 2021-04-23 22:13
***/
func BytestoHex(b []byte)(H string){
	H=fmt.Sprintf("%x",b)
	return;
}

// bytes to hex string
func bytesToHexString(b []byte) string {
	var buf bytes.Buffer
	for _, v := range b {
		t := strconv.FormatInt(int64(v), 16)
		if len(t) > 1 {
			buf.WriteString(t)
		} else {
			buf.WriteString("0" + t)
		}
	}
	return buf.String()
}

/*** *** *** *** *** *** *** *** *** *** *** *** *** ***
@title :  BytestoHex
@Description : hex string change to byte[]
@author : JiangHao
@param    str      string       "十六进制的字符串"
@return     b      []byte       "输入的二进制数组"
@Date : 2021-04-23 23:05
***/
func HextoBytes(str string)([]byte){
	slen:=len(str)
	bHex:=make([]byte,len(str)/2)
	ii:=0
	for i:=0;i<len(str);i=i+2 {
		if slen!=1{
			ss:=string(str[i])+string(str[i+1])
			bt,_:=strconv.ParseInt(ss,16,32)
			bHex[ii]=byte(bt)
			ii=ii+1;
			slen=slen-2;}
	}
	return bHex;
}

// hex string to bytes
func hexStringToBytes(s string) []byte {
	bs := make([]byte, 0)
	for i := 0; i < len(s); i = i + 2 {
		b, _ := strconv.ParseInt(s[i:i+2], 16, 16)
		bs = append(bs, byte(b))
	}
	return bs
}

//二进制字符串转byte
//a, err := strconv.ParseInt("11111111", 2, 16)
