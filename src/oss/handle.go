package main
import (
	"net/http"
	"fmt"
	"io/ioutil"
	"log"
	"simplejson"
	"strconv"
	"reflect"
	"net/url"
	"strings"
)



func NewRspByte(format string, args... interface{}) []byte {
	str := fmt.Sprintf(format, args...)
	return []byte(str)
}


func ossCreateTable(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("create_table\n")

	if r.Method != "POST" {
		w.Write(NewRspByte("error http Method, %v", r.Method))
		return
	}
	body, _ := ioutil.ReadAll(r.Body)

	allTableMeta , err := generateAllTableMeta(body)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	for _, v := range allTableMeta {
		err = gTableMgr.AddTable(&v)
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}
	}

	w.Write([]byte("create table success."))
}



func getValue(key string, kv map[string]interface{}) interface{} {
	v, ok := kv[key]
	if !ok {
		panic(NewError("key[%s] not exists, %v", key, kv))
	}

	return v
}

func getValueToString(key string, kv map[string]interface{}) string {
	v := getValue(key, kv)
	if strValue, ok := v.(string); ok {
		return strValue
	}

	panic(NewError("key[%s] value[%v] type error, %v", key, reflect.TypeOf(v), kv))
}

func getValueToMap(key string, kv map[string]interface{}) map[string]interface{} {
	v := getValue(key, kv)

	if mapValue, ok := v.(map[string]interface{}); ok {
		return mapValue
	}

	panic(NewError("key[%s] value[%v] type error, %v", key, reflect.TypeOf(v), kv))
}

func ossInsertData(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if re := recover(); re != nil {
			if err, ok := re.(error);  ok {
				log.Printf("insertData error, %s\n", err.Error())
				w.Write(NewRspByte("insertData error, %s", err.Error()))
			} else {
				log.Printf("insertData error, %v\n", re)
				w.Write(NewRspByte("insertData exception %v", re))
			}

		}
	}()

	fmt.Printf("InsertData\n")
	if r.Method != "POST" {
		panic(NewError("error http Method, %v", r.Method))
	}

	queryForm, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		panic(NewError("ParseForm failed, %v", err.Error()))
	}

	AppId := queryForm.Get("app_id")
	if AppId == "" {
		panic(NewError("get [app_id] failed"))
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err.Error())
		panic(NewError("read body failed, %v", err.Error()))
	}

	log.Printf("body=%s\n", string(body))



	js, err := simplejson.NewJson(body)
	if err != nil {
		log.Println(err.Error())
		panic(NewError("parse json failed, %v", err.Error()))
	}

	insertDataArray, err := js.Array()
	if err != nil {
		panic(NewError("json format error, %v", err.Error()))
	}
	fmt.Println(insertDataArray)
	insertDataLen := len(insertDataArray)
	insertDataMap := make(map[string] map[string] interface{}, insertDataLen)


	for _, insertDataItem := range insertDataArray {
		if insertKvMap, ok := insertDataItem.(map[string]interface{}); ok {
			TableName := getValueToString("tab_name", insertKvMap)

			UserId := getValue("user_id", insertKvMap)
			DeviceId := getValue("device_id", insertKvMap)
			TabVer := getValue("tab_ver", insertKvMap)

			insertSqlKVMap := make(map[string]interface{})
			insertSqlKVMap["user_id"] = UserId
			insertSqlKVMap["device_id"] = DeviceId
			insertSqlKVMap["tab_ver"] = TabVer
			TableDataMap := getValueToMap("tab_data", insertKvMap)
			for tdKey, tdValue  := range TableDataMap {
				insertSqlKVMap[tdKey] = tdValue
			}

			sqlTableName := fmt.Sprintf("%s_%s", AppId, TableName)

			insertDataMap[sqlTableName] = insertSqlKVMap
		} else {
			panic(NewError("error deleteKvMap Type, %v", reflect.TypeOf(insertDataItem)))
		}
	}

	for insertTableName, insertTableFiledMap := range insertDataMap {
		err = gTableMgr.InsertTableData(insertTableName, insertTableFiledMap)
		if err != nil {
			panic(err)
		}
	}

	w.Write([]byte("insertData success."))

}

func ossInsertData2(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if re := recover(); re != nil {
			if err, ok := re.(error);  ok {
				log.Printf("insertData2 error, %s\n", err.Error())
				w.Write(NewRspByte("insertData2 error, %s", err.Error()))
			} else {
				log.Printf("insertData2 error, %v\n", re)
				w.Write(NewRspByte("insertData2 exception %v", re))
			}

		}
	}()

	fmt.Printf("InsertData2\n")
	if r.Method != "POST" {
		panic(NewError("error http Method, %v", r.Method))
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err.Error())
		panic(NewError("read body failed, %v", err.Error()))
	}


	if len(body) < 24 {
		panic(NewError("body length exception, %s", string(body)))
	}

	UrlHead := body[:24]
	StrUrlHead := string(UrlHead)

	UrlValueItems := strings.Split(StrUrlHead, "&")
	if len(UrlValueItems) != 2 {
		panic(NewError("StrUrlHead exception, %s",StrUrlHead))
	}

	AppItem := strings.Split(UrlValueItems[0], "=")
	if len(AppItem) != 2 {
		panic(NewError("AppItem exception, %s",UrlValueItems[0]))
	}

	if AppItem[0] != "app_id" {
		panic(NewError("Appid exception, %s", AppItem[0]))
	}

	StrAppId := AppItem[1]

	Json := body[24:]
	js, err := simplejson.NewJson([]byte(Json))
	if err != nil {
		log.Println(err.Error())
		panic(NewError("parse json failed, %v", err.Error()))
	}

	insertDataArray, err := js.Array()
	if err != nil {
		panic(NewError("json format error, %v", err.Error()))
	}
	fmt.Println(insertDataArray)
	insertDataLen := len(insertDataArray)
	insertDataMap := make(map[string] map[string] interface{}, insertDataLen)


	for _, insertDataItem := range insertDataArray {
		if insertKvMap, ok := insertDataItem.(map[string]interface{}); ok {
			TableName := getValueToString("tab_name", insertKvMap)

			UserId := getValue("user_id", insertKvMap)
			DeviceId := getValue("device_id", insertKvMap)
			TabVer := getValue("tab_ver", insertKvMap)

			insertSqlKVMap := make(map[string]interface{})
			insertSqlKVMap["user_id"] = UserId
			insertSqlKVMap["device_id"] = DeviceId
			insertSqlKVMap["tab_ver"] = TabVer
			TableDataMap := getValueToMap("tab_data", insertKvMap)
			for tdKey, tdValue  := range TableDataMap {
				insertSqlKVMap[tdKey] = tdValue
			}

			sqlTableName := fmt.Sprintf("%s_%s", StrAppId, TableName)

			insertDataMap[sqlTableName] = insertSqlKVMap
		} else {
			panic(NewError("error deleteKvMap Type, %v", reflect.TypeOf(insertDataItem)))
		}
	}

	for insertTableName, insertTableFiledMap := range insertDataMap {
		err = gTableMgr.InsertTableData(insertTableName, insertTableFiledMap)
		if err != nil {
			panic(err)
		}
	}

	w.Write([]byte("insertData success."))

}


func ossDeleteTable(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if re := recover(); re != nil {
			if err, ok := re.(error);  ok {
				w.Write(NewRspByte("deleteTable error, %s", err.Error()))
			} else {
				log.Printf("deleteTable error, %v\n", re)
				w.Write(NewRspByte("deleteTable exception %v", re))
			}

		}
	}()
	fmt.Printf("deleteTable\n")
	if r.Method != "POST" {
		w.Write(NewRspByte("error http Method, %v", r.Method))
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err.Error())
		w.Write(NewRspByte("read body failed, %v", err.Error()))
		return
	}

	js, err := simplejson.NewJson(body)
	if err != nil {
		log.Println(err.Error())
		w.Write(NewRspByte("parse json failed, %v", err.Error()))
		return
	}

	jsAppId := js.Get("app_id")
	intAppId, err := jsAppId.Int()
	if err != nil {
		log.Println(err.Error())
		w.Write(NewRspByte("parse appid failed, %v", err.Error()))
		return
	}

	strAppId := strconv.FormatInt(int64(intAppId), 10)

	jsTable := js.Get("table")

	deleteItems, err := jsTable.Array()
	if err != nil {
		w.Write(NewRspByte("parse table failed, %v", err.Error()))
		return
	}

	deleteLen := len(deleteItems)
	deleteTableNames := make([]string, deleteLen)
	for index, deleteItem := range deleteItems {
		if deleteKvMap, ok := deleteItem.(map[string]interface{}); ok {
			//只能有一个字段，name
			deleteKvMapLen := len(deleteKvMap)
			if deleteKvMapLen != 1 {
				w.Write(NewRspByte("error deleteKvMap len error, %v", deleteKvMapLen))
				return
			}
			strDeleteName := getValueToString("name", deleteKvMap)

			deleteTableNames[index] = fmt.Sprintf("%s_%s", strAppId, strDeleteName)
		} else {
			w.Write(NewRspByte("error deleteKvMap Type, %v", reflect.TypeOf(deleteItem)))
			return
		}
	}


	for _, deleteTableName := range deleteTableNames {
		err = gTableMgr.DeleteTable(deleteTableName)
		if err != nil {
			w.Write(NewRspByte("deleteTable[%s] failed, %v", deleteTableName, err))
			return
		}
	}


	w.Write([]byte("delete table success."))
}

