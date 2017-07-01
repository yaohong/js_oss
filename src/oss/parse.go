package main
import (
	"simplejson"
	"errors"
	"strings"
	"fmt"
	"strconv"
	"reflect"
	"sort"
)


func NewError(formt string, args...interface{}) error {
	str := fmt.Sprintf(formt, args...)
	return errors.New(str)
}

type fieldTypeAttr struct {
	fType string
	fValue int32
}

type fieldAttr struct {
	fieldName string
	fieldType fieldTypeAttr
}

type tableMeta struct {
	app_id string
	name string
	filedArray []fieldAttr
	tableName string
}

func (self *tableMeta)TableName() string {
	if self.tableName == "" {
		self.tableName = fmt.Sprintf("%s_%s", self.app_id, self.name)
	}

	return self.tableName
}


//每个表格添加auto_id和created_time字段，其他字段由客户端传入

func generateTableMeta(tm *tableMeta, appId string, kvMap map[string]interface{}) error {

	tName, ok := kvMap["name"]
	if !ok {
		return NewError("name not exist")
	}

	var strName string
	if strName, ok = tName.(string); !ok {
		return NewError("name value type error, %v", reflect.TypeOf(tName))
	}

	delete(kvMap, "name")

	//以下2个字段不需要客户端传入
	delete(kvMap, "auto_id")
	delete(kvMap, "created_time")

	mapLen := len(kvMap)
	fieldArray := make([]fieldAttr, mapLen)
	keys := make([]string, 0, mapLen)
	for k, _ := range kvMap {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	index := 0
	for _, k := range keys {
		fieldArray[index].fieldName = k
		v := kvMap[k]
		if strV, ok := v.(string); ok {
			err := generateFieldTypeAttr(strV, &fieldArray[index].fieldType)
			if err != nil {
				return err
			}
			index++
		} else {
			return  NewError("error value type， %v", reflect.TypeOf(v))
		}

	}

	tm.app_id = appId
	tm.name = strName
	tm.filedArray = fieldArray
	tm.tableName = ""
	return nil
}


func generateAllTableMeta(buf []byte) ([]tableMeta, error) {
	js, err := simplejson.NewJson(buf)
	if err != nil {
		return nil, NewError("parse json failed, %v", err)
	}

	jsAppId := js.Get("app_id")
	intAppId, err := jsAppId.Int()
	if err != nil {
		return nil, NewError("parse appid failed, %v", err)
	}

	strAppId := strconv.FormatInt(int64(intAppId), 10)


	jsTable := js.Get("table")

	tableItems, err := jsTable.Array()
	if err != nil {
		return nil, NewError("parse table failed, %v", err)
	}

	len := len(tableItems)
	tableMeats := make([]tableMeta, len)
	for i:=0; i<len; i++ {
		tableItem := tableItems[i]
		if childKvMap, ok := tableItem.(map[string]interface{}); ok {
			err  := generateTableMeta(&tableMeats[i], strAppId, childKvMap)
			if err != nil {
				return nil, err
			}

		} else {
			return nil, NewError("error childKvMap Type, %v", reflect.TypeOf(tableItem))
		}
	}

	return tableMeats,nil
}


func generateFieldTypeAttr(rawStr string, ft *fieldTypeAttr) error {
	strItems := strings.Split(rawStr, ",")
	if len(strItems) != 2 {
		return NewError("fieldTypeAttr error, %s", rawStr)
	}

	fieldType := strItems[0]
	fieldValue := strItems[1]

	switch fieldType {
	case "int":
		if fieldValue != "32" && fieldValue != "64" {
			return NewError("fieldType error1, %v", rawStr)
		}
		v , _ := strconv.ParseInt(fieldValue, 10, 32)
		ft.fType = fieldType
		ft.fValue = int32(v)
	case "string":
		v , err := strconv.ParseInt(fieldValue, 10, 32)
		if err != nil {
			return NewError("fieldType error2, %v  %v", rawStr, err)
		}

		ft.fType = fieldType
		ft.fValue = int32(v)
//	case "data_time":
//		ft.fType =  fieldType
//		ft.fValue = 0
	default:
		return NewError("fieldType error3, %v", rawStr)
	}


	return nil
}