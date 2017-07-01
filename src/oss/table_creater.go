package main

import (
	_ "database/sql"
	_ "github.com/go-sql-driver/MySQL"
	"fmt"
	"database/sql"
	"strings"
	"log"
	"reflect"
	"encoding/json"
)


var (
	dbhostsip  = "127.0.0.1:3306"//IP地址
	dbusername = "root"//用户名
	dbpassword = "123456"//密码
	dbname     = "js_oss"//数据库名
)

type TableField struct {
	name string
	fType string
}

type Table struct {
	name string
	fields []TableField
}

func NewTable(name string) *Table {
	return &Table {
		name: name,
		fields : make([]TableField, 0, 10),
	}
}


func (self *Table)AddField(name string, tp string) {
	if name == "auto_id" || name == "created_time" {
		return
	}

	tf := TableField {
		name: name,
		fType: tp,
	}

	self.fields = append(self.fields, tf)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////


type TableMgr struct {
	connStr string
	tables map[string] *Table
	db *sql.DB
}


func NewTableMgr() *TableMgr {
	str := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8", dbusername, dbpassword, dbhostsip, dbname)
	return &TableMgr {
		connStr: str,
		tables: make(map[string] *Table, 100),
		db: nil,
	}
}

func (self *TableMgr)Init() error {
	db, err := sql.Open("mysql", self.connStr)
	if err != nil {
		return err
	}
	err = db.Ping()
	if err != nil {
		return err
	}

	self.db = db
	return nil
}





func (self *TableMgr)LoadAllTable() error {
	rows, err := self.db.Query("show tables")
	if err != nil {
		return err
	}

	defer rows.Close()

	tmpTableNames := make([]string,0 ,10)
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			return err
		}

		tmpTableNames = append(tmpTableNames, tableName)
	}

	rows.Close()

	tableCount := len(tmpTableNames)
	for i:=0; i<tableCount; i++ {
		tableName := tmpTableNames[i]
		newTable := NewTable(tableName)
		sqlStr := fmt.Sprintf("select column_name, data_type from Information_schema.columns where table_schema='%s' and table_name='%s'", dbname, tableName)
		rows2 ,err := self.db.Query(sqlStr)
		if err != nil {
			return err
		}
		defer rows2.Close()

		log.Printf("table:%s\n", tableName)
		for rows2.Next() {
			var columnName, columnType string
			err = rows2.Scan(&columnName, &columnType)
			if err != nil {
				return err
			}
			log.Printf("	columnName:%s columnType:%s\n", columnName, columnType)
			newTable.AddField(columnName, columnType)
		}
		rows2.Close()
		self.tables[tableName] = newTable
	}

	return nil
}

func (self *TableMgr)AddTable(tm *tableMeta) error {
	_, ok := self.tables[tm.TableName()]
	if ok {
		return nil
	}

	fieldLen := len(tm.filedArray)
	newTable := NewTable(tm.TableName())
	sqlFieldItems := make([]string, fieldLen)
	for i:=0; i<fieldLen; i++ {
		createTableStrItem, err := formatFieldToCreateTableStr(&tm.filedArray[i])
		if err != nil {
			return err
		}

		mysqlFieldType, _ := transformMysqlFieldType(&tm.filedArray[i])
		newTable.AddField(tm.filedArray[i].fieldName, mysqlFieldType)

		sqlFieldItems[i] = createTableStrItem
	}

	tableFieldAttrStr := strings.Join(sqlFieldItems, ",")

	createTableSql := fmt.Sprintf("create table %s(auto_id bigint not null AUTO_INCREMENT, %s, created_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`auto_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8", tm.TableName(), tableFieldAttrStr)

	fmt.Println(createTableSql)
	rows, err := self.db.Query(createTableSql)
	if err != nil {
		return err
	}
	defer rows.Close()

	if rows.Err() != nil {
		return rows.Err()
	}

	self.tables[tm.TableName()] = newTable
	log.Printf("create_table[%s] success, field %v\n", newTable.name, newTable.fields)
	return nil
}

func (self *TableMgr)DeleteTable(tableName string) error {
	_, ok := self.tables[tableName]
	if !ok {
		return nil
	}

	deleteTableSql := fmt.Sprintf("drop table %s", tableName)
	fmt.Println(deleteTableSql)

	rows, err := self.db.Query(deleteTableSql)
	if err != nil {
		return err
	}

	defer rows.Close()

	if rows.Err() != nil {
		return rows.Err()
	}


	delete(self.tables, tableName)

	return nil
}


func (self *TableMgr)InsertTableData(tableName string, tableField map[string]interface{}) error {
	table, ok := self.tables[tableName]
	if !ok {
		return nil
	}

	fieldsLen := len(table.fields)
	log.Printf("table[%s] fieldLen[%d] %v", tableName, len(table.fields), table.fields)
	insertFieldItems := make([]string, fieldsLen)
	insertValueItems := make([]string, fieldsLen)
	index := 0
	for _, field := range table.fields {

		filedValue, ok := tableField[field.name]
		if !ok {
			return NewError("field[%s] not exist", field.name)
		}

		formatFieldValue, err := formatFieldValueToInsertTableStr(field.name, field.fType, filedValue)
		if err != nil {
			return err
		}

		insertFieldItems[index] = field.name
		insertValueItems[index] = formatFieldValue
		index++
	}

	fieldStr := strings.Join(insertFieldItems, ",")
	valueStr := strings.Join(insertValueItems, ",")


	insertSqlStr := fmt.Sprintf("insert into %s(%s) values(%s)", tableName, fieldStr, valueStr)
	fmt.Println(insertSqlStr)
	rows, err := self.db.Query(insertSqlStr)
	if err != nil {
		return err
	}

	defer rows.Close()
	if rows.Err() != nil {
		return rows.Err()
	}

	return nil
}


func transformMysqlFieldType(fa *fieldAttr) (string, error) {
	switch fa.fieldType.fType {
	case "int":
		switch fa.fieldType.fValue {
		case 32:
			return "int", nil
		case 64:
			return "bigint", nil
		default:
			return "", NewError("unkown errorFieldTypeValue[%d] fieldType[%s].", fa.fieldType.fValue, fa.fieldType.fType)
		}
	case "string":
		return "varchar", nil
	//	case "data_time":
	//		return fmt.Sprintf("%s timestamp not null default CURRENT_TIMESTAMP", fa.fieldName), nil
	default:
		return "", NewError("unkown fieldType[%s] fieldTypeValue[%s].", fa.fieldType.fType, fa.fieldType.fValue)
	}
}


func formatFieldToCreateTableStr(fa *fieldAttr) (string, error) {
	switch fa.fieldType.fType {
	case "int":
		switch fa.fieldType.fValue {
		case 32:
			return fmt.Sprintf("%s int", fa.fieldName), nil
		case 64:
			return fmt.Sprintf("%s bigint", fa.fieldName), nil
		default:
			return "", NewError("unkown errorFieldTypeValue[%d] fieldType[%s].", fa.fieldType.fValue, fa.fieldType.fType)
		}
	case "string":
		return fmt.Sprintf("%s varchar(%d)", fa.fieldName, fa.fieldType.fValue), nil
//	case "data_time":
//		return fmt.Sprintf("%s timestamp not null default CURRENT_TIMESTAMP", fa.fieldName), nil
	default:
		return "", NewError("unkown fieldType[%s] fieldTypeValue[%s].", fa.fieldType.fType, fa.fieldType.fValue)
	}
}

func formatFieldValueToInsertTableStr(fieldName string, fieldType string, value interface{}) (string, error) {
	switch fieldType {
	case "int", "bigint":
		var intValue int
		switch value.(type) {
		case json.Number:
			i, err := value.(json.Number).Int64()
			if err != nil {
				return "", err
			}
			intValue = int(i)
		case int:
			intValue = int(reflect.ValueOf(value).Int())
		default:
			return "", NewError("error fieldName[%s] fieldValueType[%s], int or bigint.", fieldName, reflect.TypeOf(value))
		}
		return fmt.Sprintf("%d", intValue), nil
	case "varchar":
		strValue, ok := value.(string)
		if !ok {
			return "", NewError("error fieldName[%s] fieldValueType[%s], string.", fieldName, reflect.TypeOf(value))
		}
		return fmt.Sprintf("'%s'", strValue), nil
	default:
		return "", NewError("unkown fieldType[%s].", fieldType)
	}
}
