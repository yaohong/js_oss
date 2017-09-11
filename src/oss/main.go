package main
import (
	"net/http"
//	"log"
	"log"
	"fmt"
)


var gTableMgr *TableMgr = nil

func CreateTableMgr() {
	if gTableMgr == nil {
		gTableMgr = NewTableMgr()
	}
}

func main() {

	CreateTableMgr()
	err := gTableMgr.Init()
	if err != nil {
		log.Printf("tableMgr init failed, %v", err)
		return
	}

	err = gTableMgr.LoadAllTable()
	if err != nil {
		log.Printf("tableMgr LoadAllTable failed, %v", err)
		return
	}

	{
		fmt.Println("allDbTable")
		for k, _ := range gTableMgr.tables {
			fmt.Println(k)
		}
	}



	http.HandleFunc("/createTable", ossCreateTable)
	http.HandleFunc("/insertData", ossInsertData)
	http.HandleFunc("/insertData2", ossInsertData2)
	http.HandleFunc("/deleteTable", ossDeleteTable)
	http.HandleFunc("/gameCDK", useCdk)
	err = http.ListenAndServe(":8888", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}