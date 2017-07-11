package main
import (
//	"net/http"
	"fmt"
	"os"
	_ "database/sql"
	_ "github.com/go-sql-driver/MySQL"
	"database/sql"
	"time"
	"strings"
	"errors"
)

const (
	dbhostsip  = "127.0.0.1:3306"//IP地址
	dbusername = "root"//用户名
	dbpassword = "123456"//密码
	dbname     = "js_oss"//数据库名
)

func main() {
	fmt.Println(time.Now().Format("dasdads#2006-01-02"))
	//	http.HandleFunc("/inquiry", inquiry)
	//
	//	err := http.ListenAndServe(":8889", nil)
	//	if err != nil {
	//		log.Fatal("ListenAndServe: ", err)
	//	}

	if len(os.Args) < 4 {
		fmt.Println("args error")
		return
	}
	var err error
	switch os.Args[1] {
	case "missionNormalFailed":
		err = handle_missionNormalFailed(os.Args[2], os.Args[3])
	case "missionNormalCount":
		err = handle_missionNormalCount(os.Args[2], os.Args[3])
	case "missionEliteFailed":
		err = handle_missionEliteFailed(os.Args[2], os.Args[3])
	case "missionEliteCount":
		err = handle_missionEliteCount(os.Args[2], os.Args[3])
	case "missionTowerMax":
		err = handle_missionTowerMax(os.Args[2], os.Args[3])
	case "missionTowerCount":
		err = handle_missionTowerCount(os.Args[2], os.Args[3])
	case "missionSeasonMax":
		err = handle_missionSeasonMax(os.Args[2], os.Args[3])
	case "missionSeasonCount":
		err = handle_missionSeasonCount(os.Args[2], os.Args[3])
	case "missionTowerInsprite":
		err = handle_missionTowerInsprite(os.Args[2], os.Args[3])
	case "missionTowerTimes":
		err = handle_missionTowerTimes(os.Args[2], os.Args[3])
	case "missionChest":
		err = handle_missionChest(os.Args[2], os.Args[3])
	default:
		fmt.Printf("cmd %s error.\n", os.Args[1])
		return
	}

	if err != nil {
		fmt.Println(err)
	}

}


func CreateDbConnect() (db *sql.DB, err error) {
	str := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8", dbusername, dbpassword, dbhostsip, dbname)
	tmpDb, err := sql.Open("mysql", str)
	if err != nil {
		return nil, err
	}
	err = tmpDb.Ping()
	if err != nil {
		return nil, err
	}

	return tmpDb, nil
}

func handle_missionNormalFailed(startTime string ,endTime string) error {
	fmt.Println("handle_missionNormalFailed")
	db, err := CreateDbConnect()
	if err != nil {
		fmt.Println("connect failed.")
	}

	offset := 0
	step := 5000
	datas := make(map[string]int, 1000)
	for {
		sqlStr := fmt.Sprintf("select id from 137_mission_normal where created_time > '%s' and created_time < '%s' and re=-1 limit %d, %d", startTime, endTime,  offset, step)
		rows ,err := db.Query(sqlStr)
		if err != nil {
			fmt.Println(err)
			break
		}

		num := 0
		for rows.Next() {
			var id string
			err = rows.Scan(&id)
			if err != nil {
				return err
			}

			OldCount, ok := datas[id]
			if ok {
				datas[id] = OldCount + 1
			} else {
				datas[id] = 1
			}


			num++
		}
		rows.Close()

		if num < step {
			break
		}

		offset = offset + step
	}

	fileName := time.Now().Format("missionNormalFailed#2006-01-02_15:04:05.csv")
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}

	for k, v:= range datas {
		fmt.Fprintf(f, "%s,%d\n", k, v)
	}

	f.Close()
	return nil
}

func handle_missionNormalCount(startTime string ,endTime string) error {
	fmt.Println("handle_missionNormalCount")
	db, err := CreateDbConnect()
	if err != nil {
		fmt.Println("connect failed.")
	}

	offset := 0
	step := 5000
	datas := make(map[string] map[string]int, 10)
	for {
		sqlStr := fmt.Sprintf("select id, created_time from 137_mission_normal where created_time > '%s' and created_time < '%s' limit %d, %d", startTime, endTime,  offset, step)
		rows ,err := db.Query(sqlStr)
		if err != nil {
			fmt.Println(err)
			break
		}

		num := 0
		for rows.Next() {
			var id, createTime string

			err = rows.Scan(&id, &createTime)
			if err != nil {
				return err
			}

			timeValue := strings.Split(createTime, " ")
			if len(timeValue) != 2 {
				return errors.New("createTime value error," + createTime)
			}

			childMap, ok := datas[timeValue[0]]
			if ok {
				OldCount, ok1 := childMap[id]
				if ok1 {
					childMap[id] = OldCount + 1
				} else {
					childMap[id] = 1
				}
			} else {
				childMap = make(map[string]int, 500)
				childMap[id] = 1
				datas[timeValue[0]] = childMap
			}

			num++
		}
		rows.Close()

		if num < step {
			break
		}

		offset = offset + step
	}

	fileName := time.Now().Format("missionNormalCount#2006-01-02_15:04:05.csv")
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}

	for k, v:= range datas {
		for k1, v1 := range v {
			fmt.Fprintf(f, "%s,%s,%d\n", k, k1, v1)
		}
	}

	f.Close()
	return nil
}

func handle_missionEliteFailed(startTime string ,endTime string) error {
	fmt.Println("handle_missionEliteFailed")
	db, err := CreateDbConnect()
	if err != nil {
		fmt.Println("connect failed.")
	}

	offset := 0
	step := 5000
	datas := make(map[string]int, 1000)
	for {
		sqlStr := fmt.Sprintf("select id from 137_mission_elite where created_time > '%s' and created_time < '%s' and re=-1 limit %d, %d", startTime, endTime,  offset, step)
		rows ,err := db.Query(sqlStr)
		if err != nil {
			fmt.Println(err)
			break
		}

		num := 0
		for rows.Next() {
			var id string
			err = rows.Scan(&id)
			if err != nil {
				return err
			}

			OldCount, ok := datas[id]
			if ok {
				datas[id] = OldCount + 1
			} else {
				datas[id] = 1
			}


			num++
		}
		rows.Close()

		if num < step {
			break
		}

		offset = offset + step
	}

	fileName := time.Now().Format("missionEliteFailed#2006-01-02_15:04:05.csv")
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}

	for k, v:= range datas {
		fmt.Fprintf(f, "%s,%d\n", k, v)
	}

	f.Close()
	return nil
}

func handle_missionEliteCount(startTime string ,endTime string) error {
	fmt.Println("handle_missionEliteCount")
	db, err := CreateDbConnect()
	if err != nil {
		fmt.Println("connect failed.")
	}

	offset := 0
	step := 5000
	datas := make(map[string] map[string]int, 10)
	for {
		sqlStr := fmt.Sprintf("select id, created_time from 137_mission_elite where created_time > '%s' and created_time < '%s' limit %d, %d", startTime, endTime,  offset, step)
		rows ,err := db.Query(sqlStr)
		if err != nil {
			fmt.Println(err)
			break
		}

		num := 0
		for rows.Next() {
			var id, createTime string

			err = rows.Scan(&id, &createTime)
			if err != nil {
				return err
			}

			timeValue := strings.Split(createTime, " ")
			if len(timeValue) != 2 {
				return errors.New("createTime value error," + createTime)
			}

			childMap, ok := datas[timeValue[0]]
			if ok {
				OldCount, ok1 := childMap[id]
				if ok1 {
					childMap[id] = OldCount + 1
				} else {
					childMap[id] = 1
				}
			} else {
				childMap = make(map[string]int, 500)
				childMap[id] = 1
				datas[timeValue[0]] = childMap
			}

			num++
		}
		rows.Close()

		if num < step {
			break
		}

		offset = offset + step
	}

	fileName := time.Now().Format("missionEliteCount#2006-01-02_15:04:05.csv")
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}

	for k, v:= range datas {
		for k1, v1 := range v {
			fmt.Fprintf(f, "%s,%s,%d\n", k, k1, v1)
		}
	}

	f.Close()
	return nil
}


func handle_missionTowerMax(startTime string ,endTime string) error {

	return nil
}

func handle_missionTowerCount(startTime string ,endTime string) error {

	return nil
}

func handle_missionSeasonMax(startTime string ,endTime string) error {

	return nil
}

func handle_missionSeasonCount(startTime string ,endTime string) error {

	return nil
}

func handle_missionTowerInsprite(startTime string ,endTime string) error {
	fmt.Println("handle_missionTowerInsprite")
	db, err := CreateDbConnect()
	if err != nil {
		fmt.Println("connect failed.")
	}

	offset := 0
	step := 5000
	fileName := time.Now().Format("missionTowerInsprite#2006-01-02_15:04:05.csv")
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	for {
		sqlStr := fmt.Sprintf("select auto_id,device_id,time,user_id,created_time from 137_mission_tower_insprite where created_time > '%s' and created_time < '%s' limit %d, %d", startTime, endTime,  offset, step)
		rows ,err := db.Query(sqlStr)
		if err != nil {
			fmt.Println(err)
			break
		}

		num := 0
		for rows.Next() {
			var device_id,user_id,created_time string
			var id,time1 int
			err = rows.Scan(&id, &device_id, &time1, &user_id, &created_time)
			if err != nil {
				return err
			}
			fmt.Fprintf(f, "%d,%s,%d,%s,%s\n", id, device_id, time1, user_id, created_time)

			num++
		}
		rows.Close()

		if num < step {
			break
		}

		offset = offset + step
	}

	f.Close()
	return nil
}

func handle_missionTowerTimes(startTime string ,endTime string) error {
	fmt.Println("handle_missionTowerTimes")
	db, err := CreateDbConnect()
	if err != nil {
		fmt.Println("connect failed.")
	}

	offset := 0
	step := 5000
	fileName := time.Now().Format("missionTowerTimes#2006-01-02_15:04:05.csv")
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	for {
		sqlStr := fmt.Sprintf("select auto_id,device_id,mode,time,user_id,created_time from 137_mission_tower_times where created_time > '%s' and created_time < '%s' limit %d, %d", startTime, endTime,  offset, step)
		rows ,err := db.Query(sqlStr)
		if err != nil {
			fmt.Println(err)
			break
		}

		num := 0
		for rows.Next() {
			var device_id,user_id,created_time string
			var mode, id,time1 int
			err = rows.Scan(&id, &device_id, &mode, &time1, &user_id, &created_time)
			if err != nil {
				return err
			}
			fmt.Fprintf(f, "%d,%s,%d,%d,%s,%s\n", id, device_id, mode, time1, user_id, created_time)

			num++
		}
		rows.Close()

		if num < step {
			break
		}

		offset = offset + step
	}

	f.Close()
	return nil
}

func handle_missionChest(startTime string ,endTime string) error {
	fmt.Println("handle_missionChest")
	db, err := CreateDbConnect()
	if err != nil {
		fmt.Println("connect failed.")
	}

	offset := 0
	step := 5000
	fileName := time.Now().Format("missionChest#2006-01-02_15:04:05.csv")
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	for {
		sqlStr := fmt.Sprintf("select auto_id,chests,device_id,id,user_id,created_time from 137_mission_chest where created_time > '%s' and created_time < '%s' limit %d, %d", startTime, endTime,  offset, step)
		rows ,err := db.Query(sqlStr)
		if err != nil {
			fmt.Println(err)
			break
		}

		num := 0
		for rows.Next() {
			var chests,device_id,id,user_id,created_time string
			var auto_id int
			err = rows.Scan(&auto_id, &chests, &device_id, &id, &user_id, &created_time)
			if err != nil {
				return err
			}
			fmt.Fprintf(f, "%d|%s|%s|%s|%s|%s\n", auto_id, chests, device_id, id, user_id, created_time)

			num++
		}
		rows.Close()

		if num < step {
			break
		}

		offset = offset + step
	}

	f.Close()
	return nil
}