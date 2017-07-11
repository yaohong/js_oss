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
		fmt.Fprintf(f, "%s|%d\n", k, v)
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
	datas := make(map[string] int, 10)
	for {
		sqlStr := fmt.Sprintf("select created_time from 137_mission_normal where created_time > '%s' and created_time < '%s' limit %d, %d", startTime, endTime,  offset, step)
		rows ,err := db.Query(sqlStr)
		if err != nil {
			fmt.Println(err)
			break
		}

		num := 0
		for rows.Next() {
			var createTime string

			err = rows.Scan(&createTime)
			if err != nil {
				return err
			}

			timeValue := strings.Split(createTime, " ")
			if len(timeValue) != 2 {
				return errors.New("createTime value error," + createTime)
			}
			bigTime := timeValue[0]

			oldCount, ok := datas[bigTime]
			if ok {
				datas[bigTime] = oldCount  + 1
			} else {
				datas[bigTime] = 1
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
		fmt.Fprintf(f, "%s|%d\n", k, v)
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
		fmt.Fprintf(f, "%s|%d\n", k, v)
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
	datas := make(map[string] int, 10)
	for {
		sqlStr := fmt.Sprintf("select created_time from 137_mission_elite where created_time > '%s' and created_time < '%s' limit %d, %d", startTime, endTime,  offset, step)
		rows ,err := db.Query(sqlStr)
		if err != nil {
			fmt.Println(err)
			break
		}

		num := 0
		for rows.Next() {
			var createTime string

			err = rows.Scan(&createTime)
			if err != nil {
				return err
			}

			timeValue := strings.Split(createTime, " ")
			if len(timeValue) != 2 {
				return errors.New("createTime value error," + createTime)
			}
			bigTime := timeValue[0]

			oldCount, ok := datas[bigTime]
			if ok {
				datas[bigTime] = oldCount + 1
			} else {
				datas[bigTime] = 1
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
		fmt.Fprintf(f, "%s|%d\n", k, v)
	}

	f.Close()
	return nil
}

type UserTowerMax struct {
	DeviceId string
	Id string
	Heroes string
	Score int
	Floor int
	CreateTime string
}

func NewUserTowerMax(deviceId string, id string, heroes string, score int, floor int, createTime string) *UserTowerMax {
	return &UserTowerMax {
		DeviceId: deviceId,
		Id: id,
		Heroes: heroes,
		Score: score,
		Floor: floor,
		CreateTime: createTime,
	}
}


func (self *UserTowerMax)compareFloor(floor int) bool {
	return floor > self.Floor
}

func handle_missionTowerMax(startTime string ,endTime string) error {
	fmt.Println("handle_missionTowerMax")
	db, err := CreateDbConnect()
	if err != nil {
		fmt.Println("connect failed.")
	}

	offset := 0
	step := 5000
	allData := make(map[string]*UserTowerMax, 1000)
	for {
		sqlStr := fmt.Sprintf(
			"select user_id,device_id,id,heroes,score,floor,created_time from 137_mission_tower where created_time > '%s' and created_time < '%s' and re!=-1 limit %d, %d",
			startTime, endTime,  offset, step)
		rows ,err := db.Query(sqlStr)
		if err != nil {
			fmt.Println(err)
			break
		}

		num := 0
		for rows.Next() {
			var user_id, device_id, id, heroes, createTime string
			var score, floor int
			err = rows.Scan(&user_id, &device_id, &id, &heroes, &score, &floor, &createTime)
			if err != nil {
				return err
			}

			old, ok := allData[user_id]
			if ok {
				if old.compareFloor(floor) {
					data := NewUserTowerMax(device_id, id, heroes, score, floor, createTime)
					allData[user_id] = data
				}
			} else {
				data := NewUserTowerMax(device_id, id, heroes, score, floor, createTime)
				allData[user_id] = data
			}


			num++
		}
		rows.Close()

		if num < step {
			break
		}

		offset = offset + step
	}

	fileName := time.Now().Format("missionTowerMax#2006-01-02_15:04:05.csv")
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}

	for k, v:= range allData {
		fmt.Fprintf(f, "%s|%s|%s|%s|%d|%d|%s\n", k, v.DeviceId, v.Id, v.Heroes, v.Score, v.Floor, v.CreateTime)
	}

	f.Close()
	return nil
}


type ExcellentUser struct {
	UserId string
	Floor int
	Score int
}

func NewExcellentUser(userId string, floor int, score int) *ExcellentUser {
	return &ExcellentUser{
		UserId: userId,
		Floor: floor,
		Score: score,
	}
}


func (self *ExcellentUser)Replace(userId string, floor int, score int) {
	if self.Floor > floor {

	} else if self.Floor == floor {
		if self.Score >= score {

		} else {
			self.Score = score
			self.UserId = userId
		}
	} else {
		self.Floor = floor
		self.Score = score
		self.UserId = userId
	}
}

func handle_missionTowerCount(startTime string ,endTime string) error {
	fmt.Println("handle_missionTowerCount")
	db, err := CreateDbConnect()
	if err != nil {
		fmt.Println("connect failed.")
	}

	offset := 0
	step := 5000
	countMap := make(map[string] int, 10)
	mostExcellentMap := make(map[string] *ExcellentUser, 10)
	for {
		sqlStr := fmt.Sprintf(
			"select created_time,id,user_id,floor,score from 137_mission_tower where created_time > '%s' and created_time < '%s' and re!=-1 limit %d, %d",
			startTime, endTime,  offset, step)
		rows ,err := db.Query(sqlStr)
		if err != nil {
			fmt.Println(err)
			break
		}

		num := 0
		for rows.Next() {
			var createTime, id, user_id string
			var floor,score int
			err = rows.Scan(&createTime, &id, &user_id, &floor, &score)
			if err != nil {
				return err
			}

			timeValue := strings.Split(createTime, " ")
			if len(timeValue) != 2 {
				return errors.New("createTime value error," + createTime)
			}

			bigTime := timeValue[0]
			OldCount, ok := countMap[bigTime]
			if ok {
				countMap[bigTime] = OldCount + 1
			} else {
				countMap[bigTime] = 1
			}

			mostExcellent, ok1 := mostExcellentMap[bigTime]
			if ok1 {
				mostExcellent.Replace(user_id, floor, score)
			} else {
				mostExcellentMap[bigTime] = NewExcellentUser(user_id, floor, score)
			}


			num++
		}
		rows.Close()

		if num < step {
			break
		}

		offset = offset + step
	}

	fileName := time.Now().Format("missionTowerCount#2006-01-02_15:04:05.csv")
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}

	for k, v:= range countMap {
		v1, _ := mostExcellentMap[k]
		fmt.Fprintf(f, "%s|%d|%s|%d|%d\n", k, v, v1.UserId, v1.Floor, v1.Score)
	}

	f.Close()
	return nil
}

func handle_missionSeasonMax(startTime string ,endTime string) error {
	fmt.Println("handle_missionSeasonMax")
	db, err := CreateDbConnect()
	if err != nil {
		fmt.Println("connect failed.")
	}

	offset := 0
	step := 5000
	allData := make(map[string]*UserTowerMax, 1000)
	for {
		sqlStr := fmt.Sprintf(
			"select user_id,device_id,id,heroes,score,floor,created_time from 137_mission_season where created_time > '%s' and created_time < '%s' and re!=-1 limit %d, %d",
			startTime, endTime,  offset, step)
		rows ,err := db.Query(sqlStr)
		if err != nil {
			fmt.Println(err)
			break
		}

		num := 0
		for rows.Next() {
			var user_id, device_id, id, heroes, createTime string
			var score, floor int
			err = rows.Scan(&user_id, &device_id, &id, &heroes, &score, &floor, &createTime)
			if err != nil {
				return err
			}

			old, ok := allData[user_id]
			if ok {
				if old.compareFloor(floor) {
					data := NewUserTowerMax(device_id, id, heroes, score, floor, createTime)
					allData[user_id] = data
				}
			} else {
				data := NewUserTowerMax(device_id, id, heroes, score, floor, createTime)
				allData[user_id] = data
			}


			num++
		}
		rows.Close()

		if num < step {
			break
		}

		offset = offset + step
	}

	fileName := time.Now().Format("missionSeasonMax#2006-01-02_15:04:05.csv")
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}

	for k, v:= range allData {
		fmt.Fprintf(f, "%s|%s|%s|%s|%d|%d|%s\n", k, v.DeviceId, v.Id, v.Heroes, v.Score, v.Floor, v.CreateTime)
	}

	f.Close()
	return nil
}

func handle_missionSeasonCount(startTime string ,endTime string) error {
	fmt.Println("handle_missionSeasonCount")
	db, err := CreateDbConnect()
	if err != nil {
		fmt.Println("connect failed.")
	}

	offset := 0
	step := 5000
	countMap := make(map[string] int, 10)
	mostExcellentMap := make(map[string] *ExcellentUser, 10)
	for {
		sqlStr := fmt.Sprintf(
			"select created_time,id,user_id,floor,score from 137_mission_season where created_time > '%s' and created_time < '%s' and re!=-1 limit %d, %d",
			startTime, endTime,  offset, step)
		rows ,err := db.Query(sqlStr)
		if err != nil {
			fmt.Println(err)
			break
		}

		num := 0
		for rows.Next() {
			var createTime, id, user_id string
			var floor,score int
			err = rows.Scan(&createTime, &id, &user_id, &floor, &score)
			if err != nil {
				return err
			}

			timeValue := strings.Split(createTime, " ")
			if len(timeValue) != 2 {
				return errors.New("createTime value error," + createTime)
			}

			bigTime := timeValue[0]
			OldCount, ok := countMap[bigTime]
			if ok {
				countMap[bigTime] = OldCount + 1
			} else {
				countMap[bigTime] = 1
			}

			mostExcellent, ok1 := mostExcellentMap[bigTime]
			if ok1 {
				mostExcellent.Replace(user_id, floor, score)
			} else {
				mostExcellentMap[bigTime] = NewExcellentUser(user_id, floor, score)
			}


			num++
		}
		rows.Close()

		if num < step {
			break
		}

		offset = offset + step
	}

	fileName := time.Now().Format("missionSeasonCount#2006-01-02_15:04:05.csv")
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}

	for k, v:= range countMap {
		v1, _ := mostExcellentMap[k]
		fmt.Fprintf(f, "%s|%d|%s|%d|%d\n", k, v, v1.UserId, v1.Floor, v1.Score)
	}

	f.Close()
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
			fmt.Fprintf(f, "%d|%s|%d|%s|%s\n", id, device_id, time1, user_id, created_time)

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
			fmt.Fprintf(f, "%d|%s|%d|%d|%s|%s\n", id, device_id, mode, time1, user_id, created_time)

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