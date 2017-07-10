package main
import (
//	"net/http"
	"fmt"
	"os"
)

func main() {
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

	switch os.Args[1] {
	case "missionNormalFailed":
		handle_missionNormalFailed(os.Args[2], os.Args[3])
	case "missionNormalCount":
		handle_missionNormalCount(os.Args[2], os.Args[3])
	case "missionEliteFailed":
		handle_missionEliteFailed(os.Args[2], os.Args[3])
	case "missionEliteCount":
		handle_missionEliteCount(os.Args[2], os.Args[3])
	case "missionTowerMax":
		handle_missionTowerMax(os.Args[2], os.Args[3])
	case "missionTowerCount":
		handle_missionTowerCount(os.Args[2], os.Args[3])
	case "missionSeasonMax":
		handle_missionSeasonMax(os.Args[2], os.Args[3])
	case "missionSeasonCount":
		handle_missionSeasonCount(os.Args[2], os.Args[3])
	case "missionTowerInsprite":
		handle_missionTowerInsprite(os.Args[2], os.Args[3])
	case "missionTowerTimes":
		handle_missionTowerTimes(os.Args[2], os.Args[3])
	case "missionChest":
		handle_missionChest(os.Args[2], os.Args[3])
	default:
		fmt.Printf("cmd %s error.\n", os.Args[1])
	}

}


func handle_missionNormalFailed(startTime string ,endTime string) error {

	return nil
}

func handle_missionNormalCount(startTime string ,endTime string) error {

	return nil
}

func handle_missionEliteFailed(startTime string ,endTime string) error {

	return nil
}

func handle_missionEliteCount(startTime string ,endTime string) error {

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

	return nil
}

func handle_missionTowerTimes(startTime string ,endTime string) error {

	return nil
}

func handle_missionChest(startTime string ,endTime string) error {

	return nil
}