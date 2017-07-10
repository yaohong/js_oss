package main


const (
	MissionNormalFailed = 1
	MissionNormalCount = 2

	MissionEliteField = 3
	MissionEliteCount = 4

	MissionTower
)

type InquiryItem struct {
	key string
	id int64
}

func NewInquiryItem(key string, id int64) *InquiryItem {
	return &InquiryItem{
		key: key,
		id: id,
	}
}


func (self *InquiryItem)Key() string {
	return self.key
}

func (self *InquiryItem)Id() int64 {
	return self.id
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type InquiryMgr struct {
	inquiryItems map[string] *InquiryItem
	idAlloc int64
	ch chan interface{}
}



func NewInquiryMgr() *InquiryMgr {
	return &InquiryMgr{
		inquiryItems: make(map[string] *InquiryItem),
		idAlloc: 0,
	}
}



func (self *InquiryMgr)generateInquiryId() int64{
	id := self.idAlloc
	self.idAlloc++
	return id
}


func (self *InquiryMgr)Inquiry_MissionNormalFailed(startTime string, endTime string) (id int64, isSuccess bool) {

	return 0, false
}
