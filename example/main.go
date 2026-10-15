package main

import (
	"fmt"

	"github.com/civet148/go-csv"
)

type CsvData struct {
	UserId           uint64 `csv:"用户ID"`
	CardNumber       string `csv:"卡片号码"`
	PhoneNumber      string `csv:"手机号"`
	ChargeCount      uint32 `csv:"充电次数"`
	ExpirationTime   string `csv:"失效时间"`
	CardType         string `csv:"卡片类型"`
	CardCategory     string `csv:"卡类别"`
	StationName      string `csv:"站点名称"`
	MengmaCardNumber string `csv:"猛犸卡号"`
}

func main() {
	reader, err := csv.OpenCSV("./test.csv")
	if err != nil {
		panic(err)
	}
	results, err := csv.ParseCSVRows[CsvData](reader, true)
	if err != nil {
		panic(err)
	}
	for k, v := range results {
		fmt.Printf("row [%v] data [%+v]\n", k, v)
	}
}
