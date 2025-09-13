# go-csv
golang CSV file generator which support zip compress

## Usage

```go
package main

import (
    "fmt"
    "time"
    "github.com/civet148/go-csv"
)

func main() {

    var err error
    var writer *csv.CsvWriter
    writer, err = csv.NewWriter(fmt.Sprintf("export-20250913.csv"),
        csv.WithZip(), // zip compress
        csv.WithFlushCount(2000), // flush when rows % 2000 == 0
    ) 
    if err != nil {
        fmt.Printf("ERROR: %s \n", err)
        return 
    }
    defer writer.Close()
    // add csv header
    err = writer.WriteHeader([]string{"OrderID", "OrderTime", "GoodsName", "Quantity"}) 
    // mock csv rows
    for i := 0; i < 100000; i++ {
        var record []any
        record = append(record, fmt.Sprintf("%d", i+1))
        record = append(record, time.Format(time.DateTime))
        record = append(record, "food")
        record = append(record, i+100)
        if err = writer.WriteRow(record); err != nil {
            fmt.Printf("ERROR: %s \n", err)
            return 
        } 
    }
	
    // true means flush and generate zip file
    if err = writer.Flush(true); err != nil {
        fmt.Printf("ERROR: %s \n", err)
        return 
    } 
}

```

