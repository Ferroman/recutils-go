package main

import (
	"fmt"

	"github.com/Ferroman/recutils/rec"
	reclib "github.com/Ferroman/recutils/reclib"
)

func main() {
	filename := "example.rec"
	db := reclib.NewRecDB()
	if db == nil {
		fmt.Println("Failed to open rec file")
		return
	}
	err := db.LoadFile(filename)
	if err != nil {
		fmt.Println("Failed to load rec file:", err)
		return
	}
	fmt.Println("Rec file opened successfully!")
	fmt.Println("Database size:", db.Size())

	rs, err := db.GetRecordSet(0)
	if err != nil {
		fmt.Println("Failed to get record set:", err)
		return
	}

	db.PrintRecordSet(rs)

	// load all records by query
	rs, err = db.Query(rec.QueryParams{
		Type: "Book",
	})
	if err != nil {
		fmt.Println("Failed to query records:", err)
		return
	}
	db.PrintRecordSet(rs)
}
