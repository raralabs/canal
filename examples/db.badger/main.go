package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"

	"github.com/raralabs/canal/config"
	"github.com/raralabs/canal/core/pipeline"
)

func main() {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(dir)

	dbPath := config.DbRoot + "test/"

	badger := pipeline.NewBadger(dbPath)
	defer func() {
		badger.Close()
		os.RemoveAll(dbPath)
	}()

	m := map[string][]byte{
		"A": []byte("1"),
		"B": []byte("2"),
		"C": []byte("3"),
		"D": []byte("4"),
	}

	for k, v := range m {
		badger.Store(k, v)
	}

	val, err := badger.Get("E")
	if err == nil {
		fmt.Println(val)
	}

	for k, v := range m {
		val, _ := badger.Get(k)
		if !reflect.DeepEqual(v, val) {
			fmt.Println("Error")
		}
	}

	m1 := badger.Iterate(3)
	m2 := badger.Iterate(1)

	if len(m1) != 3 {
		fmt.Println("Error")
	}
	if len(m2) != 1 {
		fmt.Println("Error")
	}

	for k, v := range m1 {
		if !reflect.DeepEqual(v, m[k]) {
			fmt.Println("Error")
		}
	}
	for k, v := range m2 {
		if !reflect.DeepEqual(v, m[k]) {
			fmt.Println("Error")
		}
	}
}
