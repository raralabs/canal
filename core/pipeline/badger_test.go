package pipeline

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"github.com/raralabs/canal/config"
	"github.com/stretchr/testify/assert"
)

func TestBadger(t *testing.T) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(dir)

	dbPath := config.DbRoot + "test/"

	m := map[string][]byte{
		"A": []byte("1"),
		"B": []byte("2"),
		"C": []byte("3"),
		"D": []byte("4"),
	}

	t.Run("KV tests", func(t *testing.T) {
		bgDB := NewBadger(dbPath)
		defer func() {
			bgDB.Close()
			// Remove all files created by db during test
			os.RemoveAll(dbPath)
		}()

		for k, v := range m {
			bgDB.Store(k, v)
		}

		for k, v := range m {
			val, _ := bgDB.Get(k)
			if !reflect.DeepEqual(v, val) {
				fmt.Println("Error")
			}
		}

		for k, v := range bgDB.Iterate(1) {
			if !reflect.DeepEqual(v, m[k]) {
				fmt.Printf("Want: %v, Got: %v", m[k], v)
			}
		}

		m2 := bgDB.Iterate(1)
		assert.Equal(t, 1, len(m2))
		for k, v := range m2 {
			if !reflect.DeepEqual(v, m[k]) {
				fmt.Printf("Want: %v, Got: %v", m[k], v)
			}
		}

		bgDB.ResetPaging()
		m3 := bgDB.Iterate(4)

		assert.Equal(t, 4, len(m3))
		for k, v := range m3 {
			if !reflect.DeepEqual(v, m[k]) {
				fmt.Printf("Want: %v, Got: %v", m[k], v)
			}
		}
	})

	t.Run("Map tests", func(t *testing.T) {
		badger := NewBadger(dbPath)
		defer func() {
			badger.Close()
			// Remove all files created by db during test
			os.RemoveAll(dbPath)
		}()
		badger.StoreMap(m)

		//assert.Equal(t, uint64(len(m)), badger.Length())

		for k, v := range m {
			val, _ := badger.Get(k)
			if !reflect.DeepEqual(v, val) {
				fmt.Println("Error")
			}
		}

		m1 := badger.Iterate(3)
		m2 := badger.Iterate(1)

		assert.Equal(t, 3, len(m1))
		assert.Equal(t, 1, len(m2))

		for k, v := range m1 {
			if !reflect.DeepEqual(v, m[k]) {
				fmt.Printf("Want: %v, Got: %v", m[k], v)
			}
		}
		for k, v := range m2 {
			if !reflect.DeepEqual(v, m[k]) {
				fmt.Printf("Want: %v, Got: %v", m[k], v)
			}
		}

		badger.ResetPaging()
		m3 := badger.Iterate(4)

		assert.Equal(t, 4, len(m3))
		for k, v := range m3 {
			if !reflect.DeepEqual(v, m[k]) {
				fmt.Printf("Want: %v, Got: %v", m[k], v)
			}
		}

		badger.ResetPaging()
		m4 := badger.Iterate(0)
		assert.Equal(t, 0, len(m4))
	})

}
