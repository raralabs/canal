package pipeline

import (
	"log"
	"os"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/raralabs/canal/config"
	"github.com/raralabs/canal/utils/handle"
)

// Badger implements the IPersistor interface using badgerDB
type Badger struct {
	db *badger.DB

	lastKey      []byte
	firstIterate bool
}

func NewBadger(path string) *Badger {
	os.MkdirAll(path, os.ModePerm)

	opts := badger.DefaultOptions(path)
	db, err := badger.Open(opts)
	handle.Error(err)

	return &Badger{
		db:           db,
		firstIterate: true,
	}
}

func (b *Badger) Store(key string, value []byte) error {
	return b.StoreWithTTL(key, value, config.DefaultTTL)
}

func (b *Badger) StoreMap(m map[string][]byte) error {
	return b.StoreMapWithTTL(m, config.DefaultTTL)
}

func (b *Badger) StoreWithTTL(key string, value []byte, ttl time.Duration) error {
	err := b.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry([]byte(key), value).WithTTL(ttl)
		err := txn.SetEntry(e)
		return err
	})

	return err
}

func (b *Badger) StoreMapWithTTL(m map[string][]byte, ttl time.Duration) error {
	err := b.db.Update(func(txn *badger.Txn) error {
		for key, value := range m {
			e := badger.NewEntry([]byte(key), value).WithTTL(ttl)
			err := txn.SetEntry(e)

			if err != nil {
				log.Print(err)
			}
		}
		return nil
	})

	return err
}

func (b *Badger) Get(key string) ([]byte, error) {
	var value []byte

	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))

		if err == nil {
			value, err = item.ValueCopy(nil)
		}
		return err
	})

	return value, err
}

func (b *Badger) Iterate(numItems int) map[string][]byte {
	pairs := make(map[string][]byte, numItems)

	if numItems == 0 {
		return pairs
	}

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = numItems
		it := txn.NewIterator(opts)
		defer it.Close()

		count := 0
		if b.firstIterate {
			it.Rewind()
			b.firstIterate = false
		} else {
			it.Seek(b.lastKey)
			it.Next()
		}

		for ; it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()

			value, err := item.ValueCopy(nil)
			pairs[string(k)] = value

			count++
			if count == numItems {
				b.lastKey = k
				break
			}

			if err != nil {
				return err
			}
		}

		return nil
	})
	handle.Error(err)

	return pairs
}

func (b *Badger) ResetPaging() {
	b.firstIterate = true
}

func (b *Badger) Close() {
	b.db.Close()
}
