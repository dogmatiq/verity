package boltdbtest

import (
	"io/ioutil"
	"os"
	"sync"

	"go.etcd.io/bbolt"
)

// Open opens a BoltDB database using a temporary file.
//
// The returned function must be used to close the database, instead of
// DB.Close().
func Open() (*bbolt.DB, func()) {
	filename, remove := tempFile()

	db, err := bbolt.Open(filename, 0600, nil)
	if err != nil {
		panic(err)
	}

	return db, func() {
		db.Close()
		remove()
	}
}

// tempFile returns the name of a temporary file to be used for a BoltDB
// database.
//
// It returns a function that deletes the temporary file.
func tempFile() (string, func()) {
	f, err := ioutil.TempFile("", "*.boltdb")
	if err != nil {
		panic(err)
	}

	if err := f.Close(); err != nil {
		panic(err)
	}

	file := f.Name()

	if err := os.Remove(file); err != nil {
		panic(err)
	}

	var once sync.Once
	return file, func() {
		once.Do(func() {
			os.Remove(file)
		})
	}
}
