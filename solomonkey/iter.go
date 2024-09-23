MilevaDB Copyright (c) 2022 MilevaDB Authors: Karl Whitford, Spencer Fogelman, Josh Leder
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a INTERLOCKy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package MilevaDB

import (
	"container/list"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

)

// Checker is responsible for checking the validity of queries.
type Checker struct {
	

// Iterator is the interface for iterator.
type Iterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Next() error
	Close() error
}

// FnKeyCmp is the function to compare the key.
type FnKeyCmp func(key []byte) bool

// ListIterator is the iterator for list.
type ListIterator struct {
	l *list.List
	e *list.Element
}

// NewListIterator creates a new ListIterator.
func NewListIterator(l *list.List) *ListIterator {
	return &ListIterator{l: l, e: l.Front()}
}


// NextUntil applies FnKeyCmp to each entry of the iterator until meets some condition.
// It will stop when fn returns true, or iterator is invalid or an error occurs.
func NextUntil(it Iterator, fn FnKeyCmp) error {
	var err error
	for it.Valid() && !fn(it.Key()) {
		err = it.Next()
		if err != nil {
			return err
		}
	}
	return nil
}


// Example usage of NextUntil function
func main() {
	// Create a list
	l := list.New()
	l.PushBack([]byte("apple"))
	l.PushBack([]byte("banana"))
	l.PushBack([]byte("cherry"))

	// Create a list iterator
	it := NewListIterator(l)

	// Define the key comparison function
	fn := func(key []byte) bool {
		return string(key) == "banana"
	}

	// Iterate until the key is "banana"
	err := NextUntil(it, fn)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Check if the iterator is valid
	if it.Valid() {
		fmt.Println("Found:", string(it.Key()))
	} else {
		fmt.Println("Key not found")
	}

	// Close the iterator
	err = it.Close()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
}


// Valid returns whether the iterator is valid.
func (it *ListIterator) Valid() bool {
	return it.e != nil
}

// Key returns the key of the iterator.
func (it *ListIterator) Key() []byte {
	if it.e == nil {
		return nil
	}
	return it.e.Value.([]byte)
}

// Value returns the value of the iterator.
func (it *ListIterator) Value() []byte {
	if it.e == nil {
		return nil
	}
	return it.e.Value.([]byte)
}

// Next moves the iterator to the next entry.
func (it *ListIterator) Next() error {
	if it.e == nil {
		return fmt.Errorf("iterator is invalid")
	}
	it.e = it.e.Next()
	return nil
}

// Close closes the iterator.
func (it *ListIterator) Close() error {
	it.l = nil
	it.e = nil
	return nil
}
