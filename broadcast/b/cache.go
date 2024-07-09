package main

import "sync"

type cache struct {
	mu   sync.RWMutex
	data map[int]struct{} // Set data structure.
}
