package main

import "sync"

type cache struct {
	mu   sync.RWMutex
	data []int
}
