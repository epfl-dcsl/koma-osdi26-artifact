package engine

import (
	"container/heap"
	"fmt"
)

type ConnHeapItem struct {
	ConnID   int
	InitTime float64
}

type ConnHeap []*ConnHeapItem

// Implement heap.Interface
func (h ConnHeap) Len() int { return len(h) }

// min-heap
func (h ConnHeap) Less(i, j int) bool {
	return h[i].InitTime < h[j].InitTime
}

func (h ConnHeap) Swap(i, j int) {
	// Swap elements
	h[i], h[j] = h[j], h[i]
}

// Push adds a new element to the heap
func (h *ConnHeap) Push(x interface{}) {
	item := x.(*ConnHeapItem)
	*h = append(*h, item)
}

// Pop removes the minimum element from the heap (the one with the lowest InitTime)
func (h *ConnHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// Wrapper around heap.Pop() that checks if the heap is empty
func (h *ConnHeap) SafePop() *ConnHeapItem {
	if h.Len() == 0 {
		return nil
	}
	item := heap.Pop(h).(*ConnHeapItem) // Safe to pop now
	return item
}

// PrintAllElements prints all elements in the heap
func (h ConnHeap) PrintAllElements() {
	for _, item := range h {
		fmt.Printf("[ConnHeap] ConnID: %d, InitTime: %f\n", item.ConnID, item.InitTime)
	}
}
