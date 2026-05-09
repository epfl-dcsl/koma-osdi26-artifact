package blocks

import (
	//"container/heap"
	"container/list"
	//"sort"
	//"fmt"

	"gitlab.epfl.ch/ryang/schedsim/engine"
)

var count = 0

// Queue is a imple FIFO queue
type Queue struct {
	l         *list.List
	id        int
	inProcess bool // for ConnHeap, true if there is a req fro the queue being processing by a processor
}

// NewQueue returns a new *Queue
func NewQueue() *Queue {
	q := &Queue{}
	q.l = list.New()
	q.id = count
	count++
	return q
}

// Enqueue enqueues a new ReqInterface at the queue
func (q *Queue) Enqueue(el engine.ReqInterface) {
	// fmt.Printf("time: %v, queue: %v, len: %v\n", engine.GetTime(), q.id, q.Len())
	q.l.PushBack(el)
}

// Dequeue dequeues the last ReqInterface from the queue
func (q *Queue) Dequeue() engine.ReqInterface {
	el := q.l.Front()
	q.l.Remove(el)
	return el.Value.(engine.ReqInterface)
}

// Len returns the queue length
func (q *Queue) Len() int {
	return q.l.Len()
}

func (q *Queue) GetinProcess() bool {
	return q.inProcess
}

func (q *Queue) SetinProcess(v bool) {
	q.inProcess = v
}

// Peek the first element of the queue without removing it from the queue.
func (q *Queue) Peekqueue() engine.ReqInterface {
	el := q.l.Front()
	return el.Value.(engine.ReqInterface)
}
