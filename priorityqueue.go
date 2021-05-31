package distkvs

type Item struct {
	ClientId        string
	OpId            uint32
	Key             string
	Value           string
	Request         string
	ResponseChannel chan FrontEndResult

	Index int // The index of the item in the heap.
}

type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the lowest based on expiration number as the priority
	// The lower the opId, the higher the priority
	return pq[i].OpId < pq[j].OpId
}

// We just implement the pre-defined function in interface of heap.

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.Index = -1
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) Peek() interface{} {
	old := *pq
	n := len(old)
	if n == 0 {
		return nil
	}
	item := old[0]
	return item
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}
