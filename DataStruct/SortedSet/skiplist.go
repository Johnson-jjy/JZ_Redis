package SortedSet

import (
	"math/rand"
)

const (
	maxLevel = 32
)

// Element is a key-score pair
type Element struct {
	Member string
	Score  float64
}

type node struct {
	backward *node
	level    []*Level // level[0] is base level
	Element
}

// Level aspect of a node
type Level struct {
	forward *node // forward node has greater score
	span    int64 // number of nodes skipped to forward
}

// 这里重新排列是否
type skiplist struct {
	header *node
	tail   *node
	length int64
	level  int16
}

func makeNode(level int16, score float64, member string) *node {
	n := &node{
		backward: nil,
		level: make([]*Level, level),
		Element: Element{
			member,
			score,
		},
	}
	for i := range n.level {
		n.level[i] = new(Level)
	}

	return n
}

func makeSkiplist() *skiplist {
	return &skiplist{
		header: makeNode(maxLevel, 0, ""),
		tail: nil,
		length: 0,
		level: 1,
	}
}

func randomLevel() int16 {
	level := int16(1)
	for float32(rand.Int31() & 0xFFFF) < (0.25 * 0xFFFF) {
		level++
	}
	if level < maxLevel {
		return level
	}
	return maxLevel
}

func (skiplist *skiplist) insert(member string, score float64) *node {
	update := make([]*node, maxLevel) // link new with node in this array
	rank := make([]int64, maxLevel) // this store the pre rank to calculate new span

	// find position to insert
	node := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		if i == skiplist.level - 1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i + 1]
		}

		if node.level[i].forward != nil {
			// traverse the skip list
			for node.level[i].forward != nil && (node.level[i].forward.Score < score ||
				(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) {// same score but less member
				rank[i] += node.level[i].span
				node = node.level[i].forward
			}
		}

		update[i] = node
	}

	level := randomLevel()
	// extend skiplist level
	if level > skiplist.level {
		for i := skiplist.level; i < level; i++ {
			rank[i] = 0
			update[i] = skiplist.header
			update[i].level[i].span = skiplist.length
		}
		skiplist.level = level
	}

	// make node and link into skiplist
	node = makeNode(level, score, member)
	for i := int16(0); i < level; i++ {
		node.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = node

		// update span covered by update[i] as node is inserted here
		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// increment span for untouched levels
	for i := level; i < skiplist.level; i++ {
		update[i].level[i].span++
	}

	// set backward node
	if update[0] == skiplist.header {
		node.backward = nil
	} else {
		node.backward = update[0]
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		skiplist.tail = node
	}
	skiplist.length++
	return node
}

func (skiplist *skiplist) remove(memeber string, score float64) bool {
	update := make([]*node, maxLevel)
	node := skiplist.header

	for i := int16(0); i < skiplist.level; i++ {
		for node.level[i].forward != nil && (node.level[i].forward.Score < score ||
			(node.level[i].forward.Score == score && node.level[i].forward.Member < memeber)) {
			node = node.level[i].forward
		}
		update[i] = node
	}

	node = update[0].level[0].forward
	if node != nil && node.Score == score && node.Member == memeber {
		skiplist.removeNode(node, update)
		// free x
		return true
	}
	return false
}

func (skiplist *skiplist)removeNode(node *node, update []*node) {
	for i := int16(0); i < skiplist.level; i++ {
		if update[i].level[i].forward == node {
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].forward = node.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = update[0]
	} else {
		skiplist.tail = node.backward
	}
	for skiplist.level > 1 && skiplist.header.level[skiplist.level - 1].forward == nil {
		skiplist.level--
	}
	skiplist.length--
}