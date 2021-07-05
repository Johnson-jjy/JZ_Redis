package SortedSet

import (
	"strconv"
)

// SortedSet is a set which keys sorted by bound score
type SortedSet struct {
	dict map[string]*Element
	skiplist *skiplist
}

// Make makes a new SortedSet
func Make() *SortedSet {
	return &SortedSet{
		dict: make(map[string]*Element),
		skiplist: makeSkiplist(),
	}
}

// Add puts member into set, and returns whether has inserted new node
func (sortedSet *SortedSet) Add(member string, score float64) bool {
	element, ok := sortedSet.dict[member]
	sortedSet.dict[member] = &Element{
		Member: member,
		Score: score,
	}
	if ok {
		if score != element.Score{
			sortedSet.skiplist.remove(member, score)
			sortedSet.skiplist.insert(member, score)
		}
		return false
	}
	sortedSet.skiplist.insert(member, score)
	return true
}

// Len returns the given member
func (sortedSet *SortedSet) Len() int64 {
	return int64(len(sortedSet.dict)) // 这里为什么不返回skiplist的length?
}

// Get returns the given member
func (sortedSet *SortedSet) Get(member string) (element *Element, ok bool) {
	element, ok = sortedSet.dict[member]
	if !ok {
		return nil, false
	}
	return element, true
}

// Remove removes the given member from set
func (sortedSet *SortedSet) Remove(member string) bool {
	element, ok := sortedSet.dict[member]
	if ok {
		sortedSet.skiplist.remove(member, element.Score)
		delete(sortedSet.dict, member)
		return true
	}
	return false
}

// GetRank returns the rank of the given member, sort by ascending order, rank starts from 0
func (sortedSet *SortedSet) GetRank(member string, desc bool) (rank int64) {
	element, ok := sortedSet.dict[member]
	if !ok {
		return -1
	}
	r := sortedSet.skiplist.getRank(member, element.Score)
	if desc {
		return sortedSet.skiplist.length - r
	} else {
		r--
	}
	return r
}

// ForEach visits each member which rank within [start, stop), sort by ascending order, rank start from 0
func (sortedSet *SortedSet) Foreach(start int64, stop int64, desc bool, consumer func(element *Element) bool)  {
	size := int64(sortedSet.Len()) // 这里的int64多余了，下同
	if start < 0 || start >= size {
		panic("illegal start " + strconv.FormatInt(start, 10))
	}
	if stop < start || stop > size {
		panic("illegal stop " + strconv.FormatInt(stop, 10))
	}

	// find start node
	var node *node
	if desc {
		node = sortedSet.skiplist.tail
		if start > 0 {
			node = sortedSet.skiplist.getByRank(int64(size - start))
		}
	} else {
		node = sortedSet.skiplist.header.level[0].forward
		if start > 0 {
			node = sortedSet.skiplist.getByRank(int64(start + 1))
		}
	}

	sliceSize := int(stop - start)
	for i := 0; i < sliceSize; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}
}
