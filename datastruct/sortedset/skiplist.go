package SortedSet

import (
	"math/rand"
)

const (
	maxLevel = 32
)

// Element is a key-score pair 对外的元素抽象
type Element struct {
	Member string
	Score  float64
}

type node struct {
	backward *node // 后向指针
	level    []*Level // 前向指针, level[0] 为最下层 level[0] is base level
	Element // 元素的名称和 score
}

// Level aspect of a node 节点中每一层的抽象
type Level struct {
	forward *node // forward node has greater score 指向同层中的下一个节点
	span    int64 // number of nodes skipped to forward 到 forward 跳过的节点数
}

// 跳表的定义
type skiplist struct {
	header *node
	tail   *node
	length int64
	level  int16
}

func makeNode(level int16, score float64, member string) *node {
	n := &node{
		backward: nil,
		level:    make([]*Level, level),
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
		tail:   nil,
		length: 0,
		level:  1,
	}
}

// randomLevel 用于随机决定新节点包含的层数，随机结果出现2的概率是出现1的25%， 出现3的概率是出现2的25%
func randomLevel() int16 {
	level := int16(1)
	for float32(rand.Int31()&0xFFFF) < (0.25 * 0xFFFF) {
		level++
	}
	if level < maxLevel {
		return level
	}
	return maxLevel
}

func (skiplist *skiplist) insert(member string, score float64) *node {
	// 寻找新节点的先驱节点，它们的 forward 将指向新节点
	// 因为每层都有一个 forward 指针, 所以每层都会对应一个先驱节点
	// 找到这些先驱节点并保存在 update 数组中
	update := make([]*node, maxLevel) // link new with node in this array
	rank := make([]int64, maxLevel)   // this store the pre rank to calculate new span 保存各层先驱节点的排名，用于计算span

	// find position to insert
	node := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- { // 从上层向下寻找
		// 初始化 rank
		if i == skiplist.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		if node.level[i].forward != nil {
			// traverse the skip list // 遍历搜索
			for node.level[i].forward != nil && (node.level[i].forward.Score < score ||
				(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) { // same score but less member
				rank[i] += node.level[i].span
				node = node.level[i].forward
			}
		}

		update[i] = node
	}

	level := randomLevel() // 随机决定新节点的层数
	// extend skiplist level
	if level > skiplist.level {
		for i := skiplist.level; i < level; i++ {
			rank[i] = 0
			update[i] = skiplist.header
			update[i].level[i].span = skiplist.length
		}
		skiplist.level = level
	}

	// make node and link into skiplist 创建新节点并插入跳表
	node = makeNode(level, score, member)
	for i := int16(0); i < level; i++ {
		// 新节点的 forward 指向先驱节点的 forward
		node.level[i].forward = update[i].level[i].forward
		// 先驱节点的 forward 指向新节点
		update[i].level[i].forward = node

		// update span covered by update[i] as node is inserted here 计算先驱节点和新节点的 span
		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// increment span for untouched levels
	// 新节点可能不会包含所有层
	// 对于没有层，先驱节点的 span 会加1 (后面插入了新节点导致span+1)
	for i := level; i < skiplist.level; i++ {
		update[i].level[i].span++
	}

	// set backward node 更新后向指针
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

/*
 * return: has found and removed node
 */
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

/*
 * param node: node to delete
 * param update: backward node (of target)
 */
// 传入目标节点和删除后的先驱节点
// 在批量删除时我们传入的 update 数组是相同的
func (skiplist *skiplist) removeNode(node *node, update []*node) {
	for i := int16(0); i < skiplist.level; i++ {
		// 如果先驱节点的forward指针指向了目标节点，则需要修改先驱的forward指针跳过要删除的目标节点
		// 同时更新先驱的 span
		if update[i].level[i].forward == node {
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].forward = node.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}
	// 修改目标节点后继节点的backward指针
	if node.level[0].forward != nil {
		node.level[0].forward.backward = update[0]
	} else {
		skiplist.tail = node.backward
	}
	// 必要时删除空白的层
	for skiplist.level > 1 && skiplist.header.level[skiplist.level-1].forward == nil {
		skiplist.level--
	}
	skiplist.length--
}

/*
 * return: 1 based rank, 0 means member not found
 */
// 寻找排名为 rank 的节点, rank 从1开始
func (skiplist *skiplist) getRank(member string, score float64) int64 {
	var rank int64 = 0
	x := skiplist.header

	for i := skiplist.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && (x.level[i].forward.Score < score ||
			(x.level[i].forward.Score == score && x.level[i].forward.Member <= member)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}

		/* x might be equal to zsl->header, so test if obj is non-NULL */
		if x.Member == member {
			return rank
		}
	}

	return 0 // not found
}

/*
 * 1 - based rank
 */
func (skiplist *skiplist) getByRank(rank int64) *node {
	var i int64 = 0
	n := skiplist.header

	// 从顶层向下查询
	for level := skiplist.level; level >= 0; level-- {
		// 从当前层向前搜索
		// 若当前层的下一个节点已经超过目标 (i+n.level[level].span > rank)，则结束当前层搜索进入下一层
		for n.level[level].forward != nil && (i+n.level[level].span <= rank) {
			i += n.level[level].span
			n = n.level[level].forward
		}

		if i == rank {
			return n
		}
	}

	return nil
}

func (skiplist *skiplist) hasInRange(min *ScoreBorder, max *ScoreBorder) bool {
	// min & max = empty
	if min.Value > max.Value || (min.Value == max.Value && (min.Exclude || max.Exclude)) {
		return false
	}
	// min > tail
	n := skiplist.tail
	if n == nil || min.less(n.Score) {
		return false
	}
	// max < head
	n = skiplist.header.level[0].forward
	if n == nil || max.greater(n.Score) {
		return false
	}
	return true
}

// ZRangeByScore 命令需要 getFirstInScoreRange 函数找到分数范围内第一个节点
func (skiplist *skiplist) getFirstInScoreRange(min *ScoreBorder, max *ScoreBorder) *node {
	// 判断跳表和范围是否有交集，若无交集提早返回
	if !skiplist.hasInRange(min, max) {
		return nil
	}
	n := skiplist.header

	// scan from top level 从顶层向下查询
	for i := skiplist.level; i >= 0; i-- {
		// if forward is not in range then move forward
		// 若 forward 节点仍未进入范围则继续向前(forward)
		// 若 forward 节点已进入范围，当 level > 0 时 forward 节点不能保证是 *第一个* 在 min 范围内的节点， 因此需进入下一层查找
		if n.level[i].forward != nil && !min.less(n.level[i].forward.Score) {
			n = n.level[i].forward
		}
	}
	/* This is an inner range, so the next node cannot be NULL */
	// 当从外层循环退出时 level=0 (最下层), n.level[0].forward 一定是 min 范围内的第一个节点
	n = n.level[0].forward
	if !max.greater(n.Score) {
		return nil
	}
	return n
}

func (skiplist *skiplist) getLastInScoreRange(min *ScoreBorder, max *ScoreBorder) *node {
	if !skiplist.hasInRange(min, max) {
		return nil
	}
	n := skiplist.header
	// scan from top level
	for i := skiplist.level - 1; i >= 0; i-- {
		for n.level[i].forward != nil && max.greater(n.level[i].forward.Score) {
			n = n.level[i].forward
		}
	}
	if !min.less(n.Score) {
		return nil
	}
	return n
}

/*
 * return removed elements
 */
func (skiplist *skiplist) RemoveRangeByScore(min *ScoreBorder, max *ScoreBorder) (removed []*Element) {
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)
	// find target node (of target range) or last node of each level
	node := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil {
			if min.less(node.level[i].forward.Score) { //already in range
				break
			}
			node = node.level[i].forward
		}
		update[i] = node.level[i].forward
	}

	// node is the first one within range
	node = node.level[0].forward

	// remove nodes in range
	for node != nil {
		if !max.greater(node.Score) { // already out of range
			break
		}
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		skiplist.removeNode(node, update)
		node = next
	}
	return removed
}

// 1-based rank, include start, exclude stop.
// 删除操作可能一次删除多个节点
func (skiplist *skiplist) RemoveRangeByRank(start int64, stop int64) (removed []*Element) {
	var i int64 = 0 // rank of iterator 当前指针的排名
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)

	// scan from top level 从顶层向下寻找目标的先驱节点
	node := skiplist.header
	for level := skiplist.level - 1; level >= 0; level-- {
		for node.level[level].forward != nil && i + node.level[level].span < start {
			i += node.level[level].span
			node = node.level[level].forward
		}
		update[level] = node
	}

	i++
	node = node.level[0].forward // first node in range node 是目标范围内第一个节点

	// remove nodes in range 删除范围内的所有节点
	for node != nil && i < stop {
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		skiplist.removeNode(node, update)
		node = next
		i++
	}
	return removed
}