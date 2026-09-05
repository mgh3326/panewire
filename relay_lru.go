package panewire

import "container/list"

// lruIndex tracks recency separately from a value map. Callers keep their
// value map authoritative and remove the returned evicted key from it.
type lruIndex[K comparable] struct {
	entries map[K]*list.Element
	order   list.List
}

// touch marks key as most recently used and, if necessary, returns the least
// recently used key that no longer fits in maxEntries.
func (l *lruIndex[K]) touch(key K, maxEntries int) (already bool, evicted K, didEvict bool) {
	if element, exists := l.entries[key]; exists {
		l.order.MoveToFront(element)
		return true, evicted, false
	}
	if l.entries == nil {
		l.entries = make(map[K]*list.Element)
	}
	l.entries[key] = l.order.PushFront(key)
	if l.order.Len() <= maxEntries {
		return false, evicted, false
	}
	oldest := l.order.Back()
	evicted = oldest.Value.(K)
	delete(l.entries, evicted)
	l.order.Remove(oldest)
	return false, evicted, true
}

func (l *lruIndex[K]) forget(key K) {
	element, exists := l.entries[key]
	if !exists {
		return
	}
	delete(l.entries, key)
	l.order.Remove(element)
}
