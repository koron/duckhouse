// Package syncmap provides sync.Map that supports generic types.
package syncmap

import "sync"

type Map[K comparable, V any] struct {
	m  map[K]V
	rw sync.RWMutex
}

func (m *Map[K, V]) rlock() {
	m.rw.RLock()
}

func (m *Map[K, V]) runlock() {
	m.rw.RUnlock()
}

func (m *Map[K, V]) lock() {
	m.rw.Lock()
}

func (m *Map[K, V]) unlock() {
	m.rw.Unlock()
}

func (m *Map[K, V]) load(key K) (V, bool) {
	v, has := m.m[key]
	return v, has
}

func (m *Map[K, V]) store(key K, value V) {
	if m.m == nil {
		m.m = map[K]V{}
	}
	m.m[key] = value
}

func (m *Map[K, V]) delete(key K) {
	delete(m.m, key)
}

func (m *Map[K, V]) Load(key K) (V, bool) {
	m.rlock()
	v, has := m.load(key)
	m.runlock()
	return v, has
}

func (m *Map[K, V]) Store(key K, value V) {
	m.lock()
	m.store(key, value)
	m.unlock()
}

func (m *Map[K, V]) Delete(key K) {
	m.lock()
	m.delete(key)
	m.unlock()
}

func (m *Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	m.lock()
	defer m.unlock()
	if actual, has := m.load(key); has {
		return actual, true
	}
	m.store(key, value)
	return value, false
}

func (m *Map[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	m.lock()
	value, loaded = m.load(key)
	m.delete(key)
	m.unlock()
	return value, loaded
}

func (m *Map[K, V]) Range(f func(K, V) bool) {
	m.rlock()
	for k, v := range m.m {
		if !f(k, v) {
			break
		}
	}
	m.runlock()
}
