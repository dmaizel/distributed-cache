package singleflight

import "sync"

// call represents a request that is in progress or has ended. Use sync.WaitGroup lock to avoid reentry.
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group is the main data structure of singleflight, which manages the requests (calls) of different keys.
type Group struct {
	mu sync.Mutex // protects m
	m  map[string]*call
}

// Do is responsible for making sure that for the same key, no matter how many times Do is called,
// the function fn will only be called once, waiting for the end of the fn call to return a return value or an error.
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}

	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.err
}
