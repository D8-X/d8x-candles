package sportsclient

import (
	"sync"
	"time"
)

type Window struct {
	Elem      map[string]int64
	Mux       sync.RWMutex
	MaxAgeSec int64
	LastFlush int64
}

func NewWindow(maxAgeSec int64) *Window {
	return &Window{
		Elem:      make(map[string]int64),
		Mux:       sync.RWMutex{},
		MaxAgeSec: maxAgeSec,
		LastFlush: time.Now().Unix(),
	}
}

func (w *Window) FlushOld() {
	w.Mux.Lock()
	defer w.Mux.Unlock()
	thresh := time.Now().Unix() - w.MaxAgeSec
	for name, ts := range w.Elem {
		if ts < thresh {
			delete(w.Elem, name)
		}
	}
	w.LastFlush = time.Now().Unix()
}

func (w *Window) Exists(name string) bool {
	w.Mux.RLock()
	_, exists := w.Elem[name]
	w.Mux.RUnlock()
	if time.Now().Unix()-w.LastFlush > 5*60 {
		w.FlushOld()
	}
	return exists
}

// AddElement adds an element
func (w *Window) AddElement(name string) {
	if w.Exists(name) {
		return
	}
	w.Mux.Lock()
	defer w.Mux.Unlock()
	w.Elem[name] = time.Now().Unix()
}
