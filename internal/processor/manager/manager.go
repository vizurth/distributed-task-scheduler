package manager

import (
	"sync"
	"time"
)

type WorkerInfo struct {
	WorkerId       string
	AvailableSlots int32
	LastHeartbeat  time.Time
	TaskInProgress int32
}

type WorkerManager struct {
	mu      sync.RWMutex
	workers map[string]*WorkerInfo
}

func NewWorkerManager() *WorkerManager {
	return &WorkerManager{
		workers: make(map[string]*WorkerInfo),
	}
}

func (wm *WorkerManager) RegisterWorker(workerId string) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	wm.workers[workerId] = &WorkerInfo{
		WorkerId:       workerId,
		AvailableSlots: 5,
		LastHeartbeat:  time.Now(),
		TaskInProgress: 0,
	}
}

func (wm *WorkerManager) UnregisterWorker(workerId string) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	delete(wm.workers, workerId)
}

func (wm *WorkerManager) UpdateSlots(workerId string, slots int32) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	if _, ok := wm.workers[workerId]; ok {
		wm.workers[workerId].AvailableSlots = slots
	}
}

func (wm *WorkerManager) UpdateHeartbeat(workerID string) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if w, exists := wm.workers[workerID]; exists {
		w.LastHeartbeat = time.Now()
	}
}

func (wm *WorkerManager) GetAvailableWorker() *WorkerInfo {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	var bestWorker *WorkerInfo
	for _, worker := range wm.workers {
		if worker.AvailableSlots > 0 {
			if bestWorker == nil || worker.AvailableSlots > bestWorker.AvailableSlots {
				bestWorker = worker
			}
		}
	}

	return bestWorker
}

func (wm *WorkerManager) GetAllWorkers() []*WorkerInfo {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	var workers []*WorkerInfo
	for _, w := range wm.workers {
		workers = append(workers, w)
	}
	return workers
}

func (wm *WorkerManager) RemoveDeadWorkers() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	now := time.Now()
	for id, w := range wm.workers {
		if now.Sub(w.LastHeartbeat) > 30*time.Second {
			delete(wm.workers, id)
		}
	}
}
