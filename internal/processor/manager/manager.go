package manager

import (
	"sync"
	"time"

	"github.com/vizurth/distributed-task-scheduler/internal/constants"
)

// WorkerInfo содержит информацию о воркере
type WorkerInfo struct {
	WorkerId       string
	AvailableSlots int32
	LastHeartbeat  time.Time
	TaskInProgress int32
}

// WorkerManager управляет воркерами в системе
type WorkerManager struct {
	mu      sync.RWMutex
	workers map[string]*WorkerInfo
}

// NewWorkerManager создает новый менеджер воркеров
func NewWorkerManager() *WorkerManager {
	return &WorkerManager{
		workers: make(map[string]*WorkerInfo),
	}
}

// RegisterWorker регистрирует нового воркера в системе
func (wm *WorkerManager) RegisterWorker(workerId string) {
	if workerId == "" {
		return
	}

	wm.mu.Lock()
	defer wm.mu.Unlock()

	wm.workers[workerId] = &WorkerInfo{
		WorkerId:       workerId,
		AvailableSlots: constants.DefaultWorkerSlots,
		LastHeartbeat:  time.Now(),
		TaskInProgress: 0,
	}
}

// UnregisterWorker удаляет воркера из системы
func (wm *WorkerManager) UnregisterWorker(workerId string) {
	if workerId == "" {
		return
	}

	wm.mu.Lock()
	defer wm.mu.Unlock()

	delete(wm.workers, workerId)
}

// UpdateSlots обновляет количество доступных слотов у воркера
func (wm *WorkerManager) UpdateSlots(workerId string, slots int32) {
	if workerId == "" {
		return
	}

	wm.mu.Lock()
	defer wm.mu.Unlock()

	if worker, ok := wm.workers[workerId]; ok {
		worker.AvailableSlots = slots

		// Вычисляем количество задач в работе
		if slots < constants.DefaultWorkerSlots {
			worker.TaskInProgress = constants.DefaultWorkerSlots - slots
		} else {
			worker.TaskInProgress = 0
		}
	}
}

// UpdateHeartbeat обновляет время последнего heartbeat от воркера
func (wm *WorkerManager) UpdateHeartbeat(workerID string) {
	if workerID == "" {
		return
	}

	wm.mu.Lock()
	defer wm.mu.Unlock()

	if w, exists := wm.workers[workerID]; exists {
		w.LastHeartbeat = time.Now()
	}
}

// GetAvailableWorker возвращает воркера с максимальным количеством доступных слотов
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

// GetAllWorkers возвращает копию списка всех воркеров
func (wm *WorkerManager) GetAllWorkers() []*WorkerInfo {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	workers := make([]*WorkerInfo, 0, len(wm.workers))
	for _, w := range wm.workers {
		// Создаем копию, чтобы избежать race conditions
		workerCopy := &WorkerInfo{
			WorkerId:       w.WorkerId,
			AvailableSlots: w.AvailableSlots,
			LastHeartbeat:  w.LastHeartbeat,
			TaskInProgress: w.TaskInProgress,
		}
		workers = append(workers, workerCopy)
	}

	return workers
}

// RemoveDeadWorkers удаляет воркеров, от которых долго не было heartbeat
func (wm *WorkerManager) RemoveDeadWorkers() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	now := time.Now()
	for id, w := range wm.workers {
		if now.Sub(w.LastHeartbeat) > constants.WorkerDeadTimeout {
			delete(wm.workers, id)
		}
	}
}

// GetActiveWorkersCount возвращает количество активных воркеров
func (wm *WorkerManager) GetActiveWorkersCount() int {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	return len(wm.workers)
}

// GetWorkerInfo возвращает информацию о конкретном воркере
func (wm *WorkerManager) GetWorkerInfo(workerID string) (*WorkerInfo, bool) {
	if workerID == "" {
		return nil, false
	}

	wm.mu.RLock()
	defer wm.mu.RUnlock()

	worker, exists := wm.workers[workerID]
	if !exists {
		return nil, false
	}

	// Возвращаем копию
	workerCopy := &WorkerInfo{
		WorkerId:       worker.WorkerId,
		AvailableSlots: worker.AvailableSlots,
		LastHeartbeat:  worker.LastHeartbeat,
		TaskInProgress: worker.TaskInProgress,
	}

	return workerCopy, true
}

// GetTotalAvailableSlots возвращает общее количество доступных слотов во всех воркерах
func (wm *WorkerManager) GetTotalAvailableSlots() int32 {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	var total int32
	for _, worker := range wm.workers {
		total += worker.AvailableSlots
	}

	return total
}
