package manager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterWorker(t *testing.T) {
	manager := NewWorkerManager()

	manager.RegisterWorker("worker-1")

	workers := manager.GetAllWorkers()
	assert.Len(t, workers, 1)
	assert.Equal(t, "worker-1", workers[0].WorkerId)
}

func TestRegisterMultipleWorkers(t *testing.T) {
	manager := NewWorkerManager()

	manager.RegisterWorker("worker-1")
	manager.RegisterWorker("worker-2")

	workers := manager.GetAllWorkers()

	assert.Len(t, workers, 2)
	assert.Equal(t, "worker-1", workers[0].WorkerId)
	assert.Equal(t, "worker-2", workers[1].WorkerId)
}

func TestUnregisterWorker(t *testing.T) {
	manager := NewWorkerManager()

	manager.RegisterWorker("worker-1")
	manager.RegisterWorker("worker-2")

	manager.UnregisterWorker("worker-1")

	workers := manager.GetAllWorkers()

	assert.Len(t, workers, 1)
	assert.Equal(t, "worker-2", workers[0].WorkerId)
}

func TestUpdateSlots(t *testing.T) {
	manager := NewWorkerManager()
	manager.RegisterWorker("worker-1")
	worker := manager.GetAvailableWorker()
	assert.Equal(t, int32(5), worker.AvailableSlots)

	manager.UpdateSlots(worker.WorkerId, 1)
	assert.Equal(t, int32(1), worker.AvailableSlots)
}

func TestUpdateHeartbeat(t *testing.T) {
	manager := NewWorkerManager()

	manager.RegisterWorker("worker-1")

	worker := manager.GetAvailableWorker()
	oldHeart := worker.LastHeartbeat

	time.Sleep(5 * time.Second)

	manager.UpdateHeartbeat(worker.WorkerId)

	worker = manager.GetAvailableWorker()
	assert.Equal(t, "worker-1", worker.WorkerId)
	assert.True(t, worker.LastHeartbeat.After(oldHeart))
}

func TestGetAvailableWorker(t *testing.T) {
	test := []struct {
		name        string
		setup       func(manager *WorkerManager)
		hasAvalable bool
	}{
		{
			name: "worker with available slots",
			setup: func(manager *WorkerManager) {
				manager.RegisterWorker("worker-1")
				manager.UpdateSlots("worker-1", 5)
			},
			hasAvalable: true,
		},
		{
			name: "worker with zero slots",
			setup: func(manager *WorkerManager) {
				manager.RegisterWorker("worker-1")
				manager.UpdateSlots("worker-1", 0)
			},
			hasAvalable: false,
		},
		{
			name: "prefers worker with more slots",
			setup: func(wm *WorkerManager) {
				wm.RegisterWorker("worker-1")
				wm.RegisterWorker("worker-2")
				wm.UpdateSlots("worker-1", 2)
				wm.UpdateSlots("worker-2", 5)
			},
			hasAvalable: true,
		},
	}

	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewWorkerManager()
			tt.setup(manager)
			worker := manager.GetAvailableWorker()
			if tt.hasAvalable {
				assert.NotNil(t, worker)
				assert.Greater(t, worker.AvailableSlots, int32(0))
			} else {
				assert.Nil(t, worker)
			}
		})
	}
}

func TestGetAllWorkers(t *testing.T) {
	manager := NewWorkerManager()

	manager.RegisterWorker("worker-1")
	manager.RegisterWorker("worker-2")
	manager.RegisterWorker("worker-3")

	workers := manager.GetAllWorkers()

	assert.Len(t, workers, 3)
}

func TestRemoveDeadWorkers(t *testing.T) {
	manager := NewWorkerManager()
	manager.RegisterWorker("worker-1")
	time.Sleep(30 * time.Second)

	manager.RemoveDeadWorkers()

	workers := manager.GetAllWorkers()
	assert.Len(t, workers, 0)
}

func TestWorkerInfoDefaults(t *testing.T) {
	wm := NewWorkerManager()
	wm.RegisterWorker("test-worker")

	worker := wm.GetAvailableWorker()

	require.NotNil(t, worker)
	assert.Equal(t, "test-worker", worker.WorkerId)
	assert.Equal(t, int32(5), worker.AvailableSlots)
	assert.Equal(t, int32(0), worker.TaskInProgress)
	assert.False(t, worker.LastHeartbeat.IsZero())
}
