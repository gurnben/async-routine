package async

// nolint:AsyncManager

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/openshift-online/async-routine/opid"
)

type RoutineStatus string

const (
	RoutineStatusCreated                 RoutineStatus = "created"
	RoutineStatusRunning                 RoutineStatus = "running"
	RoutineStatusExceededTimebox         RoutineStatus = "running_exceeded_timebox"
	RoutineStatusFinished                RoutineStatus = "finished"
	RoutineStatusFinishedExceededTimebox RoutineStatus = "finished_exceeded_timebox"
)

var _ AsyncRoutine = (*asyncRoutine)(nil)

// AsyncRoutine is the public interface for managed async routines
//
//go:generate mockgen -source=async_routine.go -package=async -destination=mock_async_routine.go
type AsyncRoutine interface {
	Name() string
	CreatedAt() time.Time
	StartedAt() *time.Time
	FinishedAt() *time.Time
	Status() RoutineStatus
	OpId() string
	OriginatorOpId() string
	GetData() map[string]string

	run(manager AsyncRoutineManager)
	hasExceededTimebox() bool
	isFinished() bool
	isRunning() bool
	id() string
}

type asyncRoutine struct {
	mu               sync.RWMutex
	routineId        string
	name             string
	routine          func()
	routineWithError func() error
	createdAt        time.Time
	startedAt        *time.Time
	finishedAt       *time.Time
	timebox          *time.Duration
	errGroup         *errgroup.Group
	status           RoutineStatus
	ctx              context.Context
	originatorOpId   string
	data             map[string]string
}

func (r *asyncRoutine) Name() string {
	return r.name
}

func (r *asyncRoutine) CreatedAt() time.Time {
	return r.createdAt
}

func (r *asyncRoutine) StartedAt() *time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.startedAt
}

func (r *asyncRoutine) FinishedAt() *time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.finishedAt
}

func (r *asyncRoutine) Status() RoutineStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.status
}

func (r *asyncRoutine) OpId() string {
	return opid.FromContext(r.ctx)
}

func (r *asyncRoutine) OriginatorOpId() string {
	return r.originatorOpId
}

func (r *asyncRoutine) GetData() map[string]string {
	ret := map[string]string{}
	for k, v := range r.data {
		ret[k] = v
	}
	return ret
}

func (r *asyncRoutine) isStarted() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.startedAt != nil
}

func (r *asyncRoutine) isFinished() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.finishedAt != nil
}

func (r *asyncRoutine) isRunning() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isRunningLocked()
}

func (r *asyncRoutine) isRunningLocked() bool {
	return r.startedAt != nil && r.finishedAt == nil
}

func (r *asyncRoutine) hasExceededTimebox() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.isRunningLocked() && r.timebox != nil && time.Now().UTC().After(r.startedAt.Add(*r.timebox)) {
		r.status = RoutineStatusExceededTimebox
		return true
	}
	return false
}

func (r *asyncRoutine) id() string {
	return r.routineId
}

func (r *asyncRoutine) run(manager AsyncRoutineManager) {
	if r.isStarted() {
		return
	}

	if !manager.IsEnabled() {
		r.runUnmanaged()
		return
	}

	manager.register(r)
	now := time.Now().UTC()
	r.mu.Lock()
	r.startedAt = &now
	r.status = RoutineStatusRunning
	r.mu.Unlock()

	updateFinishedRoutine := func(r *asyncRoutine) {
		finishedAt := time.Now().UTC()
		r.mu.Lock()
		r.finishedAt = &finishedAt
		if r.status == RoutineStatusExceededTimebox {
			r.status = RoutineStatusFinishedExceededTimebox
		} else {
			r.status = RoutineStatusFinished
		}
		r.mu.Unlock()
		manager.deregister(r)
		manager.notify(func(observer RoutinesObserver) {
			observer.RoutineFinished(r)
		})
	}

	manager.notify(func(observer RoutinesObserver) {
		observer.RoutineStarted(r)
	})

	if r.errGroup != nil {
		r.errGroup.Go(func() error {
			err := r.routineWithError()
			updateFinishedRoutine(r)
			return err
		},
		)
		return
	}

	go func() {
		r.routine()
		updateFinishedRoutine(r)
	}()
}

func (r *asyncRoutine) runUnmanaged() {
	if r.errGroup != nil {
		r.errGroup.Go(r.routineWithError)
		return
	}

	go r.routine()
}
