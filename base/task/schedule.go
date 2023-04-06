// Copyright 2021 gorse Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

// 注释：导入必要的包
import (
	"runtime" // 注释：Go 语言运行时库
	"sort"    // 注释：Go 语言内置的排序库
	"sync"    // 注释：Go 语言内置的同步库

	"github.com/samber/lo"                // 注释：日志库
	"github.com/zhenghaoz/gorse/base/log" // 注释：日志库
	"go.uber.org/zap"                     // 注释：日志库
	"modernc.org/mathutil"                // 注释：数学库
)

// 注释：JobsAllocator 类用于分配任务的 CPU 资源
type JobsAllocator struct {
	numJobs   int            // 注释：最大任务数量
	taskName  string         // 注释：任务名称
	scheduler *JobsScheduler // 注释：JobsScheduler 对象
}

// 注释：创建一个 JobsAllocator 对象
func NewConstantJobsAllocator(num int) *JobsAllocator {
	return &JobsAllocator{
		numJobs: num,
	}
}

// 注释：获取最大任务数量
func (allocator *JobsAllocator) MaxJobs() int {
	if allocator == nil || allocator.numJobs < 1 {
		// Return 1 for invalid allocator
		return 1
	}
	return allocator.numJobs
}

// 注释：获取可用的 CPU 资源数量
func (allocator *JobsAllocator) AvailableJobs(tracker *Task) int {
	if allocator == nil || allocator.numJobs < 1 {
		// Return 1 for invalid allocator
		return 1
	} else if allocator.scheduler != nil {
		// Use jobs scheduler
		return allocator.scheduler.allocateJobsForTask(allocator.taskName, true, tracker)
	}
	return allocator.numJobs
}

// 注释：请求分配 CPU 资源
func (allocator *JobsAllocator) Init() {
	if allocator.scheduler != nil {
		allocator.scheduler.allocateJobsForTask(allocator.taskName, true, nil)
	}
}

// 注释：taskInfo 表示 JobsScheduler 中的任务
type taskInfo struct {
	name       string // 注释：任务名称
	priority   int    // 注释：任务优先级
	privileged bool   // 注释：是否为特权任务
	jobs       int    // 注释：已分配的 CPU 资源数量
	previous   int    // 注释：上一次已分配的 CPU 资源数量
}

// 注释：JobsScheduler 类用于分配任务的 CPU 资源
type JobsScheduler struct {
	*sync.Cond                      // 注释：同步库的条件变量
	numJobs    int                  // 注释：CPU 资源总数
	freeJobs   int                  // 注释：可用的 CPU 资源数量
	tasks      map[string]*taskInfo // 注释：任务列表
}

// 注释：创建一个 JobsScheduler 对象
func NewJobsScheduler(num int) *JobsScheduler {
	if num <= 0 {
		// Use all cores if num is less than 1.
		num = runtime.NumCPU()
	}
	return &JobsScheduler{
		Cond:     sync.NewCond(&sync.Mutex{}),
		numJobs:  num,
		freeJobs: num,
		tasks:    make(map[string]*taskInfo),
	}
}

// 注释：注册一个任务
func (s *JobsScheduler) Register(taskName string, priority int, privileged bool) bool {
	s.L.Lock()
	defer s.L.Unlock()
	if _, exits := s.tasks[taskName]; !exits {
		s.tasks[taskName] = &taskInfo{name: taskName, priority: priority, privileged: privileged}
		return true
	} else {
		return false
	}
}

// 注释：注销一个任务
func (s *JobsScheduler) Unregister(taskName string) {
	s.L.Lock()
	defer s.L.Unlock()
	if task, exits := s.tasks[taskName]; exits {
		// Return allocated jobs.
		s.freeJobs += task.jobs
		delete(s.tasks, taskName)
		s.Broadcast()
	}
}

// 注释：获取 JobsAllocator 对象
func (s *JobsScheduler) GetJobsAllocator(taskName string) *JobsAllocator {
	return &JobsAllocator{
		numJobs:   s.numJobs,
		taskName:  taskName,
		scheduler: s,
	}
}

// 注释：为任务分配 CPU 资源
func (s *JobsScheduler) allocateJobsForTask(taskName string, block bool, tracker *Task) int {
	// Find current task and return the jobs temporarily.
	s.L.Lock()
	currentTask, exist := s.tasks[taskName]
	if !exist {
		panic("task not found")
	}
	s.freeJobs += currentTask.jobs
	currentTask.jobs = 0
	s.L.Unlock()

	s.L.Lock()
	defer s.L.Unlock()
	for {
		s.allocateJobsForAll()
		if currentTask.jobs == 0 && block {
			tracker.Suspend(true)
			if currentTask.previous > 0 {
				log.Logger().Debug("suspend task", zap.String("task", currentTask.name))
				s.Broadcast()
			}
			s.Wait()
		} else {
			tracker.Suspend(false)
			if currentTask.previous == 0 {
				log.Logger().Debug("resume task", zap.String("task", currentTask.name))
			}
			return currentTask.jobs
		}
	}
}

// 注释：为所有任务分配 CPU 资源
func (s *JobsScheduler) allocateJobsForAll() {
	// Separate privileged tasks and normal tasks
	privileged := make([]*taskInfo, 0)
	normal := make([]*taskInfo, 0)
	for _, task := range s.tasks {
		if task.privileged {
			privileged = append(privileged, task)
		} else {
			normal = append(normal, task)
		}
	}

	var tasks []*taskInfo
	if len(privileged) > 0 {
		tasks = privileged
	} else {
		tasks = normal
	}

	// allocate jobs
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].priority > tasks[j].priority
	})
	for i, task := range tasks {
		if s.freeJobs == 0 {
			return
		}
		targetJobs := s.numJobs/len(tasks) + lo.If(i < s.numJobs%len(tasks), 1).Else(0)
		targetJobs = mathutil.Min(targetJobs, s.freeJobs)
		if task.jobs < targetJobs {
			if task.previous != targetJobs {
				log.Logger().Debug("reallocate jobs for task",
					zap.String("task", task.name),
					zap.Int("previous_jobs", task.previous),
					zap.Int("target_jobs", targetJobs))
			}
			s.freeJobs -= targetJobs - task.jobs
			task.jobs = targetJobs
			task.previous = task.jobs
		}
	}
}
