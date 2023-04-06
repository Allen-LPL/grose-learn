// Copyright 2020 gorse Project Authors
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

package master

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/ReneKroon/ttlcache/v2"
	"github.com/emicklei/go-restful/v3"
	"github.com/juju/errors"
	"github.com/zhenghaoz/gorse/base"
	"github.com/zhenghaoz/gorse/base/encoding"
	"github.com/zhenghaoz/gorse/base/log"
	"github.com/zhenghaoz/gorse/base/parallel"
	"github.com/zhenghaoz/gorse/base/task"
	"github.com/zhenghaoz/gorse/config"
	"github.com/zhenghaoz/gorse/model/click"
	"github.com/zhenghaoz/gorse/model/ranking"
	"github.com/zhenghaoz/gorse/protocol"
	"github.com/zhenghaoz/gorse/server"
	"github.com/zhenghaoz/gorse/storage/cache"
	"github.com/zhenghaoz/gorse/storage/data"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// ScheduleState 是一个结构体，用于记录任务调度的状态
type ScheduleState struct {
	IsRunning   bool      `json:"is_running"`   // 是否正在运行
	SearchModel bool      `json:"search_model"` // 是否在搜索最优模型
	StartTime   time.Time `json:"start_time"`   // 任务开始的时间
}

// Master is the master node.
// Master 是一个结构体，用于管理一个推荐系统的服务端
type Master struct {
	protocol.UnimplementedMasterServer              // 实现了 MasterServer 接口，用于处理来自客户端的请求
	server.RestServer                               // 继承了 RestServer 类型，用于提供 RESTful API
	grpcServer                         *grpc.Server // 一个 gRPC 服务器，用于与客户端通信

	taskMonitor   *task.Monitor       // 一个任务监控器，用于追踪和记录任务的进度和状态
	jobsScheduler *task.JobsScheduler // 一个任务调度器，用于分配和执行任务
	cacheFile     string              // 缓存文件的路径，用于存储和加载数据和模型
	managedMode   bool                // 是否是托管模式，如果是，则会自动加载数据和训练模

	// cluster meta cache
	ttlCache       *ttlcache.Cache  // 一个有过期时间的缓存，用于存储一些临时数据
	nodesInfo      map[string]*Node // 一个映射，用于存储节点的信息，键是节点的 ID，值是节点的结构体
	nodesInfoMutex sync.RWMutex     // 一个读写锁，用于保护 nodesInfo 的并发访问

	// ranking dataset
	rankingTrainSet  *ranking.DataSet // 一个排名模型的训练集，用于存储用户和物品的交互数据
	rankingTestSet   *ranking.DataSet // 一个排名模型的测试集，用于评估模型的效果
	rankingDataMutex sync.RWMutex     // 一个读写锁，用于保护 rankingTrainSet 和 rankingTestSet 的并发访问

	// click dataset
	clickTrainSet  *click.Dataset // 一个点击模型的训练集，用于存储用户和物品的点击数据
	clickTestSet   *click.Dataset // 一个点击模型的测试集，用于评估模型的效果
	clickDataMutex sync.RWMutex   // 一个读写锁，用于保护 clickTrainSet 和 clickTestSet 的并发访问

	// ranking model
	rankingModelName     string                 // 排名模型的名称，目前只支持 bpr（贝叶斯个性化排序）
	rankingScore         ranking.Score          // 排名模型的评分指标，包括准确率、召回率、覆盖率等
	rankingModelMutex    sync.RWMutex           // 一个读写锁，用于保护 rankingScore 的并发访问
	rankingModelSearcher *ranking.ModelSearcher // 一个排名模型搜索器，用于寻找最优的模型参数

	// click model
	clickScore         click.Score          // 点击模型的评分指标，包括 AUC、LogLoss 等
	clickModelMutex    sync.RWMutex         // 一个读写锁，用于保护 clickScore 的并发访问
	clickModelSearcher *click.ModelSearcher // 一个点击模型搜索器，用于寻找最优的模型参数

	localCache *LocalCache // 一个本地缓存，用于存储一些常用数据，如用户和物品的 ID、名称等

	// events
	fitTicker    *time.Ticker               // 一个定时器，用于定期执行训练任务
	importedChan *parallel.ConditionChannel // feedback inserted events 一个条件通道，用于等待反馈数据导入完成
	loadDataChan *parallel.ConditionChannel // dataset loaded events 一个条件通道，用于等待数据导入完成
	triggerChan  *parallel.ConditionChannel // manually trigger events 一个条件通道，用于触发任务调度

	scheduleState         ScheduleState    // 一个结构体，用于记录任务调度的状态
	workerScheduleHandler http.HandlerFunc // 一个 HTTP 处理器，用于处理来自 worker 的任务调度请求
}

// NewMaster creates a master node.
// NewMaster 函数用于创建一个 Master 结构体
func NewMaster(cfg *config.Config, cacheFile string, managedMode bool) *Master {
	rand.Seed(time.Now().UnixNano())
	// setup trace provider
	// 设置追踪提供者, 用于追踪监控和记录程序的运行状态和日志
	tp, err := cfg.Tracing.NewTracerProvider()
	if err != nil {
		log.Logger().Fatal("failed to create trace provider", zap.Error(err))
	}
	otel.SetTracerProvider(tp)                                                                                              // 设置追踪提供者
	otel.SetErrorHandler(log.GetErrorHandler())                                                                             // 设置错误处理器
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})) // 设置文件映射传播器，用于在不同服务之间传播上下文信息， 如 trace id
	// create task monitor
	taskMonitor := task.NewTaskMonitor() // 创建一个任务监控器
	for _, taskName := range []string{   // 遍历所有的任务名称
		TaskLoadDataset,            // 加载数据集任务
		TaskFindItemNeighbors,      // 寻找物品邻居任务
		TaskFindUserNeighbors,      // 寻找用户邻居任务
		TaskFitRankingModel,        // 训练排名模型任务
		TaskFitClickModel,          // 训练点击模型任务
		TaskSearchRankingModel,     // 搜索排名模型任务
		TaskSearchClickModel,       // 搜索点击模型任务
		TaskCacheGarbageCollection, // 缓存垃圾回收任务
	} {
		taskMonitor.Pending(taskName) // 将每个任务标记为待处理状态
	}
	return &Master{
		nodesInfo: make(map[string]*Node),
		// create task monitor
		cacheFile:     cacheFile,
		managedMode:   managedMode,
		taskMonitor:   taskMonitor,
		jobsScheduler: task.NewJobsScheduler(cfg.Master.NumJobs),
		// default ranking model
		rankingModelName: "bpr",
		rankingModelSearcher: ranking.NewModelSearcher(
			cfg.Recommend.Collaborative.ModelSearchEpoch,
			cfg.Recommend.Collaborative.ModelSearchTrials,
			cfg.Recommend.Collaborative.EnableModelSizeSearch,
		),
		// default click model
		clickModelSearcher: click.NewModelSearcher(
			cfg.Recommend.Collaborative.ModelSearchEpoch,
			cfg.Recommend.Collaborative.ModelSearchTrials,
			cfg.Recommend.Collaborative.EnableModelSizeSearch,
		),
		RestServer: server.RestServer{
			Settings: &config.Settings{
				Config:       cfg,
				CacheClient:  cache.NoDatabase{},
				DataClient:   data.NoDatabase{},
				RankingModel: ranking.NewBPR(nil),
				ClickModel:   click.NewFM(click.FMClassification, nil),
				// init versions
				RankingModelVersion: rand.Int63(),
				ClickModelVersion:   rand.Int63(),
			},
			HttpHost:   cfg.Master.HttpHost,
			HttpPort:   cfg.Master.HttpPort,
			WebService: new(restful.WebService),
		},
		fitTicker:    time.NewTicker(cfg.Recommend.Collaborative.ModelFitPeriod),
		importedChan: parallel.NewConditionChannel(),
		loadDataChan: parallel.NewConditionChannel(),
		triggerChan:  parallel.NewConditionChannel(),
	}
}

// Serve starts the master node.
func (m *Master) Serve() {

	// load local cached model
	var err error
	m.localCache, err = LoadLocalCache(m.cacheFile)
	if err != nil {
		if errors.Is(err, errors.NotFound) {
			log.Logger().Info("no local cache found, create a new one", zap.String("path", m.cacheFile))
		} else {
			log.Logger().Error("failed to load local cache", zap.String("path", m.cacheFile), zap.Error(err))
		}
	}
	if m.localCache.RankingModel != nil {
		log.Logger().Info("load cached ranking model",
			zap.String("model_name", m.localCache.RankingModelName),
			zap.String("model_version", encoding.Hex(m.localCache.RankingModelVersion)),
			zap.Float32("model_score", m.localCache.RankingModelScore.NDCG),
			zap.Any("params", m.localCache.RankingModel.GetParams()))
		m.RankingModel = m.localCache.RankingModel
		m.rankingModelName = m.localCache.RankingModelName
		m.RankingModelVersion = m.localCache.RankingModelVersion
		m.rankingScore = m.localCache.RankingModelScore
		CollaborativeFilteringPrecision10.Set(float64(m.rankingScore.Precision))
		CollaborativeFilteringRecall10.Set(float64(m.rankingScore.Recall))
		CollaborativeFilteringNDCG10.Set(float64(m.rankingScore.NDCG))
		MemoryInUseBytesVec.WithLabelValues("collaborative_filtering_model").Set(float64(m.RankingModel.Bytes()))
	}
	if m.localCache.ClickModel != nil {
		log.Logger().Info("load cached click model",
			zap.String("model_version", encoding.Hex(m.localCache.ClickModelVersion)),
			zap.Float32("model_score", m.localCache.ClickModelScore.Precision),
			zap.Any("params", m.localCache.ClickModel.GetParams()))
		m.ClickModel = m.localCache.ClickModel
		m.clickScore = m.localCache.ClickModelScore
		m.ClickModelVersion = m.localCache.ClickModelVersion
		RankingPrecision.Set(float64(m.clickScore.Precision))
		RankingRecall.Set(float64(m.clickScore.Recall))
		RankingAUC.Set(float64(m.clickScore.AUC))
		MemoryInUseBytesVec.WithLabelValues("ranking_model").Set(float64(m.ClickModel.Bytes()))
	}

	// create cluster meta cache
	m.ttlCache = ttlcache.NewCache()
	m.ttlCache.SetExpirationCallback(m.nodeDown)
	m.ttlCache.SetNewItemCallback(m.nodeUp)
	if err = m.ttlCache.SetTTL(m.Config.Master.MetaTimeout + 10*time.Second); err != nil {
		log.Logger().Fatal("failed to set TTL", zap.Error(err))
	}

	// connect data database
	m.DataClient, err = data.Open(m.Config.Database.DataStore, m.Config.Database.DataTablePrefix)
	if err != nil {
		log.Logger().Fatal("failed to connect data database", zap.Error(err),
			zap.String("database", log.RedactDBURL(m.Config.Database.DataStore)))
	}
	if err = m.DataClient.Init(); err != nil {
		log.Logger().Fatal("failed to init database", zap.Error(err))
	}

	// connect cache database
	m.CacheClient, err = cache.Open(m.Config.Database.CacheStore, m.Config.Database.CacheTablePrefix)
	if err != nil {
		log.Logger().Fatal("failed to connect cache database", zap.Error(err),
			zap.String("database", log.RedactDBURL(m.Config.Database.CacheStore)))
	}
	if err = m.CacheClient.Init(); err != nil {
		log.Logger().Fatal("failed to init database", zap.Error(err))
	}

	m.RestServer.HiddenItemsManager = server.NewHiddenItemsManager(&m.RestServer)
	m.RestServer.PopularItemsCache = server.NewPopularItemsCache(&m.RestServer)

	if m.managedMode {
		go m.RunManagedTasksLoop()
	} else {
		go m.RunPrivilegedTasksLoop()
		log.Logger().Info("start model fit", zap.Duration("period", m.Config.Recommend.Collaborative.ModelFitPeriod))
		go m.RunRagtagTasksLoop()
		log.Logger().Info("start model searcher", zap.Duration("period", m.Config.Recommend.Collaborative.ModelSearchPeriod))
	}

	// start rpc server
	go func() {
		log.Logger().Info("start rpc server",
			zap.String("host", m.Config.Master.Host),
			zap.Int("port", m.Config.Master.Port))
		lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", m.Config.Master.Host, m.Config.Master.Port))
		if err != nil {
			log.Logger().Fatal("failed to listen", zap.Error(err))
		}
		m.grpcServer = grpc.NewServer(grpc.MaxSendMsgSize(math.MaxInt))
		protocol.RegisterMasterServer(m.grpcServer, m)
		if err = m.grpcServer.Serve(lis); err != nil {
			log.Logger().Fatal("failed to start rpc server", zap.Error(err))
		}
	}()

	// start http server
	m.StartHttpServer()
}

func (m *Master) Shutdown() {
	// stop http server
	err := m.HttpServer.Shutdown(context.TODO())
	if err != nil {
		log.Logger().Error("failed to shutdown http server", zap.Error(err))
	}
	// stop grpc server
	m.grpcServer.GracefulStop()
}

func (m *Master) RunPrivilegedTasksLoop() {
	defer base.CheckPanic()
	var (
		err   error
		tasks = []Task{
			NewFitClickModelTask(m),
			NewFitRankingModelTask(m),
			NewFindUserNeighborsTask(m),
			NewFindItemNeighborsTask(m),
		}
		firstLoop = true
	)
	go func() {
		m.importedChan.Signal()
		for {
			if m.checkDataImported() {
				m.importedChan.Signal()
			}
			time.Sleep(time.Second)
		}
	}()
	for {
		select {
		case <-m.fitTicker.C:
		case <-m.importedChan.C:
		}

		// download dataset
		err = m.runLoadDatasetTask()
		if err != nil {
			log.Logger().Error("failed to load ranking dataset", zap.Error(err))
			continue
		}
		if m.rankingTrainSet.UserCount() == 0 && m.rankingTrainSet.ItemCount() == 0 && m.rankingTrainSet.Count() == 0 {
			log.Logger().Warn("empty ranking dataset",
				zap.Strings("positive_feedback_type", m.Config.Recommend.DataSource.PositiveFeedbackTypes))
			continue
		}

		if firstLoop {
			m.loadDataChan.Signal()
			firstLoop = false
		}

		var registeredTask []Task
		for _, t := range tasks {
			if m.jobsScheduler.Register(t.name(), t.priority(), true) {
				registeredTask = append(registeredTask, t)
			}
		}
		for _, t := range registeredTask {
			go func(task Task) {
				j := m.jobsScheduler.GetJobsAllocator(task.name())
				defer m.jobsScheduler.Unregister(task.name())
				j.Init()
				if err := task.run(j); err != nil {
					log.Logger().Error("failed to run task", zap.String("task", task.name()), zap.Error(err))
					return
				}
			}(t)
		}
	}
}

// RunRagtagTasksLoop searches optimal recommendation model in background. It never modifies variables other than
// rankingModelSearcher, clickSearchedModel and clickSearchedScore.
func (m *Master) RunRagtagTasksLoop() {
	defer base.CheckPanic()
	<-m.loadDataChan.C
	var (
		err   error
		tasks = []Task{
			NewCacheGarbageCollectionTask(m),
			NewSearchRankingModelTask(m),
			NewSearchClickModelTask(m),
		}
	)
	for {
		if m.rankingTrainSet == nil || m.clickTrainSet == nil {
			time.Sleep(time.Second)
			continue
		}
		var registeredTask []Task
		for _, t := range tasks {
			if m.jobsScheduler.Register(t.name(), t.priority(), false) {
				registeredTask = append(registeredTask, t)
			}
		}
		for _, t := range registeredTask {
			go func(task Task) {
				defer m.jobsScheduler.Unregister(task.name())
				j := m.jobsScheduler.GetJobsAllocator(task.name())
				j.Init()
				if err = task.run(j); err != nil {
					log.Logger().Error("failed to run task", zap.String("task", task.name()), zap.Error(err))
					m.taskMonitor.Fail(task.name(), err.Error())
				}
			}(t)
		}
		time.Sleep(m.Config.Recommend.Collaborative.ModelSearchPeriod)
	}
}

func (m *Master) RunManagedTasksLoop() {
	var (
		privilegedTasks = []Task{
			NewFitClickModelTask(m),
			NewFitRankingModelTask(m),
			NewFindUserNeighborsTask(m),
			NewFindItemNeighborsTask(m),
		}
		ragtagTasks = []Task{
			NewCacheGarbageCollectionTask(m),
			NewSearchRankingModelTask(m),
			NewSearchClickModelTask(m),
		}
	)

	for range m.triggerChan.C {
		func() {
			defer base.CheckPanic()

			searchModel := m.scheduleState.SearchModel
			m.scheduleState.IsRunning = true
			m.scheduleState.StartTime = time.Now()
			defer func() {
				m.scheduleState.IsRunning = false
				m.scheduleState.SearchModel = false
				m.scheduleState.StartTime = time.Time{}
			}()
			_ = searchModel

			// download dataset
			if err := m.runLoadDatasetTask(); err != nil {
				log.Logger().Error("failed to load ranking dataset", zap.Error(err))
				return
			}
			if m.rankingTrainSet.UserCount() == 0 && m.rankingTrainSet.ItemCount() == 0 && m.rankingTrainSet.Count() == 0 {
				log.Logger().Warn("empty ranking dataset",
					zap.Strings("positive_feedback_type", m.Config.Recommend.DataSource.PositiveFeedbackTypes))
				return
			}

			var registeredTask []Task
			for _, t := range privilegedTasks {
				if m.jobsScheduler.Register(t.name(), t.priority(), true) {
					registeredTask = append(registeredTask, t)
				}
			}
			if searchModel {
				for _, t := range ragtagTasks {
					if m.jobsScheduler.Register(t.name(), t.priority(), false) {
						registeredTask = append(registeredTask, t)
					}
				}
			}

			var wg sync.WaitGroup
			wg.Add(len(registeredTask))
			for _, t := range registeredTask {
				go func(task Task) {
					j := m.jobsScheduler.GetJobsAllocator(task.name())
					defer m.jobsScheduler.Unregister(task.name())
					defer wg.Done()
					j.Init()
					if err := task.run(j); err != nil {
						log.Logger().Error("failed to run task", zap.String("task", task.name()), zap.Error(err))
						return
					}
				}(t)
			}
			wg.Wait()
		}()
	}
}

func (m *Master) checkDataImported() bool {
	ctx := context.Background()
	isDataImported, err := m.CacheClient.Get(ctx, cache.Key(cache.GlobalMeta, cache.DataImported)).Integer()
	if err != nil {
		if !errors.Is(err, errors.NotFound) {
			log.Logger().Error("failed to read meta", zap.Error(err))
		}
		return false
	}
	if isDataImported > 0 {
		err = m.CacheClient.Set(ctx, cache.Integer(cache.Key(cache.GlobalMeta, cache.DataImported), 0))
		if err != nil {
			log.Logger().Error("failed to write meta", zap.Error(err))
		}
		return true
	}
	return false
}

func (m *Master) notifyDataImported() {
	ctx := context.Background()
	err := m.CacheClient.Set(ctx, cache.Integer(cache.Key(cache.GlobalMeta, cache.DataImported), 1))
	if err != nil {
		log.Logger().Error("failed to write meta", zap.Error(err))
	}
}
