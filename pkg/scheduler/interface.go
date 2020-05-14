package scheduler

// TransportTable 代表一次迁移表操作
type TransportTable struct {
	FromCaptureID string
	ToCaptureID   string
	TableID       int64
}

type Scheduler interface {
	// 更新指定 capture 的负载情况
	SetWorkloads(captureID string, workloads map[int64]uint64)
	// 计算偏斜度
	Skewness() float64
	// 计算调度方案，当调度后偏斜量达到 targetSkewness，或无法继续调度后返回；
	// 返回值是调度后的偏斜量和迁移表操作
	CalRebalanceOperates(targetSkewness float64) (float64, []*TransportTable, error)
}
