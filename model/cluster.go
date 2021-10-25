package model

type ExecutorInfo struct {
	Name 			  string
	Addr 			  string
	LastHeartbeatTime int64
	Status            string
}

type JobInfo struct {
	JobType   string
	JobConfig string
	UserName  string // reserved field
}

//type TaskInfo struct {
//
//}