package runtime

type Operator interface {
	Next(ctx *TaskContext, r *Record, idx int) ([]Chunk, bool, error)
	NextWantedInputIdx() int
	// Prepare is called when a job is submitted and going to run on an executor
	// Application can do any preparatory work in Prepare function, besides the
	// task resource unit will be initialized and returned from this function.
	Prepare(ctx *TaskContext) (TaskRescUnit, error)
	Pause() error
	Close() error
}
