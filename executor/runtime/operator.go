package runtime

type Operator interface {
	Next(ctx *TaskContext, r *Record, idx int) ([]Chunk, bool, error)
	NextWantedInputIdx() int
	Prepare(ctx *TaskContext) error
	Close() error
}
