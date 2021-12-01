package runtime

type Operator interface {
	Next(ctx *TaskContext, r *Record, idx int) ([]Chunk, bool, error)
	NextWantedInputIdx() int
	Prepare() error
	Close() error
}
