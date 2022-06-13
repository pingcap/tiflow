package rpcerror

import "github.com/pingcap/errors"

type Prototype[E errorIface] struct {
	perror *errors.Error
}

func (p Prototype[E]) GenWithStack(e *E) error {
	return &normalizedError[E]{
		E:     *e,
		Error: p.perror.GenWithStack(),
	}
}

type normalizedError[E errorIface] struct {
	errors.Error
	E
}
