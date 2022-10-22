package wasmcore

import (
	"context"
	"os"

	"github.com/wapc/wapc-go"
	"github.com/wapc/wapc-go/engines/wasmtime"

	"go.uber.org/multierr"
)

type WasmPluginWrapper struct {
	ctx context.Context

	engine   wapc.Engine
	module   wapc.Module
	instance wapc.Instance
}

func NewWasmPlugin(guest []byte) (*WasmPluginWrapper, error) {
	ctx := context.Background()

	engine := wasmtime.Engine()

	module, err := engine.New(ctx, wapc.NoOpHostCallHandler, guest, &wapc.ModuleConfig{
		Logger: defaultLogger,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	})
	if err != nil {
		return nil, err
	}

	instance, err := module.Instantiate(ctx)
	if err != nil {
		return nil, err
	}

	p := &WasmPluginWrapper{
		ctx:      ctx,
		engine:   engine,
		module:   module,
		instance: instance,
	}
	return p, nil
}

func (w *WasmPluginWrapper) Close(ctx context.Context) error {
	var err error
	w.ctx.Done()
	err = multierr.Append(err, w.instance.Close(ctx))
	err = multierr.Append(err, w.module.Close(ctx))
	return err
}

func (w *WasmPluginWrapper) Verify() error {
	if err := w.verifyWasmModule(); err != nil {
		return err
	}
	return nil
}

func (w *WasmPluginWrapper) InvokeInstance(ctx context.Context, operation string, payload []byte) ([]byte, error) {
	return w.instance.Invoke(ctx, operation, payload)
}

// Verify whether all the guest functions are implemented.
func (w *WasmPluginWrapper) verifyWasmModule() error {
	return nil
}
