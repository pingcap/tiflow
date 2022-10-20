package wasmcore

import "github.com/labstack/gommon/log"

func defaultLogger(message string) {
	log.Infof(message)
}
