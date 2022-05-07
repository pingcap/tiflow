package config

import (
	"time"
)

// Define some constants
const (
	ServerMasterEtcdDialTimeout  = 5 * time.Second
	ServerMasterEtcdSyncInterval = 3 * time.Second
)
