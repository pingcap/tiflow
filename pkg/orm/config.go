package orm

import "time"

// TODO: split the config file
const (
	DefaultFrameMetaEndpoints = "127.0.0.1:3336"
	DefaultFrameMetaUser      = "root"
	DefaultFrameMetaPassword  = "123456"
)

const (
	defaultConnMaxIdleTime = 30 * time.Second
	defaultConnMaxLifeTime = 12 * time.Hour
	defaultMaxIdleConns    = 3
	defaultMaxOpenConns    = 10
	defaultReadTimeout     = "3s"
	defaultWriteTimeout    = "3s"
	defaultDialTimeout     = "3s"
	// TODO: more params for mysql connection
)

// refer to: https://pkg.go.dev/database/sql#SetConnMaxIdleTime
type DBConfig struct {
	ReadTimeout     string
	WriteTimeout    string
	DialTimeout     string
	ConnMaxIdleTime time.Duration
	ConnMaxLifeTime time.Duration
	MaxIdleConns    int
	MaxOpenConns    int
}

func NewDefaultDBConfig() DBConfig {
	return DBConfig{
		ReadTimeout:     defaultReadTimeout,
		WriteTimeout:    defaultWriteTimeout,
		DialTimeout:     defaultDialTimeout,
		ConnMaxIdleTime: defaultConnMaxIdleTime,
		ConnMaxLifeTime: defaultConnMaxLifeTime,
		MaxIdleConns:    defaultMaxIdleConns,
		MaxOpenConns:    defaultMaxOpenConns,
	}
}
