package storagecfg

// Config defines configurations for a external storage resource
type Config struct {
	Local *LocalFileConfig `json:"local" toml:"local"`
}

// LocalFileConfig defines configurations for a local file based resource
type LocalFileConfig struct {
	BaseDir string `json:"base-dir" toml:"base-dir"`
}
