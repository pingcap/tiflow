package storagecfg

type Config struct {
	Local *LocalFileConfig `json:"local" toml:"local"`
}

type LocalFileConfig struct {
	BaseDir string `json:"base-dir" toml:"base-dir"`
}
