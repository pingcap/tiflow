package config

import (
	"flag"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
)

func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("microcosom", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.configFile, "config", "", "Config file")
	return cfg
}

type Config struct {
	*flag.FlagSet

	TableNum int      `toml: "table-cnt"`
	Servers  []string `toml: "servers"`
	Timeout  int      `toml: "timeout"`

	configFile string
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	// Load config file if specified.
	if c.configFile != "" {
		err = c.configFromFile(c.configFile)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		return errors.New("please designate a config file")
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	return nil
}
