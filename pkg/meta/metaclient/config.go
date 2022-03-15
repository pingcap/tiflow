package metaclient

type Config struct {
	// Endpoints is a list of URLs.
	Endpoints []string `json:"endpoints"`
	Auth      AuthConf
	Log       LogConf
}

type AuthConf struct {
	// [TODO] TLS holds the client secure credentials, if any.

	// Username is a user name for authentication.
	Username string `json:"username"`

	// Password is a password for authentication.
	Password string `json:"password"`
}

type LogConf struct {
	File  string
	Level int
}

func (cf *Config) Clone() *Config {
	newConf := *cf
	return &newConf
}
