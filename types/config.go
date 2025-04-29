package types

type GraceConfig struct {
	Config struct {
		Profile string `yaml:"profile"`
	} `yaml:"config"`

	Datasets struct {
		JCL     string `yaml:"jcl"`
		SRC     string `yaml:"src"`
		LoadLib string `yaml:"loadlib"`
	} `yaml:"datasets"`

	Jobs []*Job `yaml:"jobs"`
}

type ZoweConfig struct {
	Schema    string                 `json:"$schema"`
	Profiles  map[string]ZoweProfile `json:"profiles"`
	Defaults  map[string]string      `json:"defaults"`
	AutoStore bool                   `json:"autoStore"`
}

type ZoweProfile struct {
	Type       string         `json:"type"`
	Properties map[string]any `json:"properties"`
	Secure     []string       `json:"secure"`
}

type OutputStyle int

const (
	StyleHuman OutputStyle = iota
	StyleHumanVerbose
	StyleMachineJSON
)
