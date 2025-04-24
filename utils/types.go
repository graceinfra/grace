package utils

type GraceConfig struct {
	Config struct {
		Profile string `yaml:"profile"`
	} `yaml:"config"`

	Datasets struct {
		Prefix string `yaml:"prefix"`
	} `yaml:"datasets"`

	Jobs []Job `yaml:"jobs"`
}

type Job struct {
	Name     string `yaml:"name"`
	Step     string `yaml:"step"`
	Source   string `yaml:"source"`
	Template string `yaml:"template,omitempty"`
	Wait     bool   `yaml:"wait,omitempty"`
	View     string `yaml:"view"`
	Retries  int    `yaml:"retries"`
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
