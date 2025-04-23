package utils

type GraceConfig struct {
	Config struct {
		Profile string `yaml:"profile"`
	} `yaml:"config"`

	Datasets struct {
		Prefix string `yaml:"prefix"`
	} `yaml:"datasets"`

	Jobs []struct {
		Name string `yaml:"name"`
		Step string `yaml:"step"`
		JCL  string `yaml:"jcl"`
	} `yaml:"jobs"`
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
