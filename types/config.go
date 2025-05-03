package types

type OutputStyle int

const (
	StyleHuman OutputStyle = iota
	StyleHumanVerbose
	StyleMachineJSON
)

type GraceConfig struct {
	Config struct {
		Profile     string `yaml:"profile"`
		Concurrency int    `yaml:"concurrency"`
		Defaults    struct {
			Compiler Compiler `yaml:"compiler,omitempty"`
			Linker   Linker   `yaml:"linker,omitempty"`
		} `yaml:"defaults,omitempty"`
	} `yaml:"config"`

	Datasets Datasets `yaml:"datasets"`

	Jobs []*Job `yaml:"jobs"`
}

type Compiler struct {
	Pgm     string `yaml:"pgm,omitempty"`
	Parms   string `yaml:"parms,omitempty"`
	StepLib string `yaml:"steplib,omitempty"`
}
type Linker struct {
	Pgm     string `yaml:"pgm,omitempty"`
	Parms   string `yaml:"parms,omitempty"`
	StepLib string `yaml:"steplib,omitempty"`
}

type Datasets struct {
	JCL     string `yaml:"jcl"`
	SRC     string `yaml:"src"`
	LoadLib string `yaml:"loadlib"`
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
