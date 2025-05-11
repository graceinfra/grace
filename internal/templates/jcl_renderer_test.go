package templates

import (
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/jcl"
	"github.com/graceinfra/grace/types"
)

func TestPrepareJCLTemplateData(t *testing.T) {
	// Test cases
	tests := []struct {
		name          string
		ctx           *context.ExecutionContext
		job           *types.Job
		expectedData  *JCLTemplateData
		expectedError bool
	}{
		{
			name: "Basic job with inputs and outputs",
			ctx: &context.ExecutionContext{
				WorkflowId: uuid.MustParse("12345678-1234-1234-1234-123456789012"),
				ResolvedPaths: map[string]string{
					"zos-temp://output1": "TEST.GRC.HC0FFEE.OUT1",
				},
				Config: &types.GraceConfig{
					Config: struct {
						Profile     string "yaml:\"profile\""
						Concurrency int    "yaml:\"concurrency\""
						Defaults    struct {
							Compiler types.Compiler "yaml:\"compiler,omitempty\""
							Linker   types.Linker   "yaml:\"linker,omitempty\""
						} "yaml:\"defaults,omitempty\""
						Cleanup types.CleanupSettings "yaml:\"cleanup,omitempty\""
					}{
						Defaults: struct {
							Compiler types.Compiler "yaml:\"compiler,omitempty\""
							Linker   types.Linker   "yaml:\"linker,omitempty\""
						}{
							Compiler: types.Compiler{
								Pgm:     "IGYCRCTL",
								Parms:   "LIST,RENT",
								StepLib: "IGY.V4R2M0.SIGYCOMP",
							},
							Linker: types.Linker{
								Pgm:     "IEWBLINK",
								Parms:   "LIST,RENT",
								StepLib: "SYS1.LINKLIB",
							},
						},
					},
					Datasets: types.Datasets{
						SRC:     "TEST.SRC",
						LoadLib: "TEST.LOADLIB",
					},
				},
			},
			job: &types.Job{
				Name:    "TESTJOB",
				Type:    "compile",
				Program: stringPtr("MYPGM"),
				Inputs: []types.FileSpec{
					{
						Name: "INPUT1",
						Path: "src://program.cbl",
					},
					{
						Name: "INPUT2",
						Path: "zos://EXISTING.DATASET",
						Disp: "OLD",
					},
				},
				Outputs: []types.FileSpec{
					{
						Name:  "OUT1",
						Path:  "zos-temp://output1",
						Space: "(CYL,(10,5))",
						DCB:   "(RECFM=FB,LRECL=80,BLKSIZE=32720)",
						Keep:  true,
					},
				},
			},
			expectedData: &JCLTemplateData{
				JobName:         "TESTJOB",
				WorkflowId:      "12345678-1234-1234-1234-123456789012",
				Type:            "compile",
				ProgramName:     "MYPGM",
				LoadLib:         "TEST.LOADLIB",
				CompilerPgm:     "IGYCRCTL",
				CompilerParms:   "LIST,RENT",
				CompilerSteplib: "IGY.V4R2M0.SIGYCOMP",
				LinkerPgm:       "IEWBLINK",
				LinkerParms:     "LIST,RENT",
				LinkerSteplib:   "SYS1.LINKLIB",
				// DDStatements is mocked in the test
				Inputs: []ResolvedDD{
					{
						Name: "INPUT1",
						DSN:  "TEST.SRC(PROGRAM)",
						DISP: jcl.DefaultInputDISP,
					},
					{
						Name: "INPUT2",
						DSN:  "EXISTING.DATASET",
						DISP: "OLD",
					},
				},
				Outputs: []ResolvedDD{
					{
						Name:     "OUT1",
						DSN:      "TEST.GRC.HC0FFEE.OUT1",
						DISP:     "(NEW,CATLG,CATLG)",
						Space:    "(CYL,(10,5))",
						DCB:      "(RECFM=FB,LRECL=80,BLKSIZE=32720)",
						IsOutput: true,
					},
				},
			},
			expectedError: false,
		},
		{
			name: "Job with default output settings",
			ctx: &context.ExecutionContext{
				WorkflowId: uuid.MustParse("12345678-1234-1234-1234-123456789012"),
				ResolvedPaths: map[string]string{
					"zos-temp://default-output": "TEST.GRC.HD3F4UL.OUT1",
				},
				Config: &types.GraceConfig{
					Config: struct {
						Profile     string "yaml:\"profile\""
						Concurrency int    "yaml:\"concurrency\""
						Defaults    struct {
							Compiler types.Compiler "yaml:\"compiler,omitempty\""
							Linker   types.Linker   "yaml:\"linker,omitempty\""
						} "yaml:\"defaults,omitempty\""
						Cleanup types.CleanupSettings "yaml:\"cleanup,omitempty\""
					}{
						Defaults: struct {
							Compiler types.Compiler "yaml:\"compiler,omitempty\""
							Linker   types.Linker   "yaml:\"linker,omitempty\""
						}{
							Compiler: types.Compiler{
								Pgm: "IGYCRCTL",
							},
						},
					},
					Datasets: types.Datasets{
						SRC: "TEST.SRC",
					},
				},
			},
			job: &types.Job{
				Name: "DEFAULTJOB",
				Type: "compile",
				Outputs: []types.FileSpec{
					{
						Name: "OUT1",
						Path: "zos-temp://default-output",
						// No custom Space, DCB, or Keep settings
					},
				},
			},
			expectedData: &JCLTemplateData{
				JobName:         "DEFAULTJOB",
				WorkflowId:      "12345678-1234-1234-1234-123456789012",
				Type:            "compile",
				CompilerPgm:     "IGYCRCTL",
				Outputs: []ResolvedDD{
					{
						Name:     "OUT1",
						DSN:      "TEST.GRC.HD3F4UL.OUT1",
						DISP:     jcl.DefaultOutputDISP,
						Space:    jcl.DefaultOutputSpace,
						DCB:      jcl.DefaultOutputDCB,
						IsOutput: true,
					},
				},
			},
			expectedError: false,
		},
		{
			name: "Job with file:// paths",
			ctx: &context.ExecutionContext{
				WorkflowId: uuid.MustParse("12345678-1234-1234-1234-123456789012"),
				ConfigDir:  "/path/to/project",
				ResolvedPaths: map[string]string{
					"file://output.txt": "output.txt",
				},
				Config: &types.GraceConfig{
					Config: struct {
						Profile     string "yaml:\"profile\""
						Concurrency int    "yaml:\"concurrency\""
						Defaults    struct {
							Compiler types.Compiler "yaml:\"compiler,omitempty\""
							Linker   types.Linker   "yaml:\"linker,omitempty\""
						} "yaml:\"defaults,omitempty\""
						Cleanup types.CleanupSettings "yaml:\"cleanup,omitempty\""
					}{},
				},
			},
			job: &types.Job{
				Name: "FILEJOB",
				Type: "shell",
				Inputs: []types.FileSpec{
					{
						Name: "IN1",
						Path: "file://input.txt",
					},
				},
				Outputs: []types.FileSpec{
					{
						Name: "OUT1",
						Path: "file://output.txt",
					},
				},
			},
			expectedData: &JCLTemplateData{
				JobName:    "FILEJOB",
				WorkflowId: "12345678-1234-1234-1234-123456789012",
				Type:       "shell",
				Inputs: []ResolvedDD{
					{
						Name: "IN1",
						DSN:  "/path/to/project/input.txt",
						DISP: jcl.DefaultInputDISP,
					},
				},
				Outputs: []ResolvedDD{
					{
						Name:     "OUT1",
						DSN:      "/path/to/project/output.txt",
						DISP:     jcl.DefaultOutputDISP,
						Space:    jcl.DefaultOutputSpace,
						DCB:      jcl.DefaultOutputDCB,
						IsOutput: true,
					},
				},
			},
			expectedError: false,
		},
	}

	// Mock the jcl.GenerateDDStatements function
	originalGenerateDDStatements := jcl.GenerateDDStatements
	defer func() { jcl.GenerateDDStatements = originalGenerateDDStatements }()
	jcl.GenerateDDStatements = func(job *types.Job, ctx *context.ExecutionContext) (string, error) {
		return "//MOCK DD DSN=MOCK.DATASET,DISP=SHR", nil
	}

	// Run tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call the function under test
			result, err := PrepareJCLTemplateData(tt.ctx, tt.job)

			// Check error condition
			if (err != nil) != tt.expectedError {
				t.Errorf("PrepareJCLTemplateData() error = %v, expectedError %v", err, tt.expectedError)
				return
			}

			if err != nil {
				return
			}

			// Check DDStatements separately, as we've mocked it
			if result.DDStatements != "//MOCK DD DSN=MOCK.DATASET,DISP=SHR" {
				t.Errorf("DDStatements not set correctly, got: %s", result.DDStatements)
			}

			// Set it to match for the deep comparison
			result.DDStatements = tt.expectedData.DDStatements

			// Check fields that might be nil in the test case
			if tt.expectedData.ProgramName == "" {
				result.ProgramName = ""
			}
			if tt.expectedData.LoadLib == "" {
				result.LoadLib = ""
			}
			if tt.expectedData.CompilerParms == "" {
				result.CompilerParms = ""
			}
			if tt.expectedData.CompilerSteplib == "" {
				result.CompilerSteplib = ""
			}
			if tt.expectedData.LinkerPgm == "" {
				result.LinkerPgm = ""
			}
			if tt.expectedData.LinkerParms == "" {
				result.LinkerParms = ""
			}
			if tt.expectedData.LinkerSteplib == "" {
				result.LinkerSteplib = ""
			}

			// Compare result with expected data
			if !reflect.DeepEqual(result, tt.expectedData) {
				t.Errorf("PrepareJCLTemplateData() = %+v, expected %+v", result, tt.expectedData)
			}
		})
	}
}

func TestRenderJCL(t *testing.T) {
	tests := []struct {
		name           string
		templateName   string
		templateContent string
		data           *JCLTemplateData
		expected       string
		expectedError  bool
	}{
		{
			name:           "Basic template substitution",
			templateName:   "test.jcl.tmpl",
			templateContent: "//{{ .JobName }} JOB\n//STEP EXEC PGM={{ .ProgramName }}",
			data: &JCLTemplateData{
				JobName:     "TESTJOB",
				ProgramName: "TESTPGM",
			},
			expected:      "//TESTJOB JOB\n//STEP EXEC PGM=TESTPGM",
			expectedError: false,
		},
		{
			name:           "Template with ToUpper function",
			templateName:   "upper.jcl.tmpl",
			templateContent: "//{{ .JobName | ToUpper }} JOB",
			data: &JCLTemplateData{
				JobName: "testjob",
			},
			expected:      "//TESTJOB JOB",
			expectedError: false,
		},
		{
			name:           "Template with Default function",
			templateName:   "default.jcl.tmpl",
			templateContent: "//STEP EXEC PGM={{ Default \"DEFAULTPGM\" .ProgramName }}",
			data: &JCLTemplateData{
				// ProgramName not set, should use default
			},
			expected:      "//STEP EXEC PGM=DEFAULTPGM",
			expectedError: false,
		},
		{
			name:           "Default function with provided value",
			templateName:   "default_with_value.jcl.tmpl",
			templateContent: "//STEP EXEC PGM={{ Default \"DEFAULTPGM\" .ProgramName }}",
			data: &JCLTemplateData{
				ProgramName: "MYPGM",
			},
			expected:      "//STEP EXEC PGM=MYPGM",
			expectedError: false,
		},
		{
			name:           "Template with empty string uses default",
			templateName:   "default_empty.jcl.tmpl",
			templateContent: "//STEP EXEC PGM={{ Default \"DEFAULTPGM\" .ProgramName }}",
			data: &JCLTemplateData{
				ProgramName: "", // Empty string should use default
			},
			expected:      "//STEP EXEC PGM=DEFAULTPGM",
			expectedError: false,
		},
		{
			name:           "Complex template with Inputs and Outputs iteration",
			templateName:   "complex.jcl.tmpl",
			templateContent: `//{{ .JobName }} JOB
//STEP EXEC PGM={{ .ProgramName }}
{{ range .Inputs }}//{{ .Name }} DD DSN={{ .DSN }},DISP={{ .DISP }}
{{ end }}
{{ range .Outputs }}//{{ .Name }} DD DSN={{ .DSN }},DISP={{ .DISP }},
//             SPACE={{ .Space }},
//             DCB={{ .DCB }}
{{ end }}`,
			data: &JCLTemplateData{
				JobName:     "CMPLXJOB",
				ProgramName: "MYPROGRAM",
				Inputs: []ResolvedDD{
					{
						Name: "IN1",
						DSN:  "TEST.INPUT1",
						DISP: "SHR",
					},
				},
				Outputs: []ResolvedDD{
					{
						Name:  "OUT1",
						DSN:   "TEST.OUTPUT1",
						DISP:  "(NEW,CATLG,DELETE)",
						Space: "(CYL,(10,5))",
						DCB:   "(RECFM=FB,LRECL=80)",
					},
				},
			},
			expected: `//CMPLXJOB JOB
//STEP EXEC PGM=MYPROGRAM
//IN1 DD DSN=TEST.INPUT1,DISP=SHR

//OUT1 DD DSN=TEST.OUTPUT1,DISP=(NEW,CATLG,DELETE),
//             SPACE=(CYL,(10,5)),
//             DCB=(RECFM=FB,LRECL=80)
`,
			expectedError: false,
		},
		{
			name:           "Invalid template syntax",
			templateName:   "invalid.jcl.tmpl",
			templateContent: "//{{ .JobName } JOB", // Missing closing brace
			data: &JCLTemplateData{
				JobName: "TESTJOB",
			},
			expected:      "",
			expectedError: true,
		},
		{
			name:           "Reference to non-existent field",
			templateName:   "nonexistent.jcl.tmpl",
			templateContent: "//{{ .NonExistentField }} JOB",
			data: &JCLTemplateData{
				JobName: "TESTJOB",
			},
			expected:      "// JOB", // Go templates silently render empty for non-existent fields
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := RenderJCL(tt.templateName, tt.templateContent, tt.data)

			// Check error condition
			if (err != nil) != tt.expectedError {
				t.Errorf("RenderJCL() error = %v, expectedError %v", err, tt.expectedError)
				return
			}

			if err != nil {
				return
			}

			// Compare result with expected output
			if result != tt.expected {
				t.Errorf("RenderJCL() =\n%s\n, expected =\n%s", result, tt.expected)
			}
		})
	}
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}