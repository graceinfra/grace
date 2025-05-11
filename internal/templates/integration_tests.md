# JCL Renderer Integration Tests

This document outlines the integration test scenarios for the JCL renderer component of the Grace project. These tests should be executed to verify the correct interaction between the JCL renderer and other components of the Grace system.

## Background

The JCL renderer supports different sources for JCL content as specified in the `jcl` field of a job in the `grace.yml` file:
1. No `jcl` field specified (Grace default templates)
2. `jcl: file://path/to/static.jcl` (a plain JCL file)
3. `jcl: file://path/to/template.jcl.tmpl` (a template JCL file using Go template variables)
4. `jcl: zos://MY.PDS.JCL(MEMBER)` (a JCL file stored in z/OS)

The integration tests should verify that each of these sources works correctly with the Grace commands (`deck`, `run`, `submit`).

## Test Scenarios

### 1. Default Grace Templates (No `jcl` field)

**Test Setup:**
```yaml
# grace.yml
config:
  profile: default
datasets:
  jcl: TEST.JCL
  src: TEST.SRC
  loadlib: TEST.LOADLIB
jobs:
  - name: TESTJOB
    type: compile
    inputs:
      - name: SYSIN
        path: src://hello.cbl
    outputs:
      - name: SYSLIN
        path: zos-temp://syslin
```

**Expected Behavior:**
- `grace deck` should generate a JCL file at `.grace/deck/TESTJOB.jcl` using the default compile template
- JCL should include correct DSN references for all inputs and outputs
- `grace run` should upload source files and submit the JCL
- `grace submit` should submit the job without recompiling

**Verification Steps:**
1. Run `grace deck` and verify the content of `.grace/deck/TESTJOB.jcl`
2. Check that template variables were correctly substituted
3. Verify the DD statements for inputs and outputs match the expected format
4. Run `grace run` and verify the job is submitted successfully
5. Check that the outputs are created with the expected names

### 2. Static JCL File (`file://path/to/static.jcl`)

**Test Setup:**
```yaml
# grace.yml
config:
  profile: default
datasets:
  jcl: TEST.JCL
  src: TEST.SRC
  loadlib: TEST.LOADLIB
jobs:
  - name: TESTJOB
    type: execute
    jcl: file://./static.jcl
    inputs:
      - name: INPUT1
        path: zos://EXISTING.DATA
    outputs:
      - name: OUTPUT1
        path: zos-temp://output
```

Create a file `static.jcl` in the same directory:
```
//TESTJOB JOB (ACCT),'TEST JOB',CLASS=A,MSGCLASS=A
//STEP1   EXEC PGM=IEFBR14
```

**Expected Behavior:**
- `grace deck` should copy `static.jcl` to `.grace/deck/TESTJOB.jcl` without template processing
- `grace run` should upload the JCL and submit it as-is
- `grace submit` should submit the job from the uploaded location

**Verification Steps:**
1. Run `grace deck` and verify the content of `.grace/deck/TESTJOB.jcl`
2. Confirm it's an exact copy of the static JCL without template processing
3. Run `grace run` and verify the job is submitted successfully

### 3. Template JCL File (`file://path/to/template.jcl.tmpl`)

**Test Setup:**
```yaml
# grace.yml
config:
  profile: default
datasets:
  jcl: TEST.JCL
  src: TEST.SRC
  loadlib: TEST.LOADLIB
jobs:
  - name: CUSTJOB
    type: execute
    jcl: file://./custom.jcl.tmpl
    program: TESTPGM
    inputs:
      - name: INPUT1
        path: zos://EXISTING.DATA
    outputs:
      - name: OUTPUT1
        path: zos-temp://output
```

Create a file `custom.jcl.tmpl` in the same directory:
```
//{{ .JobName | ToUpper }} JOB (ACCT),'CUSTOM JOB',CLASS=A,MSGCLASS=A
//STEP1    EXEC PGM={{ .ProgramName }}
{{ range .Inputs }}
//{{ .Name }} DD DSN={{ .DSN }},DISP={{ .DISP }}
{{ end }}
{{ range .Outputs }}
//{{ .Name }} DD DSN={{ .DSN }},DISP={{ .DISP }},
//             SPACE={{ .Space }},
//             DCB={{ .DCB }}
{{ end }}
```

**Expected Behavior:**
- `grace deck` should process the template and generate `.grace/deck/CUSTJOB.jcl`
- Template variables should be substituted with values from the job configuration
- Input and output sections should be populated with corresponding DD statements
- `grace run` should upload the processed JCL and submit it

**Verification Steps:**
1. Run `grace deck` and verify the content of `.grace/deck/CUSTJOB.jcl`
2. Verify that all template variables (`JobName`, `ProgramName`, etc.) are replaced
3. Check that the DD statements for inputs and outputs are generated correctly
4. Run `grace run` and verify the job is submitted successfully

### 4. JCL from z/OS (`zos://MY.PDS.JCL(MEMBER)`)

**Test Setup:**
```yaml
# grace.yml
config:
  profile: default
datasets:
  jcl: TEST.JCL
  src: TEST.SRC
  loadlib: TEST.LOADLIB
jobs:
  - name: ZOSJOB
    type: execute
    jcl: zos://TEST.JCL.LIBRARY(MEMBER)
    inputs:
      - name: INPUT1
        path: zos://EXISTING.DATA
    outputs:
      - name: OUTPUT1
        path: zos-temp://output
```

**Expected Behavior:**
- `grace deck` should fetch the JCL from `TEST.JCL.LIBRARY(MEMBER)` and use it as-is
- `grace run` should submit the job directly from the z/OS location
- `grace submit` should submit from the z/OS location without recompiling

**Verification Steps:**
1. Create or ensure `TEST.JCL.LIBRARY(MEMBER)` exists in z/OS
2. Run `grace deck` and verify it references the correct z/OS dataset
3. Run `grace run` and verify the job is submitted successfully from the z/OS location

## Additional Test Variations

### Test with `--no-compile` Flag
Run `grace run --no-compile` with each of the above configurations to verify that:
- Source files are not re-uploaded
- JCL is still correctly processed and submitted

### Test with `--no-upload` Flag
Run `grace run --no-upload` with each of the above configurations to verify that:
- Source files and JCL are not uploaded
- The job is still submitted from the correct location

## Implementation Notes

These integration tests should be run manually as they require access to a z/OS environment and interaction with multiple Grace commands. The results should be documented to ensure all scenarios function as expected.

For automated testing, consider creating mock implementations of the z/OS interactions that can simulate the behavior of a real z/OS system.