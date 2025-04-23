config:
  profile: {{ .Profile }}

datasets:
  prefix: {{ .HLQ }}.GRC

jobs:
  - name: {{ .JobName }}
    step: execute
    jcl: {{ .JobName }}.jcl
