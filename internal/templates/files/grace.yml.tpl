config:
  profile: {{ .Profile }}

datasets:
  pds: {{ .HLQ }}.{{ .WorkspaceName }}.GRC
  loadlib: {{ .HLQ }}.LOAD

jobs:
  - name: {{ .JobName }}
    step: execute
    source: {{ .JobName }}.cbl
