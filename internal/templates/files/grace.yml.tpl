config:
  profile: {{ .Profile }}

datasets:
  prefix: {{ .HLQ }}.GRC
  loadlib: {{ .HLQ }}.LOAD

jobs:
  - name: {{ .JobName }}
    step: execute
    source: {{ .JobName }}.cbl
