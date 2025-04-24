config:
  profile: {{ .Profile }}

datasets:
  prefix: {{ .HLQ }}.GRC

jobs:
  - name: {{ .JobName }}
    step: execute
    source: {{ .JobName }}.cbl
