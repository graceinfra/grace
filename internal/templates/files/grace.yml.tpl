jobs:
  - name: {{ .JobName }}
    step: execute
    jcl: {{ .JobName }}.jcl
    params:
      - name: EXAMPLE
        value: hello
