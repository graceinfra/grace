jobs:
  - name: {{ .JobName }}
    step: execute
    jcl: job-template.jcl
    params:
      - name: EXAMPLE
        value: hello
