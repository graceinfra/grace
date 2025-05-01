# Grace

**Grace** is a modern orchestration tool that lets you define and run hybrid
workflows across mainframes and cloud-native systems - all from a single
declarative spec.

With Grace, you can:
- Submit and monitor z/OS jobs using JCL, and COBOL
- Chain mainframe jobs with Python scripts, shell commands, API calls, or ETL
  flows
- Run workflows locally, or delegate them to a hosted [Hopper](https://github.com/graceinfra/hopper) runner for
  asynchronous execution
- Integrate with Slack, Teams, S3, or any external service via hooks or custom
  steps
- Use pluggable, language-agnostic [modules](https://github.com/graceinfra/grace-modules) that generate and execute logic -
  whether COBOL, Go, Python, Bash, or remote APIs

Grace treats the mainframe as just another execution backend - enabling
seamless integration into modern infrastructure without needing to live inside
the mainframe itself.
