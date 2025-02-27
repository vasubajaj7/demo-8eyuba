---
name: Bug Report
about: Report a bug or issue with the Cloud Composer 2 migration project
title: '[BUG] '
labels: ['bug', 'triage']
assignees: []
---

## Bug Description
A clear and concise description of what the bug is.

## Environment Information
Please provide the following information:
- **Environment:** (Development/QA/Production)
- **Airflow Version:** (1.10.15 / 2.X)
- **Cloud Composer Version:** (1 / 2)
- **DAG ID:** (if applicable)
- **Task ID:** (if applicable)
- **Component Type:** (DAG/Operator/Hook/Sensor/Connection/Plugin/Infrastructure)

## Reproduction Steps
Steps to reproduce the behavior:
1. Go to '...'
2. Click on '...'
3. Scroll down to '...'
4. See error

Alternatively, provide a minimal code example that reproduces the issue.

```python
# Code example here
```

## Expected Behavior
A clear and concise description of what you expected to happen.

## Actual Behavior
A clear and concise description of what actually happened. If applicable, include error messages, stack traces, or logs.

```
Error logs here
```

## Migration Related
- [ ] This bug is specifically related to the Airflow 1.x to 2.x migration
- [ ] This bug only appears in Airflow 2.x / Cloud Composer 2
- [ ] This bug existed in Airflow 1.10.15 / Cloud Composer 1 as well

## Affected Components
- [ ] DAG Parsing
- [ ] Task Execution
- [ ] Scheduler
- [ ] Webserver/UI
- [ ] API
- [ ] Database/Metadata
- [ ] Authentication/Authorization
- [ ] Operators/Hooks
- [ ] Infrastructure/Deployment
- [ ] CI/CD Pipeline
- [ ] Other (please specify)

## Possible Solution
If you have a suggestion for how to fix the bug, please describe it here.

## Workaround
If you've found a temporary workaround, please describe it here.

## Relevant Documentation
Links to relevant documentation, such as:
- [Migration Guide](src/backend/docs/migration_guide.md)
- [Operator Migration](src/backend/docs/operator_migration.md)
- [Airflow 2.X Documentation](https://airflow.apache.org/docs/apache-airflow/2.0.0/)

## Screenshots
If applicable, add screenshots to help explain your problem.

## Additional Context
Add any other context about the problem here, such as:
- When did the issue start occurring?
- Is it intermittent or consistent?
- Any recent changes that might have caused this?
- Related issues or PRs?

## Impact Level
- [ ] Critical: Production system is down or unusable
- [ ] High: Major functionality is impacted, no workaround available
- [ ] Medium: Some functionality impacted, workaround available
- [ ] Low: Minimal impact, not affecting core functionality