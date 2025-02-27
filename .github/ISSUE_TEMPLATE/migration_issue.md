---
name: Migration Issue
about: Report a specific issue related to the Airflow 1.x to 2.x migration on Cloud Composer
title: '[MIGRATION] '
labels: ['migration', 'triage']
assignees: []
---

## Issue Description
A clear and concise description of the migration issue encountered.

## Environment Information
Please provide the following information:
- **Source Environment:** (Cloud Composer 1 version/details)
- **Target Environment:** (Cloud Composer 2 version/details)
- **Source Airflow Version:** (e.g., 1.10.15)
- **Target Airflow Version:** (e.g., 2.2.3)
- **Migration Stage:** (Planning/Code Migration/Database Migration/Testing/Deployment/Post-Migration)
- **Component Type:** (DAG/Operator/Hook/Sensor/Connection/Plugin/Infrastructure/Database)

## Migration Steps Performed
List the migration steps you've performed that led to this issue:
1. 
2. 
3. 

Include relevant command-line commands if applicable:
```bash
# Commands used
```

## Expected Outcome
A clear and concise description of what you expected to happen during/after the migration step.

## Actual Outcome
A clear and concise description of what actually happened. Include error messages, stack traces, or logs.

```
Error logs or output here
```

## Code Samples
If applicable, provide before and after code samples illustrating the issue.

**Before (Airflow 1.x):**
```python
# Original code
```

**After (Airflow 2.x):**
```python
# Migrated code with issue
```

## Issue Category
- [ ] Import Path Changes
- [ ] Operator/Hook API Changes
- [ ] DAG Loading/Parsing
- [ ] Database Schema
- [ ] Execution Behavior
- [ ] UI/Web Interface
- [ ] Authentication/Authorization
- [ ] Performance
- [ ] Infrastructure/Deployment
- [ ] Configuration
- [ ] Dependencies/Requirements
- [ ] TaskFlow API Conversion
- [ ] Custom Plugin Compatibility
- [ ] Other (please specify)

## Migration Components Affected
- [ ] DAG Code
- [ ] Operators
- [ ] Hooks
- [ ] Sensors
- [ ] Connections
- [ ] Variables
- [ ] XComs
- [ ] Plugins
- [ ] Database
- [ ] Scheduler
- [ ] Web Server
- [ ] Worker
- [ ] Infrastructure
- [ ] CI/CD Pipeline
- [ ] Testing Framework
- [ ] Documentation
- [ ] Other (please specify)

## Attempted Solutions
Describe any steps you've already taken to address this issue:

```python
# Solution attempts if applicable
```

## Migration Documentation References
List any documentation or resources you've consulted:

- [Migration Guide](src/backend/docs/migration_guide.md)
- [Operator Migration](src/backend/docs/operator_migration.md)
- [Apache Airflow Upgrade Documentation](https://airflow.apache.org/docs/apache-airflow/stable/upgrading-from-1-10/index.html)
- [Google Cloud Composer Migration Guide](https://cloud.google.com/composer/docs/composer-2/composer-1-to-2-migration)

## Workaround
If you've found a temporary workaround, please describe it here.

```python
# Workaround code if applicable
```

## Impact Assessment
- [ ] Critical: Blocks migration for entire system/component
- [ ] High: Significant functionality impacted, no workaround
- [ ] Medium: Partial functionality affected, workaround available
- [ ] Low: Minor issue, easily addressable

Describe the business impact of this issue if not resolved:

## Additional Context
Add any other context about the problem here, such as:
- Does this issue occur consistently or intermittently?
- Does it affect all DAGs or only specific ones?
- Are there any patterns to when/how the issue occurs?
- Any specific workflows or use cases particularly affected?

## Screenshots
If applicable, add screenshots to help explain your problem.