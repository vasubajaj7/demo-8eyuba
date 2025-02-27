# Pull Request Description

<!-- Provide a clear and concise description of the changes made in this PR -->

## Type of Change

Please check the type of change your PR introduces:

- [ ] Migration (Airflow 1.x to 2.x compatibility update)
- [ ] Bug fix
- [ ] New feature
- [ ] Documentation update
- [ ] Test improvement
- [ ] Performance improvement
- [ ] Security enhancement
- [ ] Dependencies update
- [ ] Other (please describe):

## Migration Checklist

<!-- Skip this section if not applicable to your changes -->

- [ ] Updated imports to Airflow 2.X package structure
- [ ] Replaced deprecated operators/sensors/hooks with new versions
- [ ] Applied TaskFlow API where appropriate
- [ ] Tested in Cloud Composer 2 environment
- [ ] Verified backwards compatibility with Composer 1 (if required during transition)
- [ ] Updated documentation with migration-specific notes

## Standard Checklist

- [ ] Code follows the project's style guidelines
- [ ] Linting passes without warnings (`pylint`)
- [ ] Code formatting passes (`black`)
- [ ] Added or updated unit tests
- [ ] All tests pass locally and in CI
- [ ] Documentation has been updated (if applicable)
- [ ] Security considerations have been addressed
- [ ] Performance impact has been assessed

## Related Issues

<!-- Please link to any related issues here using the syntax: Fixes #issue_number -->

## Testing

<!-- Describe the testing process you've gone through -->

- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Migration tests added/updated
- [ ] End-to-end tests performed

## Reviewer Notes

<!-- Note any specific areas that need special attention from reviewers -->

## Deployment Notes

<!-- Any special considerations for deploying these changes -->

## Screenshots/Logs

<!-- If applicable, add screenshots or logs to help explain your changes -->