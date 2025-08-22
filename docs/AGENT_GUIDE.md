# Agent Development Guide

This guide outlines the development rules and best practices that AI agents must follow when contributing to the Altus DoorLoop Sync Backend repository.

## Core Development Rules

### 1. Branch Protection - Never Push to Main
- **Never push directly to the `main` branch**
- All changes must go through pull requests
- Use feature branches with descriptive names
- Follow the branch naming convention: `feature/description` or `fix/description`

### 2. Small Scoped Diffs
- Keep changes focused on a single concern or issue
- Avoid large, sweeping changes that touch multiple unrelated areas
- Break down complex changes into smaller, reviewable PRs
- Each PR should have a clear, single purpose

### 3. Tests Required
- All code changes must include appropriate tests
- Unit tests are required for new functions and methods
- Integration tests should be added for API endpoints and database operations
- Manual testing should be documented in the PR
- Test coverage should not decrease with new changes

### 4. No Frontend Hardcoded Counts
- Never hardcode numerical values in frontend code
- Use configuration files or API responses for dynamic values
- Count-based logic should be derived from actual data, not assumptions
- Pagination and limits should be configurable

### 5. OIDC Deploy Only
- All deployments must follow OpenID Connect (OIDC) authentication patterns
- No hardcoded secrets or API keys in deployment workflows
- Use environment variables and secure secret management
- Follow the existing GitHub Actions OIDC workflows in `.github/workflows/`

## Code Quality Standards

### Documentation
- Update relevant documentation for any code changes
- Include clear commit messages following conventional commit format
- Add inline comments for complex business logic

### Error Handling
- Implement proper error handling and logging
- Use the existing error handling patterns in the codebase
- Ensure graceful degradation for external API failures

### Security
- Follow secure coding practices
- Never commit secrets or sensitive data
- Use the existing security patterns in `doorloop_sync/security/`

## Pull Request Guidelines

### Before Submitting
1. Run the existing health checks: `python sync_health_check.py`
2. Ensure all tests pass
3. Verify no linting errors
4. Test manually if applicable

### PR Description
- Use the provided PR template
- Fill out all relevant sections
- Include screenshots for UI changes
- Reference related issues or tickets

## Getting Help

If you're unsure about any of these rules or need clarification on implementation patterns, review the existing codebase for examples or ask for guidance in the PR comments.

Remember: These rules exist to maintain code quality, security, and reliability of the DoorLoop sync pipeline that handles critical financial and property management data.