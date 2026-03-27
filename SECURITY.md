# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| Latest  | :white_check_mark: |

We recommend always using the latest version of ukpyn for the best security updates and fixes.

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly:

1. **Do NOT** open a public GitHub issue for security vulnerabilities
2. Email the UK Power Networks team with details of the vulnerability
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Any suggested fixes (optional)

We aim to acknowledge reports within 48 hours and will work with you to understand and address the issue promptly.

## Security Practices

### API Key Management

- **NEVER** commit real API keys or secrets to the repository
- Use environment variables for all sensitive configuration
- Copy `.env-example` to `.env` and add your own keys locally
- The `.env` file is gitignored and must never be committed

### For Contributors

- Do not hardcode credentials, API keys, or secrets in source code
- Do not include real API keys in tests, examples, or documentation
- Use placeholder values like `your_api_key_here` in examples
- Review your commits before pushing to ensure no secrets are included

### Dependency Security

- Dependencies are regularly reviewed for known vulnerabilities
- We use automated dependency scanning tools
- Update dependencies promptly when security patches are released
- Report any concerns about dependencies via the vulnerability reporting process

### Code Security

- Input validation is performed on all user-supplied data
- API responses are validated before processing
- Error messages do not expose sensitive information
- HTTPS is enforced for all API communications

## Security Updates

Security updates will be released as patch versions. Users are encouraged to:

1. Enable dependency update notifications
2. Subscribe to release notifications for this repository
3. Update to the latest version promptly when security fixes are announced
