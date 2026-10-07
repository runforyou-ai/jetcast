# Security policy

Please report vulnerabilities privately through
[GitHub security advisories](https://github.com/runforyou-ai/jetcast/security/advisories/new).
Do not open public issues for them.

jetcast relies on NATS to enforce permissions. Keep the callout signing key
secret, restrict the application's NATS user, and set account limits for the
client account; see [docs/deployment.md](docs/deployment.md).
