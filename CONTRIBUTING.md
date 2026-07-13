# Contributing to Tether

Thanks for your interest in contributing. This document describes how to set up
the project, propose changes, and get them merged.

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By
participating, you are expected to uphold it.

## Scope

Tether owns the service boundary and provider-neutral SDKs: sessions, events,
participants, tasks, approvals, and external client bindings. Provider-specific
adapters, private provider workflows, and local machine runbooks belong outside
this repository and should integrate over the public REST and WebSocket APIs
through `@dungle-scrubs/tether-client`, `@dungle-scrubs/tether-client-bridge`, and `@dungle-scrubs/tether-protocol`.

Please do not add provider credentials, local machine runbooks, generated
package tarballs, or private workspace packages to the public tree.

## Development Setup

Requirements: Node 22, `pnpm@11`, and Docker (for Postgres).

```sh
pnpm install
cp .env.example .env   # then set local secrets
docker compose up -d --build
```

Run the service without Docker:

```sh
pnpm dev
```

The service listens on `127.0.0.1:3025` by default.

## Making Changes

1. Fork the repository and create a branch off `main`.
2. Keep changes focused. One logical change per pull request.
3. Follow the project conventions: `pnpm` for scripts, the shared Biome and
   TypeScript configs, strict-mode TypeScript (no `any`, prefer `unknown`),
   Drizzle for schema work, and JSDoc-style comments for exported and
   non-obvious code.
4. Add or update tests for behavior you change.

## Validation

Run the full public checks before opening a pull request:

```sh
pnpm run build
pnpm lint
pnpm typecheck
pnpm test
pnpm test:e2e
```

Pre-commit hooks run automatically via lefthook. Do not use `--no-verify`; the
repository intentionally blocks commits that bypass pre-commit checks.

## Pull Requests

- Describe what the change does and why.
- Link any related issue.
- Ensure CI (secret scan, lint, typecheck, unit tests, e2e) passes.
- Be prepared to iterate in review; the priority is correctness, simplicity,
  and long-term maintainability.

## Reporting Bugs and Vulnerabilities

For functional bugs, open an issue with reproduction steps and expected versus
actual behavior. For security vulnerabilities, follow
[`SECURITY.md`](SECURITY.md) and do not include live secrets in reports or test
fixtures.

## License

By contributing, you agree that your contributions will be licensed under the
[MIT License](LICENSE) that covers this project.
