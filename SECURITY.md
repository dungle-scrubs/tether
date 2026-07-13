# Security

Tether treats sessions, tasks, approvals, and participant control as protected
service data. Run public deployments with `AUTH_MODE=required`.

## Tokens

Use signed tokens minted by the service for every non-local request. Prefer the
least privileged role and scope:

- `admin` tokens are for trusted operators and maintenance.
- `participant` tokens are for runtimes that publish events, claim tasks, or
  hold a WebSocket control lease.
- `observer` tokens are for read-only clients.

Keep `AUTH_SIGNING_SECRET`, `SERVICE_AUTH_TOKEN`, database credentials, and
deployment secrets out of git. `.env.example` is the only environment file that
belongs in the repository.

## Boundaries

Provider-specific adapters should run outside this repository and connect over
the public REST and WebSocket APIs. Do not add provider credentials, local
machine runbooks, generated package tarballs, or private workspace packages to
the public tree.

## Reporting a Vulnerability

Report vulnerabilities privately through GitHub's **Report a vulnerability**
form on the repository's Security tab:
https://github.com/dungle-scrubs/tether/security/advisories/new. Reports are
visible only to the maintainers; please do not open a public issue for a
security problem.

Include the affected endpoint, required privileges, reproduction steps, and
expected impact. Do not include live secrets in reports or test fixtures.

We aim to acknowledge reports within a few business days and will coordinate a
fix and disclosure timeline with you.
