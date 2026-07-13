## What and why

Describe what this change does and the motivation for it. Link any related
issue (e.g. `Closes #123`).

## Changes

- ...

## Validation

Confirm the public checks pass locally (CI runs the same set):

- [ ] `pnpm run build`
- [ ] `pnpm lint`
- [ ] `pnpm typecheck`
- [ ] `pnpm test`
- [ ] `pnpm test:e2e` (when service behavior changed)

## Checklist

- [ ] Change is focused; one logical change per pull request.
- [ ] Tests added or updated for the behavior I changed.
- [ ] A changeset is included if this affects a published `@dungle-scrubs/*`
      package (`pnpm changeset`).
- [ ] No provider credentials, local machine runbooks, generated tarballs, or
      private workspace packages are added to the public tree.
- [ ] No live secrets in code, tests, or fixtures.
