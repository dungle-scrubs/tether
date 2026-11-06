# Checked code conventions

These checks cover `packages/client/src/participant-rest-replay.ts`.

| Convention | Enforcement |
| --- | --- |
| Module-level dependencies are declared before use. | The declaration-order case in `packages/client/test/participant-rest-replay-style.test.ts` resolves local symbols and rejects references from an earlier module declaration. |
| Named functions, constructors and methods use JSDoc. | The documentation case in the same test inspects syntax nodes and reports the declaration needing JSDoc. |

Both cases run with the client's existing test command and the root test task graph.
