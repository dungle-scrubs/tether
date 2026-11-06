# Checked code conventions

These checks cover `packages/client/src/participant-rest-replay.ts`. The declaration-order check also covers `packages/client/test/participant-runtime-client.test.ts` with an incremental baseline of 27 findings measured at pre-feature commit `085c18223934c4e5754f6ea6328cec3dab1ffe5c`; new helpers must be declared above first use. The replay module permits zero findings. Do not increase the baseline to admit new violations.

| Convention | Enforcement |
| --- | --- |
| Module-level dependencies are declared before use. | The declaration-order case in `packages/client/test/participant-rest-replay-style.test.ts` resolves local symbols and rejects references from an earlier module declaration. |
| Named functions, constructors and methods use JSDoc. | The documentation case in the same test inspects syntax nodes and reports the declaration needing JSDoc. |

Both cases run with the client's existing test command and the root test task graph.
