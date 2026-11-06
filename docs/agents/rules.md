# Checked code conventions

These checks cover `packages/client/src/participant-rest-replay.ts` with zero permitted findings. They also cover `packages/client/test/participant-runtime-client.test.ts` using incremental baselines measured at pre-feature commit `085c18223934c4e5754f6ea6328cec3dab1ffe5c`: 27 declaration-order findings and 8 undocumented named declarations. New helpers must be declared above first use and have JSDoc. Do not increase these baselines to admit new violations.

| Convention | Enforcement |
| --- | --- |
| Module-level dependencies are declared before use. | The declaration-order case in `packages/client/test/participant-rest-replay-style.test.ts` resolves local symbols and rejects references from an earlier module declaration. |
| Named functions, constructors and methods use JSDoc. | The documentation case in the same test inspects syntax nodes and reports the declaration needing JSDoc. |

Both cases run with the client's existing test command and the root test task graph.
