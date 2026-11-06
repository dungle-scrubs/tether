# Checked code conventions

The client convention test checks every source and test file touched by REST replay recovery. Declaration-order findings are compared by exact identity with `packages/client/test/participant-conventions.baseline.json`, recorded from `25041cb`. Each identity includes its file, declaration kind and name, and referencing site. Repeated references remain separate findings. Fixing a legacy violation requires deleting its baseline entry in the same change. New identities fail even when the total count stays unchanged.

The runtime test helper block and REST parser helpers are declared before use, with zero remaining declaration-order findings. The runtime implementation retains 35 existing declaration-order identities; other checked files have none.

JSDoc checks currently require zero undocumented named declarations in `participant-rest-replay.ts` and permit the eight pre-feature `085c182` declarations in `participant-runtime-client.test.ts`.

| Convention | Enforcement |
| --- | --- |
| Module-level dependencies are declared before use. | `packages/client/test/participant-rest-replay-style.test.ts` resolves local symbols and compares every referencing-site identity with the recorded baseline. Its error requires moving dependencies above use and removing fixed baseline entries. |
| Named functions, constructors and methods use JSDoc. | The documentation cases in the same test inspect syntax nodes and require JSDoc on new named declarations in the two files described above. |

These checks run with the client's existing test command and the root test task graph.
