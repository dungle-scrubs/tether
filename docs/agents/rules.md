# Checked code conventions

The client convention test checks every source and test file touched by REST replay recovery. Findings are compared by exact identity with `packages/client/test/participant-conventions.baseline.json`, recorded from `25041cb`. Each identity includes its file, declaration kind and name, and referencing site. Repeated references remain separate findings. Fixing a legacy violation requires deleting its baseline entry in the same change. New identities fail even when the total count stays unchanged.

The replay module and runtime test file have zero remaining findings. The REST parser helpers are declared before use. Other checked files retain only the exact recorded legacy identities. New files start with an empty baseline.

| Convention | Enforcement |
| --- | --- |
| Module-level dependencies are declared before use. | `packages/client/test/participant-rest-replay-style.test.ts` resolves local symbols and compares every referencing-site identity with the recorded baseline. Its error requires moving dependencies above use and removing fixed baseline entries. |
| Named functions, constructors, methods and exported interface fields use JSDoc. | The same test checks syntax nodes, including named function-valued variables and nested exported interface fields. Its error requires adding JSDoc and removing fixed baseline entries. |

These checks run with the client's existing test command and the root test task graph.
