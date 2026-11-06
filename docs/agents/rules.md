# Checked code conventions

The client convention test checks every source and test file touched by REST replay recovery. Findings are compared by exact identity with `packages/client/test/participant-conventions.baseline.json`, verified against pre-feature `085c18223934c4e5754f6ea6328cec3dab1ffe5c`. Each identity includes its file, declaration kind and name, and referencing site. Repeated references remain separate findings. Fixing a legacy violation requires deleting its baseline entry in the same change. New identities fail even when the total count stays unchanged. Baselines must not grow to admit new violations; every retained entry must exist at the pre-feature commit.

The replay module has zero remaining findings. Type-alias member coverage includes only identities verified at the same pre-feature commit; it admits no feature-era exemptions. The REST parser helpers are declared before use. Other checked files retain only the exact recorded legacy identities. New files start with an empty baseline.

| Convention | Enforcement |
| --- | --- |
| Module-level dependencies are declared before use. | `packages/client/test/participant-rest-replay-style.test.ts` resolves local symbols and compares every referencing-site identity with the recorded baseline. Its error requires moving dependencies above use and removing fixed baseline entries. |
| Named functions, constructors, methods, class properties, every interface property or method member and object members of type aliases use JSDoc. | The same test checks syntax nodes, including named function-valued variables, class properties, property or method members of exported and private interfaces, and object members of type aliases. Nested interface and alias fields are included; unrelated inline parameter object types are not. Its error requires adding JSDoc and removing fixed baseline entries. |

These checks run with the client's existing test command and the root test task graph.
