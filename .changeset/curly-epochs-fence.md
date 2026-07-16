---
"@dungle-scrubs/tether-protocol": minor
"@dungle-scrubs/tether-client": minor
"@dungle-scrubs/tether-client-bridge": major
---

Add protocol-owned REST participant acquisition, renewal, and release
contracts. Publish the reusable REST participant control lifecycle client and
compose it into bridge cancellation and approval. Bridge participant identity
moves from per-operation inputs into task-client control configuration.

The migration release retains explicit compatibility for one full published
interval. Compatibility is removed in the immediately following breaking
release.
