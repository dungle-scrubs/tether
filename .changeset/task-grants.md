---
"@dungle-scrubs/tether-protocol": minor
"@dungle-scrubs/tether-client": minor
---

Add fine-grained task authorization: separate task.create and task.claim actions with kind allowlists and optional explicit assignee, parent linkage as lineage only, typed claim denials distinct from race rejection. Default off with no policy rows; strict when rows exist.
