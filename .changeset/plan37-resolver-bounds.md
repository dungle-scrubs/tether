---
"@dungle-scrubs/tether-client-bridge": major
---

Bound `ClientBridgeSessionResolver` with a deterministic 1,024-entry LRU cache,
30-minute idle expiry, explicit `invalidate`, same-key in-flight deduplication,
and a 64-entry distinct in-flight cap that rejects overflow with the new
`ClientBridgeSessionResolverResourceLimitError`. These bounds cannot be
disabled. The resolver debug shape now reports bounded cache, in-flight,
eviction, and expiry counters in place of `resolvedExternalIdCount`.
