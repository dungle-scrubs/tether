# Control epochs fence same-instance restarts

Status: accepted

An authenticated reconnect using the same participant ID and configured runtime
instance ID immediately supersedes its previous control lease and receives a
new monotonic control epoch; commands from older epochs are rejected. Different
instance IDs continue to conflict until release or expiry. Waiting for every
lease to expire makes ordinary launchd restarts unavailable, while unfenced
same-instance takeover risks split-brain control if the previous process is
still alive.
