import { execFileSync } from "node:child_process";

/**
 * Resolves the verification marker inside the current checkout's Git directory.
 * Linked worktrees use a `.git` file, so filesystem joins against `.git` are
 * not portable across normal checkouts and worktrees.
 */
export function resolvePrecommitMarkerPath() {
  return execFileSync("git", ["rev-parse", "--git-path", "tether-precommit-ok"], {
    encoding: "utf8",
  }).trim();
}
