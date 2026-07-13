import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import { SessionServicePersistenceError } from "../src/session-service-contracts.js";
import { trySessionPromise } from "../src/session-service-runtime.js";

describe("trySessionPromise", () => {
  it("maps rejected persistence promises into typed service failures with the original cause", async () => {
    const cause = new Error("database unavailable");

    const failure = await Effect.runPromise(
      Effect.flip(trySessionPromise(() => Promise.reject(cause), "createSession")),
    );

    expect(failure).toBeInstanceOf(SessionServicePersistenceError);
    expect(failure._tag).toBe("SessionServicePersistenceError");
    expect(failure.operation).toBe("createSession");
    expect(failure.cause).toBe(cause);
    expect(failure.message).toBe("Session service persistence failed during createSession");
  });
});
