import { describe, expect, it } from "vitest";

import { approvalTargetKey, wholeTaskApprovalTargetKey } from "../src/approval-target-key.js";

describe("approvalTargetKey", () => {
  it("derives durable approval target keys from recommendation reasons", () => {
    expect(
      approvalTargetKey({
        approvalTarget: {
          action: "junk",
          key: "m_external_compact_ref",
        },
      }),
    ).toBe("approvalTarget:junk:m_external_compact_ref");
    expect(
      approvalTargetKey({
        approvalTarget: {
          key: "<raw-rest-message@example.test>",
        },
      }),
    ).toBe("approvalTarget:<raw-rest-message@example.test>");
    expect(
      approvalTargetKey({
        approvalTarget: {
          action: "keep",
          key: "<raw-rest-message@example.test>",
        },
      }),
    ).not.toBe(
      approvalTargetKey({
        approvalTarget: {
          key: "<raw-rest-message@example.test>",
        },
      }),
    );
    expect(approvalTargetKey({ source: "external-chat" })).toBe(wholeTaskApprovalTargetKey);
  });

  it("uses one fixed-size digest for bounded opaque target identities", () => {
    const target = {
      action: "a".repeat(512),
      digest: "d".repeat(512),
      scopeKey: "🧪".repeat(128),
      targetId: "i".repeat(512),
      targetKind: "k".repeat(512),
      targetRevision: "r".repeat(512),
    };
    const key = approvalTargetKey({}, target);

    expect(key).toMatch(/^approvalTarget:v2:sha256:[0-9a-f]{64}$/);
    expect(key).toHaveLength(89);
    expect(approvalTargetKey({}, target)).toBe(key);
    expect(
      approvalTargetKey({}, { ...target, targetRevision: `2${target.targetRevision}` }),
    ).not.toBe(key);
  });
});
