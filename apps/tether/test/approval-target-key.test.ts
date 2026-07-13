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
});
