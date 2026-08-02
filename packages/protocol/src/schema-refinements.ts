import type { z } from "zod";

/**
 * Builds one Zod array refinement that rejects repeated entries under a caller
 * supplied identity, reporting the repeat at its own array index.
 */
export function rejectDuplicateEntries<TValue>(input: {
  readonly identity: (value: TValue) => string;
  readonly message: string;
}): (values: readonly TValue[], context: z.RefinementCtx) => void {
  return (values, context) => {
    const seen = new Set<string>();
    for (const [index, value] of values.entries()) {
      const identity = input.identity(value);
      if (seen.has(identity)) {
        context.addIssue({ code: "custom", message: input.message, path: [index] });
      }
      seen.add(identity);
    }
  };
}
