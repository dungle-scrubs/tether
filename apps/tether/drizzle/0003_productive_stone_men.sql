ALTER TABLE "tasks" ADD COLUMN "claim_expires_at" timestamp with time zone;
--> statement-breakpoint
UPDATE "tasks"
SET "claim_expires_at" = now() + interval '5 minutes'
WHERE "claimed_at" IS NOT NULL
  AND "claimed_by" IS NOT NULL
  AND "completed_at" IS NULL
  AND "failed_at" IS NULL
  AND "cancelled_at" IS NULL
  AND "claim_expires_at" IS NULL;
