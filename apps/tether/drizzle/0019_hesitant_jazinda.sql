DROP INDEX "tasks_schedule_identity_idx";--> statement-breakpoint
ALTER TABLE "tasks" ADD COLUMN "schedule_identity_version" integer;--> statement-breakpoint
ALTER TABLE "tasks" ADD COLUMN "schedule_scope_key" text;--> statement-breakpoint
UPDATE "tasks"
SET "schedule_identity_version" = 1,
    "schedule_scope_key" = 'legacy_v1_' || encode(
      sha256(convert_to(jsonb_build_array("mailbox_provider", "mailbox_account_id")::text, 'UTF8')),
      'hex'
    )
WHERE "mailbox_provider" IS NOT NULL
  AND "mailbox_account_id" IS NOT NULL
  AND "schedule_algorithm_version" IS NOT NULL
  AND "schedule_interval_ms" IS NOT NULL
  AND "schedule_window_start" IS NOT NULL;--> statement-breakpoint
CREATE UNIQUE INDEX "tasks_schedule_identity_idx" ON "tasks" USING btree ("session_id","kind","schedule_identity_version","schedule_scope_key","schedule_algorithm_version","schedule_interval_ms","schedule_window_start");
