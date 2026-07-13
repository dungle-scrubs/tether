ALTER TABLE "tasks" ADD COLUMN "mailbox_account_id" text;--> statement-breakpoint
ALTER TABLE "tasks" ADD COLUMN "mailbox_provider" text;--> statement-breakpoint
ALTER TABLE "tasks" ADD COLUMN "schedule_algorithm_version" integer;--> statement-breakpoint
ALTER TABLE "tasks" ADD COLUMN "schedule_interval_ms" bigint;--> statement-breakpoint
ALTER TABLE "tasks" ADD COLUMN "schedule_window_start" bigint;--> statement-breakpoint
CREATE UNIQUE INDEX "tasks_schedule_identity_idx" ON "tasks" USING btree ("session_id","kind","mailbox_provider","mailbox_account_id","schedule_algorithm_version","schedule_interval_ms","schedule_window_start");