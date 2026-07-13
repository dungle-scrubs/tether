ALTER TABLE "tasks" ADD COLUMN "claim_expired_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "tasks" ADD COLUMN "claim_expired_by" text;--> statement-breakpoint
ALTER TABLE "tasks" ADD COLUMN "released_by" text;--> statement-breakpoint
ALTER TABLE "sessions" DROP COLUMN "archived_at";