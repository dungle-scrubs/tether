CREATE TABLE "task_grants" (
	"action" text NOT NULL,
	"created_audit_id" text NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"issued_at" timestamp with time zone NOT NULL,
	"issuer" text NOT NULL,
	"jti" text PRIMARY KEY NOT NULL,
	"kind_allowlist" jsonb NOT NULL,
	"revoked_at" timestamp with time zone,
	"scope_label_allowlist" jsonb NOT NULL,
	"session_scope" text NOT NULL,
	"subject" text NOT NULL,
	CONSTRAINT "task_grants_action_check" CHECK ("task_grants"."action" IN ('task.create', 'task.claim')),
	CONSTRAINT "task_grants_expiry_check" CHECK ("task_grants"."expires_at" > "task_grants"."issued_at"),
	CONSTRAINT "task_grants_lifetime_check" CHECK ("task_grants"."expires_at" <= "task_grants"."issued_at" + interval '7 days'),
	CONSTRAINT "task_grants_issuer_length_check" CHECK (char_length("task_grants"."issuer") BETWEEN 1 AND 512),
	CONSTRAINT "task_grants_jti_length_check" CHECK (char_length("task_grants"."jti") BETWEEN 1 AND 128),
	CONSTRAINT "task_grants_jti_shape_check" CHECK ("task_grants"."jti" ~ '^tgrant_[A-Za-z0-9_-]{1,120}$'),
	CONSTRAINT "task_grants_created_audit_id_length_check" CHECK (char_length("task_grants"."created_audit_id") BETWEEN 1 AND 128),
	CONSTRAINT "task_grants_allowlist_shape_check" CHECK (jsonb_typeof("task_grants"."kind_allowlist") = 'array' AND jsonb_typeof("task_grants"."scope_label_allowlist") = 'array'),
	CONSTRAINT "task_grants_allowlist_size_check" CHECK (octet_length("task_grants"."kind_allowlist"::text) + octet_length("task_grants"."scope_label_allowlist"::text) <= 8192),
	CONSTRAINT "task_grants_revoked_check" CHECK ("task_grants"."revoked_at" IS NULL OR "task_grants"."revoked_at" >= "task_grants"."issued_at"),
	CONSTRAINT "task_grants_session_scope_length_check" CHECK (char_length("task_grants"."session_scope") BETWEEN 1 AND 255),
	CONSTRAINT "task_grants_subject_length_check" CHECK (char_length("task_grants"."subject") BETWEEN 1 AND 255)
);--> statement-breakpoint
ALTER TABLE "tasks" ADD COLUMN "assignee_participant_id" text;--> statement-breakpoint
ALTER TABLE "tasks" ADD COLUMN "parent_task_id" text;--> statement-breakpoint
ALTER TABLE "tasks" ADD COLUMN "scope_label" text;--> statement-breakpoint
ALTER TABLE "auth_grant_audit_events" ADD COLUMN "task_grant_jti" text;--> statement-breakpoint
ALTER TABLE "auth_grant_audit_events" ALTER COLUMN "grant_jti" DROP NOT NULL;--> statement-breakpoint
ALTER TABLE "auth_grant_audit_events" DROP CONSTRAINT "auth_grant_audit_action_check";--> statement-breakpoint
ALTER TABLE "auth_grant_audit_events" ADD CONSTRAINT "auth_grant_audit_action_check" CHECK ("auth_grant_audit_events"."action" IN ('grant.created', 'grant.revoked', 'task_grant.created', 'task_grant.revoked'));--> statement-breakpoint
ALTER TABLE "auth_grant_audit_events" ADD CONSTRAINT "auth_grant_audit_subject_check" CHECK (("auth_grant_audit_events"."grant_jti" IS NULL) <> ("auth_grant_audit_events"."task_grant_jti" IS NULL));--> statement-breakpoint
ALTER TABLE "auth_grant_audit_events" ADD CONSTRAINT "auth_grant_audit_task_grant_action_check" CHECK (("auth_grant_audit_events"."task_grant_jti" IS NULL) = ("auth_grant_audit_events"."action" IN ('grant.created', 'grant.revoked')));--> statement-breakpoint
ALTER TABLE "auth_grant_audit_events" ADD CONSTRAINT "auth_grant_audit_task_grant_jti_length_check" CHECK ("auth_grant_audit_events"."task_grant_jti" IS NULL OR char_length("auth_grant_audit_events"."task_grant_jti") BETWEEN 1 AND 128);--> statement-breakpoint
ALTER TABLE "tasks" ADD CONSTRAINT "tasks_session_parent_fk" FOREIGN KEY ("session_id","parent_task_id") REFERENCES "public"."tasks"("session_id","task_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "auth_grant_audit_events" ADD CONSTRAINT "auth_grant_audit_events_task_grant_jti_task_grants_jti_fk" FOREIGN KEY ("task_grant_jti") REFERENCES "public"."task_grants"("jti") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "tasks" ADD CONSTRAINT "tasks_assignee_length_check" CHECK ("tasks"."assignee_participant_id" IS NULL OR char_length("tasks"."assignee_participant_id") BETWEEN 1 AND 255);--> statement-breakpoint
ALTER TABLE "tasks" ADD CONSTRAINT "tasks_parent_length_check" CHECK ("tasks"."parent_task_id" IS NULL OR char_length("tasks"."parent_task_id") BETWEEN 1 AND 255);--> statement-breakpoint
ALTER TABLE "tasks" ADD CONSTRAINT "tasks_scope_label_length_check" CHECK ("tasks"."scope_label" IS NULL OR char_length("tasks"."scope_label") BETWEEN 1 AND 128);--> statement-breakpoint
CREATE INDEX "task_grants_expiry_idx" ON "task_grants" USING btree ("expires_at");--> statement-breakpoint
CREATE INDEX "task_grants_revoked_expiry_idx" ON "task_grants" USING btree ("revoked_at","expires_at");--> statement-breakpoint
CREATE INDEX "task_grants_subject_session_idx" ON "task_grants" USING btree ("subject","session_scope");--> statement-breakpoint
CREATE INDEX "auth_grant_audit_task_grant_occurred_idx" ON "auth_grant_audit_events" USING btree ("task_grant_jti","occurred_at");--> statement-breakpoint
CREATE INDEX "tasks_assignee_idx" ON "tasks" USING btree ("session_id","assignee_participant_id") WHERE "tasks"."assignee_participant_id" IS NOT NULL;--> statement-breakpoint
CREATE INDEX "tasks_parent_idx" ON "tasks" USING btree ("session_id","parent_task_id") WHERE "tasks"."parent_task_id" IS NOT NULL;
