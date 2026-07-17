CREATE TABLE "session_projections" (
	"active_run_id" text,
	"activity" text NOT NULL,
	"activity_changed_at" timestamp with time zone,
	"archived_at" timestamp with time zone,
	"covers_seq_to" bigint NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"deleted_at" timestamp with time zone,
	"event_count" bigint NOT NULL,
	"forked_from" jsonb,
	"host_metadata" jsonb,
	"host_metadata_source_seq" bigint,
	"last_event_at" timestamp with time zone,
	"reducer_version" integer NOT NULL,
	"session_id" text PRIMARY KEY NOT NULL,
	"tangent_of" jsonb,
	"title" text,
	"title_source_seq" bigint,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "session_projections" ADD CONSTRAINT "session_projections_session_id_sessions_session_id_fk" FOREIGN KEY ("session_id") REFERENCES "public"."sessions"("session_id") ON DELETE cascade ON UPDATE no action;