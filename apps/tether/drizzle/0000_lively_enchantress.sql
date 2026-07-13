CREATE TABLE "participants" (
	"capabilities" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"display_name" text NOT NULL,
	"joined_at" timestamp with time zone DEFAULT now() NOT NULL,
	"last_seen_at" timestamp with time zone DEFAULT now() NOT NULL,
	"participant_id" text NOT NULL,
	"runtime_kind" text NOT NULL,
	"session_id" text NOT NULL,
	CONSTRAINT "participants_session_id_participant_id_pk" PRIMARY KEY("session_id","participant_id")
);
--> statement-breakpoint
CREATE TABLE "session_event_sequences" (
	"next_seq" bigint DEFAULT 1 NOT NULL,
	"session_id" text PRIMARY KEY NOT NULL
);
--> statement-breakpoint
CREATE TABLE "session_events" (
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"event_id" text NOT NULL,
	"payload" jsonb NOT NULL,
	"producer_id" text NOT NULL,
	"seq" bigint NOT NULL,
	"session_id" text NOT NULL,
	"type" text NOT NULL,
	CONSTRAINT "session_events_session_id_seq_pk" PRIMARY KEY("session_id","seq"),
	CONSTRAINT "session_events_event_id_unique" UNIQUE("event_id")
);
--> statement-breakpoint
CREATE TABLE "sessions" (
	"archived_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"session_id" text PRIMARY KEY NOT NULL
);
--> statement-breakpoint
CREATE TABLE "tasks" (
	"claimed_at" timestamp with time zone,
	"claimed_by" text,
	"completed_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"kind" text NOT NULL,
	"objective" text NOT NULL,
	"session_id" text NOT NULL,
	"task_id" text NOT NULL,
	CONSTRAINT "tasks_session_id_task_id_pk" PRIMARY KEY("session_id","task_id")
);
--> statement-breakpoint
ALTER TABLE "participants" ADD CONSTRAINT "participants_session_id_sessions_session_id_fk" FOREIGN KEY ("session_id") REFERENCES "public"."sessions"("session_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "session_event_sequences" ADD CONSTRAINT "session_event_sequences_session_id_sessions_session_id_fk" FOREIGN KEY ("session_id") REFERENCES "public"."sessions"("session_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "session_events" ADD CONSTRAINT "session_events_session_id_sessions_session_id_fk" FOREIGN KEY ("session_id") REFERENCES "public"."sessions"("session_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "tasks" ADD CONSTRAINT "tasks_session_id_sessions_session_id_fk" FOREIGN KEY ("session_id") REFERENCES "public"."sessions"("session_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "participants_session_last_seen_idx" ON "participants" USING btree ("session_id","last_seen_at" DESC NULLS LAST);--> statement-breakpoint
CREATE INDEX "session_events_session_created_idx" ON "session_events" USING btree ("session_id","created_at");