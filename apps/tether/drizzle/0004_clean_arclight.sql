CREATE TABLE "client_session_bindings" (
	"archived_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"external_id" text NOT NULL,
	"last_seen_at" timestamp with time zone DEFAULT now() NOT NULL,
	"provider" text NOT NULL,
	"session_id" text NOT NULL,
	CONSTRAINT "client_session_bindings_provider_external_id_pk" PRIMARY KEY("provider","external_id")
);
--> statement-breakpoint
ALTER TABLE "client_session_bindings" ADD CONSTRAINT "client_session_bindings_session_id_sessions_session_id_fk" FOREIGN KEY ("session_id") REFERENCES "public"."sessions"("session_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "client_session_bindings_session_idx" ON "client_session_bindings" USING btree ("session_id");