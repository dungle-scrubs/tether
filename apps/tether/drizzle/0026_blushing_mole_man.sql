CREATE TABLE "session_bootstrap_identities" (
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"identity_key" text PRIMARY KEY NOT NULL,
	"session_id" text NOT NULL,
	CONSTRAINT "session_bootstrap_identities_session_id_unique" UNIQUE("session_id"),
	CONSTRAINT "session_bootstrap_identities_identity_key_size_check" CHECK (octet_length("session_bootstrap_identities"."identity_key") BETWEEN 1 AND 512)
);
--> statement-breakpoint
ALTER TABLE "session_bootstrap_identities" ADD CONSTRAINT "session_bootstrap_identities_session_id_sessions_session_id_fk" FOREIGN KEY ("session_id") REFERENCES "public"."sessions"("session_id") ON DELETE restrict ON UPDATE no action;