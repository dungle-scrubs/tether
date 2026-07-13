CREATE TABLE "participant_control_leases" (
	"claimed_at" timestamp with time zone DEFAULT now() NOT NULL,
	"control_channel" text NOT NULL,
	"instance_id" text NOT NULL,
	"last_seen_at" timestamp with time zone DEFAULT now() NOT NULL,
	"lease_expires_at" timestamp with time zone NOT NULL,
	"participant_id" text NOT NULL,
	"released_at" timestamp with time zone,
	"session_id" text NOT NULL,
	CONSTRAINT "participant_control_leases_session_id_participant_id_instance_id_pk" PRIMARY KEY("session_id","participant_id","instance_id")
);
--> statement-breakpoint
ALTER TABLE "participant_control_leases" ADD CONSTRAINT "participant_control_leases_session_id_sessions_session_id_fk" FOREIGN KEY ("session_id") REFERENCES "public"."sessions"("session_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "participant_control_leases_active_idx" ON "participant_control_leases" USING btree ("session_id","participant_id","lease_expires_at");