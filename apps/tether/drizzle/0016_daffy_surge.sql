CREATE TABLE "auth_grant_audit_events" (
	"action" text NOT NULL,
	"actor_subject" text NOT NULL,
	"audit_id" text PRIMARY KEY NOT NULL,
	"grant_jti" text NOT NULL,
	"metadata" jsonb NOT NULL,
	"occurred_at" timestamp with time zone DEFAULT now() NOT NULL,
	"reason_code" text NOT NULL,
	CONSTRAINT "auth_grant_audit_action_length_check" CHECK (char_length("auth_grant_audit_events"."action") BETWEEN 1 AND 64),
	CONSTRAINT "auth_grant_audit_actor_length_check" CHECK (char_length("auth_grant_audit_events"."actor_subject") BETWEEN 1 AND 255),
	CONSTRAINT "auth_grant_audit_id_length_check" CHECK (char_length("auth_grant_audit_events"."audit_id") BETWEEN 1 AND 128),
	CONSTRAINT "auth_grant_audit_metadata_size_check" CHECK (octet_length("auth_grant_audit_events"."metadata"::text) <= 4096),
	CONSTRAINT "auth_grant_audit_metadata_shape_check" CHECK (jsonb_typeof("auth_grant_audit_events"."metadata") = 'object'
        AND "auth_grant_audit_events"."metadata" ? 'requestId'
        AND "auth_grant_audit_events"."metadata" - 'requestId' = '{}'::jsonb
        AND (
          jsonb_typeof("auth_grant_audit_events"."metadata"->'requestId') = 'null'
          OR (
            jsonb_typeof("auth_grant_audit_events"."metadata"->'requestId') = 'string'
            AND "auth_grant_audit_events"."metadata"->>'requestId' ~ '^req_[A-Za-z0-9_-]{1,120}$'
          )
        )),
	CONSTRAINT "auth_grant_audit_action_check" CHECK ("auth_grant_audit_events"."action" IN ('grant.created', 'grant.revoked')),
	CONSTRAINT "auth_grant_audit_reason_length_check" CHECK (char_length("auth_grant_audit_events"."reason_code") BETWEEN 1 AND 64),
	CONSTRAINT "auth_grant_audit_reason_check" CHECK ("auth_grant_audit_events"."reason_code" IN ('bootstrap', 'key-rotation', 'migration', 'operator-request', 'security-response'))
);
--> statement-breakpoint
CREATE TABLE "auth_grants" (
	"audience" text NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"issued_at" timestamp with time zone NOT NULL,
	"issuer" text NOT NULL,
	"jti" text PRIMARY KEY NOT NULL,
	"kid" text NOT NULL,
	"metadata" jsonb NOT NULL,
	"revoked_at" timestamp with time zone,
	"role" text NOT NULL,
	"session_scope" text NOT NULL,
	"subject" text NOT NULL,
	CONSTRAINT "auth_grants_audience_check" CHECK ("auth_grants"."audience" = 'tether-rest'),
	CONSTRAINT "auth_grants_expiry_check" CHECK ("auth_grants"."expires_at" > "auth_grants"."issued_at"),
	CONSTRAINT "auth_grants_lifetime_check" CHECK ("auth_grants"."expires_at" <= "auth_grants"."issued_at" + interval '7 days'),
	CONSTRAINT "auth_grants_issuer_length_check" CHECK (char_length("auth_grants"."issuer") BETWEEN 1 AND 512),
	CONSTRAINT "auth_grants_jti_length_check" CHECK (char_length("auth_grants"."jti") BETWEEN 1 AND 128),
	CONSTRAINT "auth_grants_kid_length_check" CHECK (char_length("auth_grants"."kid") BETWEEN 1 AND 128),
	CONSTRAINT "auth_grants_metadata_size_check" CHECK (octet_length("auth_grants"."metadata"::text) <= 4096),
	CONSTRAINT "auth_grants_metadata_shape_check" CHECK (jsonb_typeof("auth_grants"."metadata") = 'object'
        AND "auth_grants"."metadata" ? 'requestId'
        AND "auth_grants"."metadata" ? 'source'
        AND "auth_grants"."metadata" - ARRAY['requestId', 'source'] = '{}'::jsonb
        AND jsonb_typeof("auth_grants"."metadata"->'source') = 'string'
        AND "auth_grants"."metadata"->>'source' IN ('admin', 'bootstrap', 'migration')
        AND (
          jsonb_typeof("auth_grants"."metadata"->'requestId') = 'null'
          OR (
            jsonb_typeof("auth_grants"."metadata"->'requestId') = 'string'
            AND "auth_grants"."metadata"->>'requestId' ~ '^req_[A-Za-z0-9_-]{1,120}$'
          )
        )),
	CONSTRAINT "auth_grants_revoked_check" CHECK ("auth_grants"."revoked_at" IS NULL OR "auth_grants"."revoked_at" >= "auth_grants"."issued_at"),
	CONSTRAINT "auth_grants_role_check" CHECK ("auth_grants"."role" IN ('observer', 'participant', 'admin')),
	CONSTRAINT "auth_grants_session_scope_length_check" CHECK (char_length("auth_grants"."session_scope") BETWEEN 1 AND 255),
	CONSTRAINT "auth_grants_subject_length_check" CHECK (char_length("auth_grants"."subject") BETWEEN 1 AND 255)
);
--> statement-breakpoint
CREATE TABLE "auth_tickets" (
	"admission_metadata" jsonb NOT NULL,
	"audience" text NOT NULL,
	"consumed_at" timestamp with time zone,
	"created_at" timestamp with time zone NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"parent_grant_jti" text NOT NULL,
	"ticket_hash" text PRIMARY KEY NOT NULL,
	CONSTRAINT "auth_tickets_admission_metadata_size_check" CHECK (octet_length("auth_tickets"."admission_metadata"::text) <= 4096),
	CONSTRAINT "auth_tickets_admission_metadata_shape_check" CHECK (jsonb_typeof("auth_tickets"."admission_metadata") = 'object'
        AND "auth_tickets"."admission_metadata" ? 'remoteAddressHash'
        AND "auth_tickets"."admission_metadata" ? 'replicaId'
        AND "auth_tickets"."admission_metadata" ? 'transport'
        AND "auth_tickets"."admission_metadata" - ARRAY['remoteAddressHash', 'replicaId', 'transport'] = '{}'::jsonb
        AND jsonb_typeof("auth_tickets"."admission_metadata"->'replicaId') = 'string'
        AND "auth_tickets"."admission_metadata"->>'replicaId' ~ '^replica_[A-Za-z0-9_-]{1,120}$'
        AND jsonb_typeof("auth_tickets"."admission_metadata"->'transport') = 'string'
        AND "auth_tickets"."admission_metadata"->>'transport' = 'websocket'
        AND (
          jsonb_typeof("auth_tickets"."admission_metadata"->'remoteAddressHash') = 'null'
          OR (
            jsonb_typeof("auth_tickets"."admission_metadata"->'remoteAddressHash') = 'string'
            AND "auth_tickets"."admission_metadata"->>'remoteAddressHash' ~ '^[0-9a-f]{64}$'
          )
        )),
	CONSTRAINT "auth_tickets_audience_check" CHECK ("auth_tickets"."audience" = 'tether-websocket'),
	CONSTRAINT "auth_tickets_expiry_check" CHECK ("auth_tickets"."expires_at" > "auth_tickets"."created_at"),
	CONSTRAINT "auth_tickets_lifetime_check" CHECK ("auth_tickets"."expires_at" <= "auth_tickets"."created_at" + interval '30 seconds'),
	CONSTRAINT "auth_tickets_consumed_check" CHECK ("auth_tickets"."consumed_at" IS NULL OR ("auth_tickets"."consumed_at" >= "auth_tickets"."created_at" AND "auth_tickets"."consumed_at" <= "auth_tickets"."expires_at")),
	CONSTRAINT "auth_tickets_hash_check" CHECK ("auth_tickets"."ticket_hash" ~ '^[0-9a-f]{64}$')
);
--> statement-breakpoint
ALTER TABLE "auth_grant_audit_events" ADD CONSTRAINT "auth_grant_audit_events_grant_jti_auth_grants_jti_fk" FOREIGN KEY ("grant_jti") REFERENCES "public"."auth_grants"("jti") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "auth_tickets" ADD CONSTRAINT "auth_tickets_parent_grant_jti_auth_grants_jti_fk" FOREIGN KEY ("parent_grant_jti") REFERENCES "public"."auth_grants"("jti") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "auth_grant_audit_grant_occurred_idx" ON "auth_grant_audit_events" USING btree ("grant_jti","occurred_at");--> statement-breakpoint
CREATE INDEX "auth_grant_audit_occurred_idx" ON "auth_grant_audit_events" USING btree ("occurred_at");--> statement-breakpoint
CREATE INDEX "auth_grants_expiry_idx" ON "auth_grants" USING btree ("expires_at");--> statement-breakpoint
CREATE INDEX "auth_grants_revoked_expiry_idx" ON "auth_grants" USING btree ("revoked_at","expires_at");--> statement-breakpoint
CREATE INDEX "auth_tickets_expiry_idx" ON "auth_tickets" USING btree ("expires_at");--> statement-breakpoint
CREATE INDEX "auth_tickets_parent_expiry_idx" ON "auth_tickets" USING btree ("parent_grant_jti","expires_at");--> statement-breakpoint
CREATE INDEX "auth_tickets_consumed_idx" ON "auth_tickets" USING btree ("consumed_at");