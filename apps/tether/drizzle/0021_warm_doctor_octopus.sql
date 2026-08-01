CREATE TABLE "browser_pairing_requests" (
	"confirmed_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"exchange_secret_hash" text NOT NULL,
	"exchanged_at" timestamp with time zone,
	"expires_at" timestamp with time zone NOT NULL,
	"failed_attempts" integer DEFAULT 0 NOT NULL,
	"invalidated_at" timestamp with time zone,
	"operator_subject" text NOT NULL,
	"origin" text NOT NULL,
	"public_nonce" text NOT NULL,
	"request_id" text PRIMARY KEY NOT NULL,
	"requested_scope" jsonb NOT NULL,
	"source_address_hash" text,
	"verification_phrase" text NOT NULL,
	CONSTRAINT "browser_pairing_requests_secret_hash_check" CHECK ("browser_pairing_requests"."exchange_secret_hash" ~ '^[0-9a-f]{64}$'),
	CONSTRAINT "browser_pairing_requests_source_hash_check" CHECK ("browser_pairing_requests"."source_address_hash" IS NULL OR "browser_pairing_requests"."source_address_hash" ~ '^[0-9a-f]{64}$'),
	CONSTRAINT "browser_pairing_requests_attempts_check" CHECK ("browser_pairing_requests"."failed_attempts" BETWEEN 0 AND 5),
	CONSTRAINT "browser_pairing_requests_lifetime_check" CHECK ("browser_pairing_requests"."expires_at" > "browser_pairing_requests"."created_at" AND "browser_pairing_requests"."expires_at" <= "browser_pairing_requests"."created_at" + interval '15 minutes'),
	CONSTRAINT "browser_pairing_requests_scope_check" CHECK (jsonb_typeof("browser_pairing_requests"."requested_scope") = 'object' AND octet_length("browser_pairing_requests"."requested_scope"::text) BETWEEN 2 AND 8192),
	CONSTRAINT "browser_pairing_requests_nonce_check" CHECK (char_length("browser_pairing_requests"."public_nonce") BETWEEN 22 AND 86 AND "browser_pairing_requests"."public_nonce" ~ '^[A-Za-z0-9_-]+$')
);
--> statement-breakpoint
CREATE TABLE "browser_sessions" (
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"csrf_token_hash" text NOT NULL,
	"grant_jti" text PRIMARY KEY NOT NULL,
	"origin" text NOT NULL,
	CONSTRAINT "browser_sessions_csrf_hash_check" CHECK ("browser_sessions"."csrf_token_hash" ~ '^[0-9a-f]{64}$'),
	CONSTRAINT "browser_sessions_origin_length_check" CHECK (char_length("browser_sessions"."origin") BETWEEN 1 AND 512)
);
--> statement-breakpoint
CREATE TABLE "operator_grant_scopes" (
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"grant_jti" text PRIMARY KEY NOT NULL,
	"scope" jsonb NOT NULL,
	CONSTRAINT "operator_grant_scopes_shape_check" CHECK (jsonb_typeof("operator_grant_scopes"."scope") = 'object'),
	CONSTRAINT "operator_grant_scopes_size_check" CHECK (octet_length("operator_grant_scopes"."scope"::text) BETWEEN 2 AND 8192)
);
--> statement-breakpoint
ALTER TABLE "browser_sessions" ADD CONSTRAINT "browser_sessions_grant_jti_auth_grants_jti_fk" FOREIGN KEY ("grant_jti") REFERENCES "public"."auth_grants"("jti") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "operator_grant_scopes" ADD CONSTRAINT "operator_grant_scopes_grant_jti_auth_grants_jti_fk" FOREIGN KEY ("grant_jti") REFERENCES "public"."auth_grants"("jti") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "browser_pairing_requests_source_created_idx" ON "browser_pairing_requests" USING btree ("source_address_hash","created_at");