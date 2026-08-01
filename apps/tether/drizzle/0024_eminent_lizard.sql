CREATE TABLE "browser_pairing_exchange_failures" (
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"failure_id" text PRIMARY KEY NOT NULL,
	"source_address_hash" text NOT NULL,
	CONSTRAINT "browser_pairing_exchange_failures_id_check" CHECK ("browser_pairing_exchange_failures"."failure_id" ~ '^pairfail_[A-Za-z0-9_-]{1,120}$'),
	CONSTRAINT "browser_pairing_exchange_failures_source_hash_check" CHECK ("browser_pairing_exchange_failures"."source_address_hash" ~ '^[0-9a-f]{64}$')
);
--> statement-breakpoint
CREATE INDEX "browser_pairing_exchange_failures_source_created_idx" ON "browser_pairing_exchange_failures" USING btree ("source_address_hash","created_at");