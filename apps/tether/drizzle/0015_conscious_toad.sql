CREATE TABLE "session_summaries" (
	"budget_class" text NOT NULL,
	"content" jsonb,
	"covers_seq_from" bigint NOT NULL,
	"covers_seq_to" bigint NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"failure" jsonb,
	"generation_task_id" text NOT NULL,
	"integrity_algorithm" text,
	"integrity_hash" text,
	"ollama_context_size" integer NOT NULL,
	"ollama_model" text NOT NULL,
	"ollama_quantization" text NOT NULL,
	"ollama_revision" text NOT NULL,
	"ollama_thinking_mode" text NOT NULL,
	"output_schema_version" text NOT NULL,
	"producer_id" text NOT NULL,
	"producer_version" text NOT NULL,
	"prompt_version" text NOT NULL,
	"published_at" timestamp with time zone,
	"quarantined_at" timestamp with time zone,
	"session_id" text NOT NULL,
	"source_event_count" bigint NOT NULL,
	"source_first_event_id" text NOT NULL,
	"source_last_event_id" text NOT NULL,
	"source_range_hash" text NOT NULL,
	"summary_id" text PRIMARY KEY NOT NULL,
	"superseded_at" timestamp with time zone,
	"validated_at" timestamp with time zone,
	CONSTRAINT "session_summaries_generation_task_unique" UNIQUE("generation_task_id"),
	CONSTRAINT "session_summaries_range_check" CHECK ("session_summaries"."covers_seq_from" <= "session_summaries"."covers_seq_to"),
	CONSTRAINT "session_summaries_integrity_pair_check" CHECK (("session_summaries"."integrity_algorithm" IS NULL) = ("session_summaries"."integrity_hash" IS NULL))
);
--> statement-breakpoint
ALTER TABLE "session_summaries" ADD CONSTRAINT "session_summaries_session_id_sessions_session_id_fk" FOREIGN KEY ("session_id") REFERENCES "public"."sessions"("session_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "session_summaries_session_budget_created_idx" ON "session_summaries" USING btree ("session_id","budget_class","created_at" DESC NULLS LAST);--> statement-breakpoint
CREATE UNIQUE INDEX "session_summaries_active_unique" ON "session_summaries" USING btree ("session_id","budget_class") WHERE "session_summaries"."published_at" IS NOT NULL AND "session_summaries"."superseded_at" IS NULL;