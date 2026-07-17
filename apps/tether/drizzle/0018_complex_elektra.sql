CREATE TABLE "session_tombstones" (
	"deleted_at" timestamp with time zone DEFAULT now() NOT NULL,
	"last_seq" bigint DEFAULT 0 NOT NULL,
	"session_id" text PRIMARY KEY NOT NULL
);
