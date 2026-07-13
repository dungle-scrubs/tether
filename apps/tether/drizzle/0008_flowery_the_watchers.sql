ALTER TABLE "participant_control_leases" ADD COLUMN "superseded_at" timestamp with time zone;--> statement-breakpoint
WITH ranked_current_leases AS (
	SELECT
		"session_id",
		"participant_id",
		"instance_id",
		row_number() OVER (
			PARTITION BY "session_id", "participant_id"
			ORDER BY "lease_expires_at" DESC, "claimed_at" DESC, "instance_id"
		) AS "winner_rank"
	FROM "participant_control_leases"
	WHERE "released_at" IS NULL
		AND "superseded_at" IS NULL
)
UPDATE "participant_control_leases" AS lease
SET "superseded_at" = now()
FROM ranked_current_leases AS ranked
WHERE lease."session_id" = ranked."session_id"
	AND lease."participant_id" = ranked."participant_id"
	AND lease."instance_id" = ranked."instance_id"
	AND ranked."winner_rank" > 1;--> statement-breakpoint
CREATE UNIQUE INDEX "participant_control_leases_current_unique" ON "participant_control_leases" USING btree ("session_id","participant_id") WHERE "participant_control_leases"."released_at" IS NULL AND "participant_control_leases"."superseded_at" IS NULL;
