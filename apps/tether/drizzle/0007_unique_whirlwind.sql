CREATE TABLE "task_approvals" (
	"approval_event_id" text NOT NULL,
	"decided_at" timestamp with time zone DEFAULT now() NOT NULL,
	"decided_by_participant_id" text NOT NULL,
	"decision" text NOT NULL,
	"reason" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"session_id" text NOT NULL,
	"target_key" text NOT NULL,
	"task_id" text NOT NULL,
	CONSTRAINT "task_approvals_session_id_task_id_target_key_pk" PRIMARY KEY("session_id","task_id","target_key"),
	CONSTRAINT "task_approvals_event_id_unique" UNIQUE("approval_event_id")
);
--> statement-breakpoint
ALTER TABLE "task_approvals" ADD CONSTRAINT "task_approvals_task_fk" FOREIGN KEY ("session_id","task_id") REFERENCES "public"."tasks"("session_id","task_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "task_approvals_task_decided_idx" ON "task_approvals" USING btree ("session_id","task_id","decided_at");
--> statement-breakpoint
INSERT INTO "task_approvals" (
	"approval_event_id",
	"decided_at",
	"decided_by_participant_id",
	"decision",
	"reason",
	"session_id",
	"target_key",
	"task_id"
)
SELECT DISTINCT ON (candidate.session_id, candidate.task_id, candidate.target_key)
	candidate.event_id,
	candidate.created_at,
	candidate.participant_id,
	candidate.decision,
	candidate.reason,
	candidate.session_id,
	candidate.target_key,
	candidate.task_id
FROM (
	SELECT
		event_id,
		created_at,
		payload->>'participantId' AS participant_id,
		payload->>'decision' AS decision,
		COALESCE(payload->'reason', '{}'::jsonb) AS reason,
		session_id,
		CASE
			WHEN jsonb_typeof(payload->'reason'->'emailRecommendation') = 'object'
				AND COALESCE(payload->'reason'->'emailRecommendation'->>'messageId', '') <> ''
				AND COALESCE(payload->'reason'->'emailRecommendation'->>'action', '') <> ''
				THEN 'emailRecommendation:'
					|| (payload->'reason'->'emailRecommendation'->>'action')
					|| ':'
					|| (payload->'reason'->'emailRecommendation'->>'messageId')
			WHEN jsonb_typeof(payload->'reason'->'emailRecommendation') = 'object'
				AND COALESCE(payload->'reason'->'emailRecommendation'->>'messageId', '') <> ''
				THEN 'emailRecommendation:'
					|| (payload->'reason'->'emailRecommendation'->>'messageId')
			ELSE 'task'
		END AS target_key,
		payload#>>'{task,taskId}' AS task_id,
		seq
	FROM "session_events"
	WHERE type = 'approval.recorded'
		AND payload->>'decision' IN ('approved', 'rejected')
		AND COALESCE(payload->>'participantId', '') <> ''
		AND COALESCE(payload#>>'{task,taskId}', '') <> ''
) candidate
ORDER BY candidate.session_id, candidate.task_id, candidate.target_key, candidate.seq
ON CONFLICT ("session_id", "task_id", "target_key") DO NOTHING;
