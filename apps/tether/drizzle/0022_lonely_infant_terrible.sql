ALTER TABLE "operator_grant_scopes" DROP CONSTRAINT "operator_grant_scopes_shape_check";--> statement-breakpoint
ALTER TABLE "browser_pairing_requests" ADD COLUMN "confirmed_by_subject" text;--> statement-breakpoint
ALTER TABLE "browser_pairing_requests" ADD CONSTRAINT "browser_pairing_requests_confirmation_check" CHECK (("browser_pairing_requests"."confirmed_at" IS NULL) = ("browser_pairing_requests"."confirmed_by_subject" IS NULL)
        AND ("browser_pairing_requests"."confirmed_at" IS NULL OR "browser_pairing_requests"."confirmed_at" >= "browser_pairing_requests"."created_at")
        AND ("browser_pairing_requests"."confirmed_by_subject" IS NULL OR char_length("browser_pairing_requests"."confirmed_by_subject") BETWEEN 1 AND 255));--> statement-breakpoint
ALTER TABLE "browser_pairing_requests" ADD CONSTRAINT "browser_pairing_requests_exchange_check" CHECK ("browser_pairing_requests"."exchanged_at" IS NULL OR (
        "browser_pairing_requests"."confirmed_at" IS NOT NULL
        AND "browser_pairing_requests"."exchanged_at" >= "browser_pairing_requests"."confirmed_at"
        AND "browser_pairing_requests"."exchanged_at" < "browser_pairing_requests"."expires_at"
      ));--> statement-breakpoint
ALTER TABLE "browser_pairing_requests" ADD CONSTRAINT "browser_pairing_requests_identity_check" CHECK (char_length("browser_pairing_requests"."operator_subject") BETWEEN 1 AND 255
        AND char_length("browser_pairing_requests"."origin") BETWEEN 1 AND 512
        AND char_length("browser_pairing_requests"."request_id") BETWEEN 1 AND 128
        AND char_length("browser_pairing_requests"."verification_phrase") BETWEEN 1 AND 128);--> statement-breakpoint
ALTER TABLE "browser_pairing_requests" ADD CONSTRAINT "browser_pairing_requests_invalidation_check" CHECK ("browser_pairing_requests"."invalidated_at" IS NULL OR (
        "browser_pairing_requests"."failed_attempts" = 5
        AND "browser_pairing_requests"."invalidated_at" >= "browser_pairing_requests"."created_at"
        AND "browser_pairing_requests"."invalidated_at" < "browser_pairing_requests"."expires_at"
      ));--> statement-breakpoint
ALTER TABLE "operator_grant_scopes" ADD CONSTRAINT "operator_grant_scopes_shape_check" CHECK (jsonb_typeof("operator_grant_scopes"."scope") = 'object'
        AND "operator_grant_scopes"."scope" ?& ARRAY['actions', 'commands', 'permissions', 'scopeKeys', 'sessionIds', 'targetKinds']
        AND "operator_grant_scopes"."scope" - ARRAY['actions', 'commands', 'permissions', 'scopeKeys', 'sessionIds', 'targetKinds'] = '{}'::jsonb
        AND jsonb_typeof("operator_grant_scopes"."scope"->'actions') = 'array'
        AND jsonb_typeof("operator_grant_scopes"."scope"->'commands') = 'array'
        AND jsonb_typeof("operator_grant_scopes"."scope"->'permissions') = 'array'
        AND jsonb_typeof("operator_grant_scopes"."scope"->'scopeKeys') = 'array'
        AND jsonb_typeof("operator_grant_scopes"."scope"->'sessionIds') = 'array'
        AND jsonb_typeof("operator_grant_scopes"."scope"->'targetKinds') = 'array');