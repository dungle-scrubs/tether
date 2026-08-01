ALTER TABLE "auth_grants" DROP CONSTRAINT "auth_grants_metadata_shape_check";--> statement-breakpoint
ALTER TABLE "auth_grants" ADD CONSTRAINT "auth_grants_metadata_shape_check" CHECK (jsonb_typeof("auth_grants"."metadata") = 'object'
        AND "auth_grants"."metadata" ? 'requestId'
        AND "auth_grants"."metadata" ? 'source'
        AND "auth_grants"."metadata" - ARRAY['requestId', 'source'] = '{}'::jsonb
        AND jsonb_typeof("auth_grants"."metadata"->'source') = 'string'
        AND "auth_grants"."metadata"->>'source' IN ('admin', 'bootstrap', 'browser', 'migration')
        AND (
          jsonb_typeof("auth_grants"."metadata"->'requestId') = 'null'
          OR (
            jsonb_typeof("auth_grants"."metadata"->'requestId') = 'string'
            AND "auth_grants"."metadata"->>'requestId' ~ '^req_[A-Za-z0-9_-]{1,120}$'
          )
        ));