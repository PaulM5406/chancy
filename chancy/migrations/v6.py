from psycopg import sql

from chancy.migrate import Migration


class GeneralizedRateLimiting(Migration):
    """
    Generalize rate limiting to support global rate limit keys and partition keys.

    This migration:
    - Adds rate_limit_key column to queues table
    - Adds rate_limit_key and rate_limit_partition columns to jobs table
    - Creates new global_rate_limits table to replace queue-specific rate limiting
    - Migrates existing queue rate limits to the new system
    """

    async def up(self, migrator, cursor):
        # Drop legacy rate limit columns from queues table
        # and add rate_limit_key
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {queues}
                DROP COLUMN IF EXISTS rate_limit,
                DROP COLUMN IF EXISTS rate_limit_window,
                ADD COLUMN rate_limit_key VARCHAR(255) NOT NULL DEFAULT ''
                """
            ).format(queues=sql.Identifier(f"{migrator.prefix}queues"))
        )

        # Add rate limit columns to jobs table
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {jobs}
                ADD COLUMN rate_limit_key VARCHAR(255) NOT NULL DEFAULT '',
                ADD COLUMN rate_limit_partition_key VARCHAR(255) NOT NULL DEFAULT ''
                """
            ).format(jobs=sql.Identifier(f"{migrator.prefix}jobs"))
        )

        # Create new global rate limits table
        await cursor.execute(
            sql.SQL(
                """
                CREATE UNLOGGED TABLE {global_rate_limits} (
                    rate_limit_key VARCHAR(255) NOT NULL,
                    partition_key VARCHAR(255) NOT NULL DEFAULT '',
                    window_start INTEGER NOT NULL,
                    count INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (rate_limit_key, partition_key)
                );
                """
            ).format(
                global_rate_limits=sql.Identifier(
                    f"{migrator.prefix}global_rate_limits"
                )
            )
        )

        # Create new global rate limit configurations table
        await cursor.execute(
            sql.SQL(
                """
                CREATE TABLE {rate_limit_configs} (
                    rate_limit_key VARCHAR(255) PRIMARY KEY,
                    rate_limit INTEGER NOT NULL,
                    rate_limit_window INTEGER NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            ).format(
                rate_limit_configs=sql.Identifier(
                    f"{migrator.prefix}rate_limit_configs"
                )
            )
        )

        # Drop the legacy queue_rate_limits table
        await cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {queue_rate_limits}").format(
                queue_rate_limits=sql.Identifier(
                    f"{migrator.prefix}queue_rate_limits"
                )
            )
        )

        # Function 1: Reserve queue rate limit capacity
        await cursor.execute(
            sql.SQL(
                """
                CREATE OR REPLACE FUNCTION {reserve_queue_rate_limit_func}(
                    p_rate_limit_key VARCHAR(255),
                    p_rate_limit INTEGER,
                    p_rate_limit_window INTEGER,
                    p_max_jobs INTEGER,
                    p_current_time INTEGER
                )
                RETURNS INTEGER
                LANGUAGE plpgsql
                AS $$
                DECLARE
                    v_queue_window_start INTEGER;
                    v_available_capacity INTEGER;
                    v_reserved_capacity INTEGER;
                BEGIN
                    v_queue_window_start := p_current_time - (p_current_time % p_rate_limit_window);
                    
                    -- Get current usage
                    SELECT p_rate_limit - COALESCE(count, 0) INTO v_available_capacity
                    FROM {global_rate_limits}
                    WHERE rate_limit_key = p_rate_limit_key 
                    AND partition_key = ''
                    AND window_start = v_queue_window_start;

                    -- If no row exists, use full capacity
                    IF v_available_capacity IS NULL THEN
                        v_available_capacity := p_rate_limit;
                    END IF;

                    -- If no capacity, return 0
                    IF v_available_capacity <= 0 THEN
                        RETURN 0;
                    END IF;
                    
                    -- Reserve capacity (min of requested and available)
                    v_reserved_capacity := LEAST(p_max_jobs, v_available_capacity);
                    
                    -- Atomically reserve this capacity
                    INSERT INTO {global_rate_limits} (
                        rate_limit_key, partition_key, window_start, count
                    )
                    VALUES (p_rate_limit_key, '', v_queue_window_start, v_reserved_capacity)
                    ON CONFLICT (rate_limit_key, partition_key) DO UPDATE
                    SET count = CASE 
                            WHEN {global_rate_limits}.window_start = EXCLUDED.window_start 
                            THEN {global_rate_limits}.count + EXCLUDED.count
                            ELSE EXCLUDED.count 
                        END,
                        window_start = EXCLUDED.window_start;

                    RETURN v_reserved_capacity;
                END;
                $$;
                """
            ).format(
                reserve_queue_rate_limit_func=sql.Identifier(
                    f"{migrator.prefix}reserve_queue_rate_limit"
                ),
                global_rate_limits=sql.Identifier(
                    f"{migrator.prefix}global_rate_limits"
                ),
            )
        )

        # Function 2: Select and process jobs with rate limiting
        await cursor.execute(
            sql.SQL(
                """
                CREATE OR REPLACE FUNCTION {select_jobs_func}(
                    p_queue_name VARCHAR(255),
                    p_max_jobs INTEGER,
                    p_worker_id VARCHAR(255),
                    p_current_time INTEGER,
                    p_reschedule_delay_seconds DOUBLE PRECISION
                )
                RETURNS TABLE(
                    id UUID,
                    queue TEXT,
                    func TEXT,
                    kwargs JSONB,
                    limits JSONB,
                    meta JSONB,
                    state VARCHAR (25),
                    priority INTEGER,
                    max_attempts INTEGER,
                    taken_by TEXT,
                    attempts INTEGER,
                    created_at TIMESTAMPTZ,
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ,
                    scheduled_at TIMESTAMPTZ,
                    unique_key TEXT,
                    errors JSONB,
                    rate_limit_key VARCHAR(255),
                    rate_limit_partition_key VARCHAR(255)
                )
                LANGUAGE plpgsql
                AS $$
                DECLARE
                    v_job_group RECORD;
                    v_job_window_start INTEGER;
                    v_available_capacity INTEGER;
                    v_capacity INTEGER;
                    v_eligible_job_ids UUID[] := ARRAY[]::UUID[];
                    v_non_eligible_job_ids UUID[] := ARRAY[]::UUID[];
                BEGIN
                    -- Process jobs grouped by rate limit key and partition
                    -- Limit to reserved_jobs for efficiency
                    FOR v_job_group IN 
                        WITH candidate_jobs AS (
                            SELECT 
                                j.id,
                                j.priority,
                                j.rate_limit_key,
                                j.rate_limit_partition_key,
                                rc.rate_limit,
                                rc.rate_limit_window
                            FROM {jobs} j
                            LEFT JOIN {rate_limit_configs} rc ON j.rate_limit_key = rc.rate_limit_key
                            WHERE
                                j.queue = p_queue_name
                            AND
                                (j.state = 'pending' OR j.state = 'retrying')
                            AND
                                j.attempts < j.max_attempts
                            AND
                                (j.scheduled_at IS NULL OR j.scheduled_at <= NOW())
                            ORDER BY
                                j.priority DESC,
                                j.id ASC
                            LIMIT p_max_jobs
                            FOR UPDATE OF j SKIP LOCKED
                        )
                        SELECT 
                            cj.rate_limit_key,
                            cj.rate_limit_partition_key,
                            cj.rate_limit,
                            cj.rate_limit_window,
                            array_agg(cj.id ORDER BY cj.priority DESC, cj.id ASC) as job_ids,
                            count(*) as job_count
                        FROM candidate_jobs cj
                        GROUP BY cj.rate_limit_key, cj.rate_limit_partition_key, cj.rate_limit, cj.rate_limit_window
                    LOOP
                        -- Check job-level rate limiting
                        IF v_job_group.rate_limit_key != '' AND v_job_group.rate_limit IS NOT NULL THEN
                            v_job_window_start := p_current_time - (p_current_time % v_job_group.rate_limit_window);
                            
                            -- Get current rate limit usage for this group
                            SELECT v_job_group.rate_limit - gl.count INTO v_available_capacity
                            FROM {global_rate_limits} gl
                            WHERE gl.rate_limit_key = v_job_group.rate_limit_key
                            AND gl.partition_key = v_job_group.rate_limit_partition_key
                            AND gl.window_start = v_job_window_start;
                            
                            -- If no row exists, use full capacity
                            IF v_available_capacity IS NULL THEN
                                v_available_capacity := v_job_group.rate_limit;
                            END IF;

                            -- Calculate how many jobs from this group we can process
                            v_capacity := LEAST(v_job_group.job_count, v_available_capacity);

                            -- Add eligible jobs
                            FOR i IN 1..v_capacity LOOP
                                v_eligible_job_ids := array_append(v_eligible_job_ids, v_job_group.job_ids[i]);
                            END LOOP;
                            
                            -- Add non-eligible jobs to reschedule
                            FOR i IN (v_capacity + 1)..v_job_group.job_count LOOP
                                v_non_eligible_job_ids := array_append(v_non_eligible_job_ids, v_job_group.job_ids[i]);
                            END LOOP;
                            
                            -- Update global rate limit table immediately for this group
                            IF v_capacity > 0 THEN
                                INSERT INTO {global_rate_limits} AS gl (
                                    rate_limit_key, partition_key, window_start, count
                                )
                                VALUES (
                                    v_job_group.rate_limit_key, 
                                    v_job_group.rate_limit_partition_key, 
                                    v_job_window_start, 
                                    v_capacity
                                )
                                ON CONFLICT ((gl.rate_limit_key), (gl.partition_key)) DO UPDATE
                                SET count = CASE
                                        WHEN gl.window_start = EXCLUDED.window_start
                                        THEN gl.count + EXCLUDED.count
                                        ELSE EXCLUDED.count
                                    END,
                                    window_start = EXCLUDED.window_start;
                            END IF;
                        ELSE
                            -- No job-level rate limiting, add all jobs up to quota
                            FOR i IN 1..v_job_group.job_count LOOP
                                v_eligible_job_ids := array_append(v_eligible_job_ids, v_job_group.job_ids[i]);
                            END LOOP;
                        END IF;
                    END LOOP;

                    -- Reschedule non-eligible jobs
                    IF array_length(v_non_eligible_job_ids, 1) > 0 THEN
                        UPDATE {jobs} 
                        SET scheduled_at = NOW() + make_interval(secs => p_reschedule_delay_seconds)
                        WHERE {jobs}.id = ANY(v_non_eligible_job_ids);
                    END IF;

                    -- Mark eligible jobs as running and return them directly
                    IF array_length(v_eligible_job_ids, 1) > 0 THEN
                        RETURN QUERY
                        UPDATE {jobs} j
                        SET
                            started_at = NOW(),
                            state = 'running',
                            taken_by = p_worker_id
                        WHERE j.id = ANY(v_eligible_job_ids)
                        RETURNING 
                            j.id,
                            j.queue,
                            j.func,
                            j.kwargs,
                            j.limits,
                            j.meta,
                            j.state,
                            j.priority,
                            j.max_attempts,
                            j.taken_by,
                            j.attempts,
                            j.created_at,
                            j.started_at,
                            j.completed_at,
                            j.scheduled_at,
                            j.unique_key,
                            j.errors,
                            j.rate_limit_key,
                            j.rate_limit_partition_key;
                    END IF;
                END;
                $$;
                """
            ).format(
                select_jobs_func=sql.Identifier(
                    f"{migrator.prefix}select_jobs_with_rate_limits"
                ),
                jobs=sql.Identifier(f"{migrator.prefix}jobs"),
                global_rate_limits=sql.Identifier(
                    f"{migrator.prefix}global_rate_limits"
                ),
                rate_limit_configs=sql.Identifier(
                    f"{migrator.prefix}rate_limit_configs"
                ),
            )
        )

        # Function 3: Adjust queue rate limit based on actual usage
        await cursor.execute(
            sql.SQL(
                """
                CREATE OR REPLACE FUNCTION {adjust_queue_func}(
                    p_queue_rate_limit_key VARCHAR(255),
                    p_reserved_jobs INTEGER,
                    p_actual_jobs INTEGER
                )
                RETURNS VOID
                LANGUAGE plpgsql
                AS $$
                BEGIN
                    -- Only adjust if there's a difference and we have a rate limit key
                    IF p_queue_rate_limit_key IS NOT NULL AND p_reserved_jobs != p_actual_jobs THEN
                        UPDATE {global_rate_limits}
                        SET count = count - (p_reserved_jobs - p_actual_jobs)
                        WHERE {global_rate_limits}.rate_limit_key = p_queue_rate_limit_key 
                        AND {global_rate_limits}.partition_key = '';
                    END IF;
                END;
                $$;
                """
            ).format(
                adjust_queue_func=sql.Identifier(
                    f"{migrator.prefix}adjust_queue_rate_limit"
                ),
                global_rate_limits=sql.Identifier(
                    f"{migrator.prefix}global_rate_limits"
                ),
            )
        )

    async def down(self, migrator, cursor):
        # Drop the PostgreSQL functions
        await cursor.execute(
            sql.SQL("DROP FUNCTION IF EXISTS {func_name}").format(
                func_name=sql.Identifier(
                    f"{migrator.prefix}reserve_queue_rate_limit"
                )
            )
        )
        await cursor.execute(
            sql.SQL("DROP FUNCTION IF EXISTS {func_name}").format(
                func_name=sql.Identifier(
                    f"{migrator.prefix}select_jobs_with_rate_limits"
                )
            )
        )
        await cursor.execute(
            sql.SQL("DROP FUNCTION IF EXISTS {func_name}").format(
                func_name=sql.Identifier(
                    f"{migrator.prefix}adjust_queue_rate_limit"
                )
            )
        )

        # Recreate the legacy queue_rate_limits table
        await cursor.execute(
            sql.SQL(
                """
                CREATE UNLOGGED TABLE {queue_rate_limits} (
                    queue VARCHAR(255) PRIMARY KEY,
                    window_start INTEGER NOT NULL,
                    count INTEGER NOT NULL DEFAULT 0
                );
                """
            ).format(
                queue_rate_limits=sql.Identifier(
                    f"{migrator.prefix}queue_rate_limits"
                )
            )
        )

        # Drop the new tables
        await cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {rate_limit_configs}").format(
                rate_limit_configs=sql.Identifier(
                    f"{migrator.prefix}rate_limit_configs"
                )
            )
        )
        await cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {global_rate_limits}").format(
                global_rate_limits=sql.Identifier(
                    f"{migrator.prefix}global_rate_limits"
                )
            )
        )

        # Remove new columns from jobs table
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {jobs}
                DROP COLUMN rate_limit_key,
                DROP COLUMN rate_limit_partition_key
                """
            ).format(jobs=sql.Identifier(f"{migrator.prefix}jobs"))
        )

        # Remove rate_limit_key column from queues table
        # and add rate_limit and rate_limit_window columns back
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {queues}
                DROP COLUMN rate_limit_key,
                ADD COLUMN rate_limit INTEGER,
                ADD COLUMN rate_limit_window INTEGER
                """
            ).format(queues=sql.Identifier(f"{migrator.prefix}queues"))
        )
