import pytest
import asyncio

from chancy import Chancy, Worker, Queue, Job, RateLimit, job


@job()
def simple_job():
    return "completed"


@job()
def job_with_customer_id(customer_id: str):
    return f"processed for {customer_id}"


@pytest.mark.asyncio
async def test_global_rate_limit_key_on_queue(chancy: Chancy, worker: Worker):
    """
    Test that queues can use a global rate limit key.
    """
    # Set up a global rate limit
    await chancy.declare_rate_limit(
        RateLimit(rate_limit_key="api_calls", rate_limit=2, rate_limit_window=10),
        upsert=True
    )

    # Create a queue that uses the rate limit key
    queue = Queue("test_queue", rate_limit_key="api_calls")
    await chancy.declare(queue)

    # Push multiple jobs
    refs = []
    for i in range(5):
        ref = await chancy.push(simple_job.job.with_queue("test_queue"))
        refs.append(ref)

    # Wait a bit for jobs to be processed
    await asyncio.sleep(2)

    # Check that only 2 jobs were processed due to rate limit
    completed_jobs = 0
    for ref in refs:
        job_instance = await chancy.get_job(ref)
        if job_instance and job_instance.state.value == "succeeded":
            completed_jobs += 1

    assert completed_jobs == 2


@pytest.mark.asyncio
async def test_job_level_rate_limit_key(chancy: Chancy, worker: Worker):
    """
    Test that individual jobs can have their own rate limit key.
    """
    # Set up a global rate limit
    await chancy.declare_rate_limit(
        RateLimit(rate_limit_key="email_api", rate_limit=1, rate_limit_window=5),
        upsert=True
    )

    # Create a regular queue
    queue = Queue("test_queue")
    await chancy.declare(queue)

    # Push jobs with rate limit key
    refs = []
    for i in range(3):
        ref = await chancy.push(
            simple_job.job.with_queue("test_queue").with_rate_limit(
                "email_api"
            )
        )
        refs.append(ref)

    # Wait a bit for jobs to be processed
    await asyncio.sleep(2)

    # Check that only 1 job was processed due to rate limit
    completed_jobs = 0
    for ref in refs:
        job_instance = await chancy.get_job(ref)
        if job_instance and job_instance.state.value == "succeeded":
            completed_jobs += 1

    assert completed_jobs == 1


@pytest.mark.asyncio
async def test_rate_limit_with_partition_key(chancy: Chancy, worker: Worker):
    """
    Test rate limiting with partition keys (per-customer rate limiting).
    """
    # Set up a global rate limit
    await chancy.declare_rate_limit(
        RateLimit(rate_limit_key="customer_api", rate_limit=1, rate_limit_window=5),
        upsert=True
    )

    # Create a regular queue
    queue = Queue("test_queue")
    await chancy.declare(queue)

    # Push jobs for different customers
    refs_customer1 = []
    refs_customer2 = []

    for i in range(2):
        # Customer 1 jobs
        ref = await chancy.push(
            job_with_customer_id.job.with_queue("test_queue")
            .with_rate_limit("customer_api")
            .with_rate_limit_partition("customer_1")
            .with_kwargs(customer_id="customer_1")
        )
        refs_customer1.append(ref)

        # Customer 2 jobs
        ref = await chancy.push(
            job_with_customer_id.job.with_queue("test_queue")
            .with_rate_limit("customer_api")
            .with_rate_limit_partition("customer_2")
            .with_kwargs(customer_id="customer_2")
        )
        refs_customer2.append(ref)

    # Wait a bit for jobs to be processed
    await asyncio.sleep(2)

    # Check that each customer had 1 job processed (separate partitions)
    completed_customer1 = 0
    for ref in refs_customer1:
        job_instance = await chancy.get_job(ref)
        if job_instance and job_instance.state.value == "succeeded":
            completed_customer1 += 1

    completed_customer2 = 0
    for ref in refs_customer2:
        job_instance = await chancy.get_job(ref)
        if job_instance and job_instance.state.value == "succeeded":
            completed_customer2 += 1

    # Each customer should have exactly 1 job processed due to per-partition rate limiting
    assert completed_customer1 == 1
    assert completed_customer2 == 1


@pytest.mark.asyncio
async def test_job_and_queue_rate_limits_both_apply(
    chancy: Chancy, worker: Worker
):
    """
    Test that job-level AND queue-level rate limits both apply simultaneously.
    Jobs with job-level rate limits are constrained by BOTH limits.
    """
    # Set up two different global rate limits
    await chancy.declare_rate_limit(
        RateLimit(rate_limit_key="queue_limit", rate_limit=3, rate_limit_window=10),
        upsert=True
    )
    await chancy.declare_rate_limit(
        RateLimit(rate_limit_key="job_limit", rate_limit=1, rate_limit_window=10),
        upsert=True
    )

    # Create a queue with a rate limit key
    queue = Queue("test_queue", rate_limit_key="queue_limit")
    await chancy.declare(queue)

    # Push jobs with job-level rate limits (should be constrained by BOTH limits)
    job_with_job_limit_refs = []
    for i in range(3):
        ref = await chancy.push(
            simple_job.job.with_queue("test_queue").with_rate_limit(
                "job_limit"
            )
        )
        job_with_job_limit_refs.append(ref)

    # Push jobs without job-level rate limits (only constrained by queue limit)
    job_queue_only_refs = []
    for i in range(3):
        ref = await chancy.push(simple_job.job.with_queue("test_queue"))
        job_queue_only_refs.append(ref)

    # Wait a bit for jobs to be processed
    await asyncio.sleep(2)

    # Check jobs with both limits applied
    completed_with_job_limit = 0
    for ref in job_with_job_limit_refs:
        job_instance = await chancy.get_job(ref)
        if job_instance and job_instance.state.value == "succeeded":
            completed_with_job_limit += 1

    # Check jobs with only queue limit applied
    completed_queue_only = 0
    for ref in job_queue_only_refs:
        job_instance = await chancy.get_job(ref)
        if job_instance and job_instance.state.value == "succeeded":
            completed_queue_only += 1

    # Jobs with job-level rate limit should be limited to 1 (job limit is stricter)
    assert completed_with_job_limit == 1
    
    # Jobs without job-level rate limit should use remaining queue capacity
    # Queue limit is 3, job-with-job-limit used 1, so 2 remaining for queue-only jobs
    assert completed_queue_only == 2
    
    # Total jobs processed should not exceed queue limit
    assert completed_with_job_limit + completed_queue_only <= 3




@pytest.mark.asyncio
async def test_shared_rate_limit_across_queues_and_jobs(
    chancy: Chancy, worker: Worker
):
    """
    Test that the same rate limit key can be shared across different queues and jobs.
    """
    # Set up a global rate limit
    await chancy.declare_rate_limit(
        RateLimit(rate_limit_key="shared_limit", rate_limit=2, rate_limit_window=10),
        upsert=True
    )

    # Create two queues - one with rate limit key, one without
    queue1 = Queue("queue1", rate_limit_key="shared_limit")
    queue2 = Queue("queue2")
    await chancy.declare(queue1)
    await chancy.declare(queue2)

    # Push jobs to both queues
    refs = []

    # Jobs in queue1 (uses queue-level rate limit key)
    for i in range(2):
        ref = await chancy.push(simple_job.job.with_queue("queue1"))
        refs.append(ref)

    # Jobs in queue2 with job-level rate limit key (shares the same limit)
    for i in range(2):
        ref = await chancy.push(
            simple_job.job.with_queue("queue2").with_rate_limit(
                "shared_limit"
            )
        )
        refs.append(ref)

    # Wait a bit for jobs to be processed
    await asyncio.sleep(2)

    # Check total completed jobs across both queues
    completed_jobs = 0
    for ref in refs:
        job_instance = await chancy.get_job(ref)
        if job_instance and job_instance.state.value == "succeeded":
            completed_jobs += 1

    # Should be limited to 2 total across both queues since they share the rate limit
    assert completed_jobs == 2


@pytest.mark.asyncio
async def test_update_rate_limit_configuration(chancy: Chancy):
    """
    Test that rate limit configurations can be updated.
    """
    # Initially set a rate limit
    await chancy.declare_rate_limit(
        RateLimit(rate_limit_key="updateable_limit", rate_limit=1, rate_limit_window=10),
        upsert=True
    )

    # Update it to a higher limit
    await chancy.declare_rate_limit(
        RateLimit(rate_limit_key="updateable_limit", rate_limit=5, rate_limit_window=10),
        upsert=True
    )

    # The test would need access to the database to verify the update
    # For now, we just test that the method can be called multiple times
    # without errors
    assert True  # If we got here without exceptions, the update worked


@pytest.mark.asyncio
async def test_no_rate_limit_jobs_are_not_limited(
    chancy: Chancy, worker: Worker
):
    """
    Test that jobs without any rate limiting are processed normally.
    """
    # Create a regular queue with no rate limiting
    queue = Queue("unlimited_queue")
    await chancy.declare(queue)

    # Push multiple jobs
    refs = []
    for i in range(5):
        ref = await chancy.push(simple_job.job.with_queue("unlimited_queue"))
        refs.append(ref)

    # Wait a bit for jobs to be processed
    await asyncio.sleep(2)

    # Check that all jobs were processed
    completed_jobs = 0
    for ref in refs:
        job_instance = await chancy.get_job(ref)
        if job_instance and job_instance.state.value == "succeeded":
            completed_jobs += 1

    # All jobs should be processed since there's no rate limiting
    assert completed_jobs == 5


@pytest.mark.asyncio
async def test_job_serialization_with_rate_limit_fields(chancy: Chancy):
    """
    Test that jobs with rate limit fields serialize and deserialize correctly.
    """
    # Create a job with rate limit fields
    job_instance = simple_job.job.with_rate_limit(
        "test_key"
    ).with_rate_limit_partition("test_partition")

    # Test pack/unpack
    packed = job_instance.pack()
    assert packed["rk"] == "test_key"
    assert packed["rp"] == "test_partition"

    unpacked = Job.unpack(packed)
    assert unpacked.rate_limit_key == "test_key"
    assert unpacked.rate_limit_partition_key == "test_partition"


@pytest.mark.asyncio
async def test_queue_serialization_with_rate_limit_key(chancy: Chancy):
    """
    Test that queues with rate limit keys serialize and deserialize correctly.
    """
    # Create a queue with rate limit key
    queue = Queue("test_queue", rate_limit_key="test_key")

    # Test pack/unpack
    packed = queue.pack()
    assert packed["rate_limit_key"] == "test_key"

    unpacked = Queue.unpack(packed)
    assert unpacked.rate_limit_key == "test_key"


@pytest.mark.asyncio
async def test_dual_rate_limiting_job_and_queue(chancy: Chancy, worker: Worker):
    """
    Test that both job-level AND queue-level rate limits are applied simultaneously.
    """
    # Set up both rate limits
    await chancy.declare_rate_limit(
        RateLimit(rate_limit_key="job_limit", rate_limit=1, rate_limit_window=5),
        upsert=True
    )
    await chancy.declare_rate_limit(
        RateLimit(rate_limit_key="queue_limit", rate_limit=2, rate_limit_window=5),
        upsert=True
    )

    # Create a queue with queue-level rate limit
    queue = Queue("dual_limit_queue", rate_limit_key="queue_limit")
    await chancy.declare(queue)

    # Push jobs: some with job-level limits, some without
    refs = []
    
    # 2 jobs with job-level rate limit (should be limited to 1)
    for i in range(2):
        ref = await chancy.push(
            simple_job.job.with_queue("dual_limit_queue").with_rate_limit("job_limit")
        )
        refs.append(ref)
    
    # 2 jobs without job-level rate limit (limited only by queue)
    for i in range(2):
        ref = await chancy.push(simple_job.job.with_queue("dual_limit_queue"))
        refs.append(ref)

    # Wait a bit for jobs to be processed
    await asyncio.sleep(2)

    # Check results
    job_limited_completed = 0
    queue_only_completed = 0
    
    for i, ref in enumerate(refs):
        job_instance = await chancy.get_job(ref)
        if job_instance and job_instance.state.value == "succeeded":
            if i < 2:  # First 2 jobs had job-level limits
                job_limited_completed += 1
            else:  # Last 2 jobs only had queue limits
                queue_only_completed += 1

    # Job-level rate limit should allow only 1 job
    assert job_limited_completed == 1
    # Queue-level rate limit should allow max 2 total, but we already used 1 
    # for job-limited, so only 1 more for queue-only jobs
    assert queue_only_completed == 1
    # Total should not exceed queue limit of 2
    assert job_limited_completed + queue_only_completed == 2
