from rq.job import Job


def walk_job_chain(parent_job: "Job", depth=0):
    """Recursively walks through a job chain and prints information about each job in the chain."""

    parent_job = Job.fetch(parent_job.id, connection=parent_job.connection)
    successors, channel_id = (
        parent_job.meta.get('rq_chains_successors', []), parent_job.meta.get('rq_chains_channel_id', [])
    )

    ident = "  " * depth
    status = parent_job.get_status()
    result = f'{parent_job.return_value()}' if status == 'finished' else f'({status=})'
    if channel_id:
        result += f" ({channel_id=})"

    print(
        f"{ident}{parent_job.func_name}(*{parent_job.args}, **{parent_job.kwargs}) = {result}")
    for successor in successors:
        successor_job = Job.fetch(successor, connection=parent_job.connection)
        walk_job_chain(successor_job, depth=depth + 1)
