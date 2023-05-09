from rq.decorators import job
from functools import wraps


class ChainsJobDecorator(job):
    def __call__(self, f):
        @wraps(f)
        def delay(*args, **kwargs):
            if isinstance(self.queue, str):
                queue = self.queue_class(name=self.queue, connection=self.connection)
            else:
                queue = self.queue

            depends_on = kwargs.pop('depends_on', None)
            job_id = kwargs.pop('job_id', None)
            at_front = kwargs.pop('at_front', False)
            rq_chains_opts = kwargs.pop('rq_chains_opts', dict())
            meta = dict(self.meta) if self.meta else dict()

            meta['rq_chains_predecessor'] = rq_chains_opts.get('predecessor', None)
            meta['rq_chains_depth'] = rq_chains_opts.get('depth', 0)
            meta['rq_chains_channel_id'] = rq_chains_opts.get('channel_id', None)
            meta['rq_chains_successors'] = list()

            if not depends_on:
                depends_on = self.depends_on

            if not at_front:
                at_front = self.at_front

            return queue.enqueue_call(
                f,
                args=args,
                kwargs=kwargs,
                timeout=self.timeout,
                result_ttl=self.result_ttl,
                ttl=self.ttl,
                depends_on=depends_on,
                job_id=job_id,
                at_front=at_front,
                meta=meta,
                description=self.description,
                failure_ttl=self.failure_ttl,
                retry=self.retry,
                on_failure=self.on_failure,
                on_success=self.on_success,
            )

        f.delay = delay
        return f
