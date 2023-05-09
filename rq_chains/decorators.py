from rq import get_current_job
from rq.decorators import job
from rq.job import Job
from functools import wraps
from typing import TYPE_CHECKING, Callable, Dict, Optional, List, Any, Union, Iterable
from collections.abc import Collection
import logging
from .publisher import Publisher
from .subscriber import Subscriber

logger = logging.getLogger(__name__)

EMPTY_LIST = []

if TYPE_CHECKING:
    from rq import Queue

_subscribers = dict()


class publisher(job):  # noqa
    def __init__(
            self,
            queue: Union['Queue', str],
            channel_ids: Optional[Union[str, Iterable[str]]] = None,
            redis_pubsub_channels: Optional[Union[str, Iterable[str]]] = None,
            redis_streams: Optional[Union[str, Iterable[str]]] = None,
            publish_condition: Callable[[Any], Any] = None,
            # Default: anything except none or empty collection
            meta: Optional[Dict[Any, Any]] = None,
            **job_kwargs
    ):
        meta = _rq_chains_meta(meta)
        self.channel_ids = _process_channel_ids(channel_ids)
        self.redis_pubsub_channels = _process_channel_ids(redis_pubsub_channels)
        self.redis_streams = _process_channel_ids(redis_streams)
        self.publish_condition = publish_condition or _default_publish_condition
        job.__init__(self, queue, meta=meta, **job_kwargs)

    def __call__(self, f):
        def append_result_to_chain(channel_id, subscriber_job):
            publisher_job = get_current_job()
            subscriber_job.meta['rq_chains_predecessor'] = publisher_job.id
            subscriber_job.meta['rq_chains_channel_id'] = channel_id
            subscriber_job.save_meta()
            publisher_job.meta['rq_chains_successors'].append(subscriber_job.id)
            publisher_job.save_meta()

        def notify_subscriber_func(subscriber_func, result, channel_id):
            if subscriber_func.subscribe_condition(result):
                subscriber_args, subscriber_kwargs = subscriber_func.result_mapper(result)
                subscriber_job = subscriber_func.delay(*subscriber_args, **subscriber_kwargs)
                append_result_to_chain(channel_id, subscriber_job)

        def publish_result(result):
            try:
                if self.publish_condition(result):
                    for channel_id in self.channel_ids:
                        for subscriber_func in _subscribers.get(channel_id, EMPTY_LIST):

                else:
                    logger.debug(
                        f'skipping publishing results (publish condition is False) for job.id={get_current_job().id}',
                        exc_info=True)

                # TODO: redis_pubsub_channels & redis_streams
            except Exception as e:
                # the pubsub code should not corrupt the functions result,
                # however silently dropping an exception is not recommended
                # alternative way to report/propagate exception?
                logger.error(f'error publishing results for job.id={get_current_job().id}', exc_info=True)
            return result

        @wraps(f)
        def wrapper(*args, **kwargs):
            result = f(*args, **kwargs)
            publish_result(result)

        wrapper.publish_condition = self.publish_condition
        wrapped = job.__call__(self, wrapper)
        return wrapped


class subscriber(job):  # noqa
    def __init__(
            self,
            queue: Union['Queue', str],
            channel_ids: Optional[Union[str, Iterable[str]]] = None,
            result_mapper: Callable[[Any], Any] = lambda x: ((x,), {}),
            subscribe_condition: Callable[[Any], Any] = lambda x: True,  # Default: always true
            meta: Optional[Dict[Any, Any]] = None,
            **job_kwargs
    ):
        meta = _rq_chains_meta(meta)
        self.channel_ids = _process_channel_ids(channel_ids)
        self.result_mapper = result_mapper
        self.subscribe_condition = subscribe_condition
        job.__init__(self, queue, meta=meta, **job_kwargs)

    def __call__(self, f):
        f.result_mapper = self.result_mapper
        f.subscribe_condition = self.subscribe_condition
        ret = job.__call__(self, f)

        for channel_id in self.channel_ids:
            if channel_id not in _subscribers:
                _subscribers[channel_id] = list()
            _subscribers[channel_id].append(ret)

        return ret


def _process_channel_ids(channel_ids: Optional[Union[str, Iterable[str]]]):
    if channel_ids is None:
        return ()
    if isinstance(channel_ids, str):
        return channel_ids,
    return tuple(*channel_ids)


def _rq_chains_meta(meta):
    rq_chains_meta = dict(rq_chains_successors=[], rq_chains_predecessor=None, rq_chains_channel_id=None)
    meta = meta or dict()
    meta.update(rq_chains_meta)
    return meta


def walk_job_chain(parent_job: "Job", depth=0):
    parent_job = Job.fetch(parent_job.id, connection=parent_job.connection)
    successors, channel_id = parent_job.meta.get('rq_chains_successors', []), \
                             parent_job.meta.get('rq_chains_channel_id', [])

    ident = "  " * depth
    print(
        f"{ident}{parent_job.func_name}(*{parent_job.args}, **{parent_job.kwargs})={parent_job.return_value()}, {channel_id=}")
    for successor in successors:
        successor_job = Job.fetch(successor, connection=parent_job.connection)
        walk_job_chain(successor_job, depth=depth + 1)


def _default_publish_condition(result):
    if result is None:
        return False

    if isinstance(result, Collection) and not isinstance(result, (str, bytes)):
        return len(result) != 0

    return True
