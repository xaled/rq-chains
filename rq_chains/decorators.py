from rq import get_current_job
from rq.decorators import job
from rq.job import Job
from rq.serializers import DefaultSerializer
from functools import wraps
from typing import TYPE_CHECKING, Callable, Dict, Optional, List, Any, Union, Iterable
from collections.abc import Collection
import logging
from .chains_job import ChainsJobDecorator
import rq_chains.redis_ops as redis_ops

# from .publisher import Publisher
# from .subscriber import Subscriber

logger = logging.getLogger(__name__)
EMPTY_LIST = []
if TYPE_CHECKING:
    from rq import Queue

_subscribers = dict()


class publisher(ChainsJobDecorator):  # noqa
    def __init__(
            self,
            queue: Union['Queue', str],
            channel_ids: Optional[Union[str, Iterable[str]]] = None,
            redis_pubsub_channels: Optional[Union[str, Iterable[str]]] = None,
            redis_streams: Optional[Union[str, Iterable[str]]] = None,
            publish_condition: Callable[[Any], Any] = None,
            # Default: anything except none or empty collection
            # meta: Optional[Dict[Any, Any]] = None,
            **job_kwargs
    ):
        # meta = _rq_chains_meta(meta)
        self.channel_ids = _process_channel_ids(channel_ids)
        self.redis_pubsub_channels = _process_channel_ids(redis_pubsub_channels)
        self.redis_streams = _process_channel_ids(redis_streams)
        self.publish_condition = publish_condition or _default_publish_condition
        job.__init__(self, queue, **job_kwargs)

    def __call__(self, f):
        def notify_subscribers(result, publisher_job):
            def notify_subscriber_func(_subscriber_func, _channel_id):
                if not _subscriber_func.subscribe_condition(result):
                    logger.debug(
                        f'skipping notifying subscriber function (subscriber condition is False) '
                        f'for job.id={get_current_job().id}, {_subscriber_func.__name__=}')
                    return

                current_depth = publisher_job.meta.get('rq_chains_depth', 0)
                subscriber_args, subscriber_kwargs = _subscriber_func.result_mapper(result)
                rq_chains_opts = dict(predecessor=publisher_job.id,
                                      depth=current_depth + 1,
                                      channel_id=_channel_id)
                logger.info(f"enqueuing subscriber func subscriber={_subscriber_func=}, publisher={f.__name__}, "
                            f"channel_id={_channel_id}  {result=} {current_depth=}")
                subscriber_job = _subscriber_func.delay(*subscriber_args, rq_chains_opts=rq_chains_opts,
                                                        **subscriber_kwargs)
                publisher_job.meta['rq_chains_successors'].append(subscriber_job.id)
                publisher_job.save_meta()

            for channel_id in self.channel_ids:
                for subscriber_func in _subscribers.get(channel_id, EMPTY_LIST):
                    notify_subscriber_func(subscriber_func, channel_id)

        def publish_to_pubsub(result, publisher_job):
            if self.redis_pubsub_channels:
                message_data = DefaultSerializer.dumps(result)
                for pubsub_channel in self.redis_pubsub_channels:
                    redis_ops.publish_to_pubsub(publisher_job.connection, channel=pubsub_channel,
                                                message_data=message_data)

        def publish_to_stream(result, publisher_job):
            if self.redis_streams:
                message_dict = dict(data=DefaultSerializer.dumps(result))
                for stream in self.redis_streams:
                    redis_ops.publish_to_stream(publisher_job.connection, stream=stream, message_dict=message_dict)

        def publish_result(result):
            try:
                if not self.publish_condition(result):
                    logger.debug(
                        f'skipping publishing results (publish condition is False) for job.id={get_current_job().id}')
                    return

                publisher_job = get_current_job()
            except Exception as e:  # noqa
                # the publish_result code should not corrupt the functions result,
                # however silently dropping an exception is not recommended
                # alternative way to report/propagate exception?
                logger.error(f'Error checking publish_condition for job.id={get_current_job().id}', exc_info=True)
                return

            for sub_call in (notify_subscribers, publish_to_pubsub, publish_to_stream):
                try:
                    sub_call(result, publisher_job)  # noqa
                except Exception as e:  # noqa
                    # the publish_result code should not corrupt the functions result,
                    # however silently dropping an exception is not recommended
                    # alternative way to report/propagate exception?
                    logger.error(f'Error publishing errors for job.id={get_current_job().id} {sub_call.__name__=}',
                                 exc_info=True)

        @wraps(f)
        def wrapper(*args, **kwargs):
            result = f(*args, **kwargs)
            f.publish_result(result)

        f.publish_result = publish_result

        return ChainsJobDecorator.__call__(self, wrapper)


class subscriber(ChainsJobDecorator):  # noqa
    def __init__(
            self,
            queue: Union['Queue', str],
            channel_ids: Optional[Union[str, Iterable[str]]] = None,
            result_mapper: Callable[[Any], Any] = lambda x: ((x,), {}),
            subscribe_condition: Callable[[Any], Any] = lambda x: True,  # Default: always true
            # meta: Optional[Dict[Any, Any]] = None,
            **job_kwargs
    ):
        # meta = _rq_chains_meta(meta)
        self.channel_ids = _process_channel_ids(channel_ids)
        self.result_mapper = result_mapper
        self.subscribe_condition = subscribe_condition
        job.__init__(self, queue, **job_kwargs)

    def __call__(self, f):
        f.subscribe_condition = self.subscribe_condition
        f.result_mapper = self.result_mapper
        ret = ChainsJobDecorator.__call__(self, f)

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


# def _rq_chains_meta(meta):
#     rq_chains_meta = dict(rq_chains_successors=[], rq_chains_predecessor=None, rq_chains_channel_id=None)
#     meta = meta or dict()
#     meta.update(rq_chains_meta)
#     return meta


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
