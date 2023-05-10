from functools import wraps
from datetime import timedelta
from typing import TYPE_CHECKING, Callable, Optional, Any, Union, Iterable
from collections.abc import Collection
import logging

from rq import get_current_job
from rq.decorators import job
from rq.serializers import DefaultSerializer

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
            **job_kwargs
    ):
        """
        A decorator class for RQ jobs to implement the publisher functionality in a publisher-subscriber model.

        Args:
            queue (Union[Queue, str]): The RQ queue instance or queue name to enqueue the job.
            channel_ids (Optional[Union[str, Iterable[str]]], optional): An iterable of channel IDs to publish to
                or a single channel ID as a string. Defaults to None.
            redis_pubsub_channels (Optional[Union[str, Iterable[str]]], optional): An iterable of Redis PubSub
                channel IDs to publish to or a single channel ID as a string. Defaults to None.
            redis_streams (Optional[Union[str, Iterable[str]]], optional): An iterable of Redis Stream names to publish
                to or a single stream name as a string. Defaults to None.
            publish_condition (Callable[[Any], Any], optional): A function that determines whether the publisher should
                publish its result. Defaults to a function returning the result as the first argument.
            **job_kwargs: Additional keyword arguments to pass to the RQ job decorator.

        Returns:
            A RQ job wrapped function that publishes to the specified channels.
        """

        self.channel_ids = _process_channel_ids(channel_ids)
        self.redis_pubsub_channels = _process_channel_ids(redis_pubsub_channels)
        self.redis_streams = _process_channel_ids(redis_streams)
        self.publish_condition = publish_condition or _default_publish_condition
        job.__init__(self, queue, **job_kwargs)

    def __call__(self, f):
        def notify_subscribers(result, publisher_job):
            def notify_subscriber_func(_subscriber_func, _channel_id):
                # check depth limit
                current_depth = publisher_job.meta.get('rq_chains_depth', 0)
                if _subscriber_func.depth_limit is not None and current_depth >= _subscriber_func.depth_limit:
                    logger.warning(f'Skipping notifying subscriber function (depth limit is reached) '
                                   f'for job.id={get_current_job().id}, {_subscriber_func.__name__=}')
                    return

                # check subscribe condition
                if not _subscriber_func.subscribe_condition(result):
                    logger.debug(
                        f'Skipping notifying subscriber function (subscriber condition is False) '
                        f'for job.id={get_current_job().id}, {_subscriber_func.__name__=}')
                    return

                subscriber_args, subscriber_kwargs = _subscriber_func.result_mapper(result)
                rq_chains_opts = dict(predecessor=publisher_job.id,
                                      depth=current_depth + 1,
                                      channel_id=_channel_id,
                                      delay_timedelta=_subscriber_func.delay_timedelta
                                      )
                logger.warning(f"enqueuing subscriber func subscriber={_subscriber_func=}, publisher={f.__name__}, "
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
            return result

        f.publish_result = publish_result

        return ChainsJobDecorator.__call__(self, wrapper)


class subscriber(ChainsJobDecorator):  # noqa
    def __init__(
            self,
            queue: Union['Queue', str],
            channel_ids: Optional[Union[str, Iterable[str]]] = None,
            result_mapper: Callable[[Any], Any] = lambda x: ((x,), {}),
            subscribe_condition: Callable[[Any], Any] = lambda x: True,  # Default: always true
            depth_limit: Optional[int] = 1000,
            delay_timedelta: Optional[int | float | timedelta] = None,
            **job_kwargs
    ):
        """
        A decorator class for RQ jobs to implement the subscriber functionality in a publisher-subscriber model.

        Args:
            queue (Union[Queue, str]): The RQ queue instance or queue name to enqueue the job.
            channel_ids (Optional[Union[str, Iterable[str]]], optional): An iterable of channel IDs to subscribe to
                or a single channel ID as a string. Defaults to None.
            result_mapper (Callable[[Any], Any], optional): A function that maps the publisher's result
                to the subscriber's arguments and keyword arguments. Defaults to a function returning the result
                as the first argument.
            subscribe_condition (Callable[[Any], Any], optional): A function that determines whether the subscriber
                should be executed based on the publisher's result. Defaults to always True.
            depth_limit (Optional[int], optional): The maximum depth of job chains that can be created
                by chained publishers and subscribers. Defaults to 1000.
            delay_timedelta (Optional[Union[int, float, timedelta]], optional): The time duration to delay the job
                execution, can be a number of seconds (int or float) or a timedelta object. Defaults to None.
            **job_kwargs: Additional keyword arguments to pass to the RQ job decorator.

        Returns:
            A RQ job wrapped function that subscribes to the specified channels.
        """
        self.channel_ids = _process_channel_ids(channel_ids)
        self.result_mapper = result_mapper
        self.subscribe_condition = subscribe_condition
        self.depth_limit = depth_limit
        self.delay_timedelta = timedelta(seconds=delay_timedelta) if isinstance(delay_timedelta, (float, int)) \
            else delay_timedelta
        job.__init__(self, queue, **job_kwargs)

    def __call__(self, f):
        f.subscribe_condition = self.subscribe_condition
        f.result_mapper = self.result_mapper
        f.depth_limit = self.depth_limit
        f.delay_timedelta = self.delay_timedelta
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


def _default_publish_condition(result):
    if result is None:
        return False

    if isinstance(result, Collection) and not isinstance(result, (str, bytes)):
        return len(result) != 0

    return True
