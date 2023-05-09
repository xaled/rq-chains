# from functools import wraps
# from rq import get_current_job
# from .subscribers_map import subscribers_map
# from .redis_ops import publish_to_pubsub, publish_to_stream
# from rq.serializers import DefaultSerializer
# import logging
#
# EMPTY_LIST = []
# logger = logging.getLogger(__name__)
#
#
# class Subscriber:
#     def __init__(self, func, decorator):
#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             result = func(*args, **kwargs)
#             self.publish_result(result)
#
#         self.func = wrapper
#         self.decorator = decorator
#
#     def notify_subscribers(self, result, publisher_job):
#         def notify_subscriber_func(_subscriber_func, _channel_id):
#             if not _subscriber_func.subscribe_condition(result):
#                 logger.debug(
#                     f'skipping notifying subscriber function (subscriber condition is False) '
#                     f'for job.id={get_current_job().id}, {_subscriber_func.func.__name__=}')
#                 return
#
#             subscriber_args, subscriber_kwargs = _subscriber_func.decorator.result_mapper(result)
#             rq_chains_opts = dict(predecessor=publisher_job.id, depth=publisher_job.meta.get('rq_chains_depth', 0) + 1,
#                                   channel_id=_channel_id)
#             subscriber_job = _subscriber_func.delay(*subscriber_args, rq_chains_opts=rq_chains_opts,
#                                                     **subscriber_kwargs)
#             publisher_job.meta['rq_chains_successors'].append(subscriber_job.id)
#             publisher_job.save_meta()
#
#         for channel_id in self.decorator.channel_ids:
#             for subscriber_func in subscribers_map.get(channel_id, EMPTY_LIST):
#                 notify_subscriber_func(subscriber_func, channel_id)
#
#     def publish_to_pubsub(self, result, publisher_job):
#         if self.decorator.redis_pubsub_channels:
#             message_data = DefaultSerializer.dumps(result)
#             for pubsub_channel in self.decorator.redis_pubsub_channels:
#                 publish_to_pubsub(publisher_job.connection, channel=pubsub_channel,
#                                   message_data=message_data)
#
#     def publish_to_stream(self, result, publisher_job):
#         if self.decorator.redis_streams:
#             message_dict = dict(data=DefaultSerializer.dumps(result))
#             for stream in self.decorator.redis_streams:
#                 publish_to_stream(publisher_job.connection, stream=stream, message_dict=message_dict)
#
#     def publish_result(self, result):
#         try:
#             if not self.decorator.publish_condition(result):
#                 logger.debug(
#                     f'skipping publishing results (publish condition is False) for job.id={get_current_job().id}')
#                 return
#
#             publisher_job = get_current_job()
#         except Exception as e:  # noqa
#             # the publish_result code should not corrupt the functions result,
#             # however silently dropping an exception is not recommended
#             # alternative way to report/propagate exception?
#             logger.error(f'Error checking publish_condition for job.id={get_current_job().id}', exc_info=True)
#             return
#
#         for sub_call in (self.notify_subscribers, self.publish_to_pubsub, self.publish_to_stream):
#             try:
#                 sub_call(result, publisher_job)  # noqa
#             except Exception as e:  # noqa
#                 # the publish_result code should not corrupt the functions result,
#                 # however silently dropping an exception is not recommended
#                 # alternative way to report/propagate exception?
#                 logger.error(f'Error publishing errors for job.id={get_current_job().id} {sub_call.__name__=}',
#                              exc_info=True)
#
#     def __call__(self, *args, **kwargs):
#         return self.func(*args, **kwargs)
