# from .subscribers_map import subscribers_map
#
#
# class Publisher:
#     def __init__(self, func, decorator):
#         self.decorator = decorator
#         self.func = func
#
#         for channel_id in self.decorator.channel_ids:
#             if channel_id not in subscribers_map:
#                 subscribers_map[channel_id] = list()
#             subscribers_map[channel_id].append(self)
#
#     def __call__(self, *args, **kwargs):
#         return self.func(*args, **kwargs)
