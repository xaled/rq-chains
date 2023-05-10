# RQ Chains
RQ Chains is a tiny Python library that extends [RQ (Redis Queue)](https://python-rq.org/) with a publisher-subscriber model for job chains.
It allows jobs to publish results to channels and other jobs to subscribe to these channels to receive the results.
This enables the creation of chains of jobs where the output of one job is the input of the next.

## Installation
```bash
pip install rq-chains
```

## Quick Start
```python
# test_module.py
from rq import Queue
from redis import Redis
from rq_chains import publisher, subscriber

redis_conn = Redis(host='redis')
q = Queue(connection=redis_conn)


# Define a function that will publish results
@publisher(queue=q, channel_ids='add_result_channel')
def add(a, b):
    return a + b


# Define a function that will subscribe to the published results
@subscriber(queue=q, channel_ids='add_result_channel')
def square(n):
    return n * n


# main.py
from test_module import add
from rq_chains import walk_job_chain
from time import sleep

j = add.delay(2, 3)
sleep(1)
walk_job_chain(j)
# Recursively walks through a job chain and prints information about each job in the chain
# output:
# test_module.add(*(2, 3), **{}) = 5
#   test_module.square(*(5,), **{}) = 25 (channel_id='add_result_channel')

```

In this example, when you call `add.delay(2, 3)`, it will compute the result (5) and publish it to 'add_result_channel'.
The `square` function is a subscriber to 'add_result_channel', so it will automatically be called with the published result (5), and it will compute and return 25.

## Advanced Usage
RQ Chains offers extra features to tailor the behavior of publishers and subscribers.
For instance, you can specify custom conditions for when a publisher should share its result or when a subscriber should start.
You can also transform the publisher's result into a different set of arguments for the subscriber.
It's possible to set a limit on the maximum depth of job chains and introduce delays to the execution of subscriber jobs.
Moreover, you can publish results to a Redis PubSub channel or a Redis stream.

This documentation provides information about the 'publisher' and 'subscriber' decorators:

```
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
```


```
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
```

## License
RQ Chains is released under the [MIT License](/LICENSE).