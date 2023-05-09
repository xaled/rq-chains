from rq.job import Job
class ResultChain:
    def __init__(self, job, predecessor=None, channel_id=None, ):
        self.predecessor = None
        self.channel_id = None
        self.job = job
        self.successors = list()

    def append_result_chain(self, job, channel_id=None):
        if not hasattr(job, 'result_chains'):
            job.result_chains = ResultChain(job, predecessor=self.job, channel_id=channel_id)
        self.successors.append(result_chain)

    def __str__(self):
        if self.job:
            return_value, successors = self.job.return_value(), self.successors
            return f"ResultChain({self.job.return_value()=}"
