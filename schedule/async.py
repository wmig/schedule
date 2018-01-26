import asyncio
from . import *


class Scheduler(Scheduler):
    """
    Objects instantiated by the :class:`Scheduler <Scheduler>` are
    factories to create jobs, keep record of scheduled jobs and
    handle their execution.
    """
    def __init__(self, loop=None):
        self.loop = loop
        super(Scheduler, self).__init__()

    @asyncio.coroutine
    def wait_empty(self):
        while self.jobs:
            yield

    @asyncio.coroutine
    def run_pending(self, delay_seconds=0):
        """
        Run all jobs that are scheduled to run.

        Please note that it is *intended behavior that run_pending()
        does not run missed jobs*. For example, if you've registered a job
        that should run every minute and you only call run_pending()
        in one hour increments then your job won't be run 60 times in
        between but only once.
        """
        while self.jobs:
            try:
                job = min(job for job in self.jobs if job.should_run)
            except ValueError:
                yield from asyncio.sleep(delay_seconds, loop=self.loop)
            else:
                yield from self._run_job(job)

    def every(self, interval=1):
        """
        Schedule a new periodic job.

        :param interval: A quantity of a certain time unit
        :return: An unconfigured :class:`Job <Job>`
        """
        job = Job(interval, self)
        return job

    @asyncio.coroutine
    def _run_job(self, job):
        ret = yield from job.run()
        if isinstance(ret, CancelJob) or ret is CancelJob:
            self.cancel_job(job)


class Job(Job):
    """
    A periodic job as used by :class:`Scheduler`.

    :param interval: A quantity of a certain time unit
    :param scheduler: The :class:`Scheduler <Scheduler>` instance that
                      this job will register itself with once it has
                      been fully configured in :meth:`Job.do()`.

    Every job runs at a given fixed time interval that is defined by:

    * a :meth:`time unit <Job.second>`
    * a quantity of `time units` defined by `interval`

    A job is usually created and returned by :meth:`Scheduler.every`
    method, which also defines its `interval`.
    """
    @asyncio.coroutine
    def run(self):
        """
        Run the job and immediately reschedule it.

        :return: The return value returned by the `job_func`
        """
        logger.info('Running job %s', self)
        ret = self.job_func()
        if asyncio.iscoroutine(ret):
            ret = yield from ret
        self.last_run = datetime.datetime.now()
        if self._times:
            self._times -= 1
        if self._times or self._times is None:
            self._schedule_next_run()
        else:
            self.cancel()
        return ret
