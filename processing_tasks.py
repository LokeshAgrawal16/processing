from celery import Task
from celery.worker.request import Request
from celery.exceptions import SoftTimeLimitExceeded
import os
import sys

from _Isolate.eretailIsolate import IsolateTransfer
from celeryapp import app
from repo_logs import Logger
from _Processing.EretailProcessing import Processing


class MyRequest(Request):
    """A minimal custom request to log failures and hard time limits."""
    """Abstract base class for all tasks in my app."""

    def on_timeout(self, soft, timeout):
        super(MyRequest, self).on_timeout(soft, timeout)
        if not soft:
            Logger('MyRequest').error(f'A hard timeout was enforced for task %s {self.task.name}')

    def on_failure(self, exc_info, send_failed_event=True, return_ok=False):
        super(MyRequest, self).on_failure(exc_info, send_failed_event=send_failed_event,
                                          return_ok=return_ok)
        Logger('MyRequest').error(f'Failure detected for task %s {self.task.name}')


class MyTask(Task):
    Request = MyRequest

    def run(self, *args, **kwargs):
        pass

    def before_start(self, *task_id):
        Logger('MyTask').info("Task %s started" % task_id[0])

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        Logger('MyTask').info("Task %s %s with %s return value" % (task_id, status, retval))


@app.task(bind=True, base=MyTask)
def vinreco_works(self, message):
    try:
        if message['call_for'] == 'PROCESSING':
            print("==>> message: ", message)
            return_message = Processing(message).processing_factory()
            return return_message
        elif message['call_for'] == 'DATA_TRANSFER':
            return_message = IsolateTransfer(message).transfer_factory()
            return return_message
    except SoftTimeLimitExceeded:
        Logger('vinreco_works').info("soft limit exceeded")
    except Exception as err:
        print(err)
        exception_message = str(err)
        exception_type, _, exception_traceback = sys.exc_info()
        filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
        Logger('vinreco_works').error(
            f"{exception_message} {exception_type} {filename}, Line {exception_traceback.tb_lineno}")
        raise err
