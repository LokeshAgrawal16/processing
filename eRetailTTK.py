import os
import sys

from _Base.eRetailBase import BaseClient
from repo_logs import Logger


class TTK(BaseClient):
    def pre_process_data(self, data):
        try:
            return data
        except Exception as err:
            print(err)
            exception_message = str(err)
            exception_type, _, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            Logger('TTK').error(
                f"{exception_message} {exception_type} {filename}, Line {exception_traceback.tb_lineno}")
            raise err

    def process_data(self, data):
        try:
            return data
        except Exception as err:
            print(err)
            exception_message = str(err)
            exception_type, _, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            Logger('TTK').error(
                f"{exception_message} {exception_type} {filename}, Line {exception_traceback.tb_lineno}")
            raise err
