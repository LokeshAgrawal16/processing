from mongodb_vinreco.mongodbops import *
from repo_logs import Logger
from _Base.eRetailBase import BaseClient
from _Clients.Skechers.Orders import OrdersProcessing
from _Clients.Skechers.OrdersReturns import OrdersReturnsProcessing


class Skechers(BaseClient):

    def process_data(self):
        try:
         OrdersProcessing(self.message).process_data()
         OrdersReturnsProcessing(self.message).process_data()
        except Exception as err:
            print(err)
            exception_message = str(err)
            exception_type, _, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            Logger('Skechers').error(
                f"{exception_message} {exception_type} {filename}, Line {exception_traceback.tb_lineno}")
            raise err

