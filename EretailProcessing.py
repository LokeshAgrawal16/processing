import time

from mongodb_vinreco.mongodbops import *
from _Clients.Faber.eRetailFaber import Faber
from _Clients.Skechers.eRetailSkechers import Skechers
from _Clients.TTK.eRetailTTK import TTK

from repo_logs import Logger


class Processing:
    def __init__(self, message):
        self.message = message
        self.subdomain = message["subdomain"]

    def processing_factory(self):
        clients = {
            'skechers': Skechers,
            'faber': Faber,
            'ttk': TTK
        }
        try:
            print("Processing.processing_factory ==>> self.message: ", self.message)
            print("==>> client: ", client)
            client = clients[self.subdomain](self.message)
            client.process_data()
        except Exception as err:
            exception_message = str(err)
            exception_type, _, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            Logger('Processing').warning(
                f"{exception_message} {exception_type} {filename}, Line {exception_traceback.tb_lineno}"
            )
