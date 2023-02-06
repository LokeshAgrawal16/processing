import sys

from mongodb_vinreco.mongodbops import *
from Global import Message, ProcessCollNames, IsolateCollNames,eRetail, DBURL, DB
from repo_logs import Logger
from datetime import datetime
import redis


class IsolateTransfer:

    def __init__(self, message):
        self.message = message
        self.messageid = message.get("messageid")
        self.client_id = message.get("client_id")
        self.operation = message.get("operation")
        self.subdomain = message.get("subdomain")
        self.date = message.get("date")
        self.records = []
        self.redis_conn = redis.Redis(host=os.environ.get("REDIS_CONNECTION"))
        print("IsolateTransfer ==>> self.redis_conn: ", self.redis_conn)

    def fetch_data(self):
        try:
            print('IsolateTransfer fetch ==>', DBURL.fetch_data_url, DB.fetch_data_db)
            client = MongoDBConnector(DBURL.fetch_data_url, DB.fetch_data_db)
            client.make_connection_url()
            data = client.find_query(ProcessCollNames[self.operation], {
                "apisubdomain": self.subdomain,
                "endt": self.datedserewo98
            })
            print(ProcessCollNames[self.operation], {
                "apisubdomain": self.subdomain,
                "endt": self.date
            })
            return data
        except Exception as err:
            exception_message = str(err)
            exception_type, _, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            Logger('IsolateTransfer').error(
                f"{exception_message} {exception_type} {filename}, Line {exception_traceback.tb_lineno}")
            raise err

    def push_data(self, data):
        try:
            print('IsolateTransfer push :', DBURL.push_data_isolate_url, DB.push_data_isolate_db)
            client = MongoDBConnector(DBURL.push_data_isolate_url, DB.push_data_isolate_db)
            client.make_connection_url()
            client.bulk_write(IsolateCollNames[self.operation], data)
        except DuplicateKeyError as err:
            exception_message = str(err)
            exception_type, _, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            Logger("IsolateTransfer").error(
                f"{exception_message} {exception_type} {filename}, Line {exception_traceback.tb_lineno}"
            )

    def transfer_factory(self):
        try:
            data = self.fetch_data()
            self.opration_validator(data)
            redis_key=f"{Message.DATA_TRANSFER}_{datetime.strftime(datetime.now(),'%d_%m_%Y')}_{self.client_id}"
            print("==>> redis_key: ", redis_key)
            try:
                data_transfer_flag = self.redis_conn.get(redis_key)
                self.redis_conn.set(redis_key,int(data_transfer_flag)-1)
            except:
                 Logger('IsolateTransfer').warning(f'Key Not found : {redis_key}')
        except Exception as err:
            exception_message = str(err)
            exception_type, _, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            Logger('IsolateTransfer').warning(
                f"{exception_message} {exception_type} {filename}, Line {exception_traceback.tb_lineno}"
            )

    def opration_validator(self, data):
        for record in data:
            # renaming the fields
            record['cid'] = record.pop('client_id')
            record['acid'] = record.pop('account_id')
            record['subdomain'] = record.pop('apisubdomain')

            record.pop('location') if self.message['operation'] == eRetail.LISTINGS else record

            record = UpdateOne({'cid': self.client_id, 'locCode': record['locCode']},{'$set': record, '$setOnInsert': {'tendt': datetime.now()}}, upsert=True) if self.message['operation'] == eRetail.LOCATIONS else InsertOne(record) # TODO : Need to be rechecked if its correct after remaning the column names


            self.records.append(record)
            if len(self.records) >= 5000:
                print('data push >= 5000')
                self.opration_push_data()
        if len(self.records) > 0:
            print('data push > 0')
            self.opration_push_data()

    def opration_push_data(self):
        try:
            print(type(self.records))
            self.push_data(self.records)
            self.records = []
        except Exception as err:
            exception_message = str(err)
            exception_type, _, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            Logger('IsolateTransfer').warning(
                f"{exception_message} {exception_type} {filename}, Line {exception_traceback.tb_lineno}"
            )
