import datetime
import time
import bson
from mongodb_vinreco.mongodbops import *
from Global import DBURL, DB, Collections
from repo_logs import Logger
from _Base.eRetailBase import BaseClient


class OrdersReturnsProcessing(BaseClient):

    def process_data(self):
        try:
            Logger('Skechers-Orders').info("Skechers-Orders ==>> OrdersReturns processing started")
            pipeline_orders = [
                {
                    "$unwind": "$items"
                },
                {
                    "$addFields": {
                        "lineno": "$items.lineno",
                        "ext_lineno": "$items.ext_lineno",
                        "sku": "$items.sku",
                        "brand": "$items.brand",
                        "return_qty": "$items.return_qty",
                        "line_unitprice": "$items.line_unitprice",
                        "line_amt": "$items.line_amt",
                        "discount_amt": "$items.discount_amt",
                        "invoice_qty": "$items.invoice_qty",
                        "return_reason": "$items.return_reason",
                        "serialNo1": [
                            ""
                        ],
                        "serialNo2": [
                            ""
                        ],
                        "serialNo3": [
                            ""
                        ],
                        "gstRt": "$items.gstRt",
                        "igstAmt": "$items.igstAmt",
                        "cgstAmt": "$items.cgstAmt",
                        "sgstAmt": "$items.sgstAmt",
                        "cesRt": "$items.cesRt",
                        "cesAmt": "$items.cesAmt",
                        "cesNonAdvlAmt": "$items.cesNonAdvlAmt",
                        "stateCesRt": "$items.stateCesRt",
                        "stateCesAmt": "$items.stateCesAmt",
                        "stateCesNonAdvlAmt": "$items.stateCesNonAdvlAmt",
                        "hsnCode": "$items.hsnCode",
                        "igstRt": "$items.igstRt",
                        "cgstRt": "$items.cgstRt",
                        "sgstRt": "$items.sgstRt",
                        "ugstRt": "$items.ugstRt",
                        "ugstAmt": "$items.ugstAmt",
                        "taxableAmount": "$items.taxableAmount",
                        "giftVoucher": "$items.giftVoucher",
                        "storeCredit": "$items.storeCredit",
                        "mopValue": "$items.mopValue",
                        "udf1": "$items.udf1",
                        "udf2": "$items.udf2",
                        "udf3": "$items.udf3",
                        "udf4": "$items.udf4",
                        "udf5": "$items.udf5",
                        "udf6": "$items.udf6",
                        "udf7": "$items.udf7",
                        "udf8": "$items.udf8",
                        "udf9": "$items.udf9",
                        "udf10": "$items.udf10",
                        "orderLineNo": "$items.orderLineNo",
                        "return_confirmdate":{"$dateFromString": {"dateString": "$return_confirmdate", "format": "%d/%m/%Y %H:%M:%S"}},
                        "return_date": {"$dateFromString": {"dateString": "$return_date", "format": "%d/%m/%Y %H:%M:%S"}},
                        "return_closedate": {
                            "$cond": [
                                {"$eq": ["$return_closedate", ""]},
                                "",
                                {"$dateFromString": {"dateString": "$return_closedate",
                                                     "format": "%d/%m/%Y %H:%M:%S"}}
                            ]
                        }
                    }
                },
                {
                    "$project": {
                        "items": 0.0,
                        "taxDtls": 0,
                        "serialNo1": 0,
                        "serialNo2": 0,
                        "serialNo3": 0
                    }
                }
            ]
            client = MongoDBConnector(
                DBURL.fetch_data_isolate_url, DB.fetch_data_isolate_db)
            curser = client.aggregate_raw_batch_query(
                Collections.ERetailOrdersReturns, pipeline_orders)
            insert_result = []
            bulk_up_list = []
            count = 0
            for batch in curser:
                batch = bson.decode_all(batch)
                for record in batch:
                    _id = record.pop('_id')
                    record.update(
                        {'endt': datetime.datetime.now(), 'lotno': int(time.mktime(datetime.datetime.now().timetuple())),
                         'chid': 1, "cid": self.client_id})
                    insert_result.append(InsertOne(record))
                    bulk_up_list.append(
                        UpdateOne({'_id': _id}, {'$set': {'flagdone': 1}}))

                    if count > 5000:
                        client.bulk_write(
                            Collections.OrdersReturns, insert_result)
                        client.bulk_write(
                            Collections.ERetailOrdersReturns, bulk_up_list)
                        insert_result = []
                        bulk_up_list = []
                        count = 0

                    count += 1

                if count > 0:
                    client.bulk_write(Collections.OrdersReturns, insert_result)
                    client.bulk_write(
                        Collections.ERetailOrdersReturns, bulk_up_list)
            Logger('Skechers-Orders').info("Skechers-Orders ==>> OrdersReturns processing ended")

        except Exception as err:
            print(err)
            exception_message = str(err)
            exception_type, _, exception_traceback = sys.exc_info()
            filename = os.path.split(
                exception_traceback.tb_frame.f_code.co_filename)[1]
            Logger('Skechers-OrdersReturns').error(
                f"{exception_message} {exception_type} {filename}, Line {exception_traceback.tb_lineno}")
            raise err
