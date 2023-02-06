import datetime
import time
import bson
from Global import DBURL, DB, Collections
from mongodb_vinreco.mongodbops import *
from repo_logs import Logger
from _Base.eRetailBase import BaseClient


class OrdersProcessing(BaseClient):

    def process_data(self):
        try:
            Logger('Skechers-Orders').info('Skechers-Orders ==> Orders processing started')
            pipeline_orders = [
                {
                    "$match": {"cid": self.client_id, "acid": self.account_id,
                               '$or': [{"flagdone": 0}, {'flagdone': {'$exists': False}}]}
                },
                {
                    "$unwind": "$items"
                },
                {
                    "$lookup": {
                        "from": Collections.ERetailStore,
                        "let": {
                            "cid": "$cid",
                            "loc": "$orderLocation"
                        },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": {
                                        "$and": [
                                            {
                                                "$eq": [
                                                    "$cid",
                                                    "$$cid"
                                                ]
                                            },
                                            {
                                                "$eq": [
                                                    "$locCode",
                                                    "$$loc"
                                                ]
                                            }
                                        ]
                                    }
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0.0,
                                    "locdesc": "$locationDescription",
                                    "loctype": "$locationType"
                                }
                            },
                            {
                                "$limit": 1.0
                            }
                        ],
                        "as": "storedata"
                    }
                },
                {
                    "$unwind": "$storedata"
                },
                {
                    "$project": {
                        "locdesc": "$storedata.locdesc",
                        "loctype": "$storedata.loctype",
                        "loc": "$orderLocation",
                        "repkey": "$repkey",
                        "discntcode": "$discountCode",
                        "delslot": "$deliverySlot",
                        "crncy": "$orderCurrency",
                        "cname": "$customerName",
                        "conversnrate": {
                            "$toDecimal": "$conversionRate"
                        },
                        "shipcntry": "$shipCountry",
                        "txamt": {
                            "$toDecimal": "$taxAmount"
                        },
                        "odno": "$orderNo",
                        "shpeml2": "$shipEmail2",
                        "cusdtf4": "$customDataFeld4",
                        "shipst": "$shipState",
                        "odrcvr": {
                            "$cond": [
                                {"$or": [
                                    {"$eq": ["$orderLocation", "M01"]},
                                    {"$eq": ["$orderLocation", "M03"]},
                                    {"$eq": ["$orderLocation", "A74"]}
                                ]},
                                "SKECHERS",
                                ""
                            ]
                        },
                        "gftwrpmsg": "$giftwrapMsg",
                        "bemail2": "$billEmail2",
                        "paymthd": "$paymentMethod",
                        "shpad1": "$shipAddress1",
                        "odexectr": "$fulFillmentLocation",
                        "gf": "$giftvoucher",
                        "custdf3": "$customDataFeld3",
                        "discntamt": {
                            "$toDecimal": "$discountAmount"
                        },
                        "odid": "$orderId",
                        "codchrgs": {
                            "$toDecimal": "$cODCharge"
                        },
                        "updt": {
                            "$dateFromString": {
                                "dateString": "$updatedDate",
                                "format": "%d/%m/%Y %H:%M:%S"
                            }
                        },
                        "shpph2": "$shipPhone2",
                        "strcrdt": "$storeCredit",
                        "bcity": "$billCity",
                        "shpchrgs": {
                            "$toDecimal": "$shippingCharges"
                        },
                        "extinvid": "$extInvoice_no",
                        "badd3": "$billAddress3",
                        "custdf1": "$customDataFeld1",
                        "odamt": {
                            "$toDecimal": "$orderAmount"
                        },
                        "isgf": {
                            "$toInt": "$isGiftwrap"
                        },
                        "invdt": {
                            "$dateFromString": {
                                "dateString": "$invoiceDate",
                                "format": "%d/%m/%Y %H:%M:%S"
                            }
                        },
                        "isvrfdod": {
                            "$toInt": "$isVerifiedOrder"
                        },
                        "shpad2": "$shipAddress2",
                        "status": "$status",
                        "sph1": "$shipPhone1",
                        "shpad3": "$shipAddress3",
                        "bad2": "$billAddress2",
                        "custdf2": "$customDataFeld2",
                        "bst": "$billState",
                        "bad1": "$billAddress1",
                        "beml": "$billEmail1",
                        "otdisc": {
                            "$toDecimal": "$otherDiscount"
                        },
                        "bname": "$billName",
                        "remark": "$orderRemarks",
                        "invno": "$invoice_no",
                        "delno": "$delivery_no",
                        "bpin": "$billPincode",
                        "shppin": "$shipPincode",
                        "bcntry": "$billCountry",
                        "bph2": "$billPhone2",
                        "shpeml": "$shipEmail1",
                        "isonhold": {
                            "$toInt": "$isOnHold"
                        },
                        "cnclremarl": "$cancelRemark",
                        "bph1": "$billPhone1",
                        "custdf5": "$customDataFeld5",
                        "handlingchrgs2": {
                            "$toDecimal": "$handlingCharge2"
                        },
                        "shpct": "$shipCity",
                        "odt": {
                            "$dateFromString": {
                                "dateString": "$orderDate",
                                "format": "%d/%m/%Y %H:%M:%S"
                            }
                        },
                        "lno": "$items.lineno",
                        "sku": "$items.sku",
                        "qty": {
                            "$toInt": {
                                "$toDecimal": "$items.orderQty"
                            }
                        },
                        "cnclqty": {
                            "$toInt": {
                                "$toDecimal": "$items.cancelledQty"
                            }
                        },
                        "shpqty": {
                            "$toInt": {
                                "$toDecimal": "$items.shippedQty"
                            }
                        },
                        "rtqty": {
                            "$toInt": {
                                "$toDecimal": "$items.returnQty"
                            }
                        },
                        "uprc": {
                            "$toDecimal": "$items.unitPrice"
                        },
                        "discamt": {
                            "$toDecimal": "$items.discountAmt"
                        },
                        "taxamt": {
                            "$toDecimal": "$items.taxAmount"
                        }
                    }
                },
                {
                    "$match": {
                        "shpqty": {
                            "$ne": 0
                        }
                    }
                }
            ]
            
            client = MongoDBConnector(DBURL.fetch_data_isolate_url, DB.fetch_data_isolate_db)
            print("Skechers-Orders ==>> DB.fetch_data_isolate_db: ", DB.fetch_data_isolate_db)
            print("Skechers-Orders ==>> DBURL.fetch_data_isolate_url: ", DBURL.fetch_data_isolate_url)
            curser = client.aggregate_raw_batch_query(Collections.ERetailInvoice, pipeline_orders)
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
                    bulk_up_list.append(UpdateOne({'_id': _id}, {'$set': {'flagdone': 1}}))

                    if count > 5000:
                        client.bulk_write(Collections.Orders, insert_result)
                        client.bulk_write(Collections.ERetailInvoice, bulk_up_list)
                        insert_result = []
                        bulk_up_list = []
                        count = 0

                    count += 1

                if count > 0:
                    client.bulk_write(Collections.Orders, insert_result)
                    client.bulk_write(Collections.ERetailInvoice, bulk_up_list)
            Logger('Skechers-Orders').info('Skechers-Orders ==> pipline processing ended')
            Logger('Skechers-Orders').info("Skechers-Orders ==>> self.update_receiver_id")
            self.update_receiver_id()
            Logger('Skechers-Orders').info("Skechers-Orders ==>> self.update_hsn_code")
            self.update_hsn_code()
            Logger('Skechers-Orders').info("Skechers-Orders ==>> self.update_transporter_code")
            self.update_transporter_code()
            Logger('Skechers-Orders').info("Skechers-Orders ==>> Orders processing ended")
        except Exception as err:
            print(err)
            exception_message = str(err)
            exception_type, _, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            Logger('Skechers-Orders').error(
                f"{exception_message} {exception_type} {filename}, Line {exception_traceback.tb_lineno}")
            raise err

    def update_receiver_id(self):
        try:
            regx = bson.regex.Regex(
                r"^([1-9]|([012][0-9])|(3[01]))\/([0]{0,1}[1-9]|1[012])\/\d\d\d\d (20|21|22|23|[0-1]?\d):[0-5]?\d:[0-5]?\d$")
            pipeline = [

                {"$match": {'cid': self.client_id, 'chid': 1,"loc":{'$in':self.location_to_process},"$or": [
                    {"odrcvr": ""},
                    {"odrcvr": None},
                    {"odrcvr": {"$exists": False}},
                ]}},
                {
                    "$lookup": {
                        "from": Collections.ERetailOrder,
                        "let": {"cid": "$cid", "odno": "$odno"},
                        "pipeline": [
                            {"$match": {
                                "$expr": {"$and": [{"$eq": ["$cid", "$$cid"]}, {"$eq": ["$$odno", "$orderNo"]}]}}},
                            {"$project": {'_id': 0, "docdateformat": 1, "orderLocation": 1, "customDataFeld10": 1,
                                          "customDataFeld9": 1, "paymentItems": 1, "customDataFeld7": 1}},
                            {"$limit": 1}
                        ],
                        "as": "all_orders"}
                },
                {
                    "$unwind": "$all_orders"
                },
                {
                    "$lookup": {
                        "from": "stores",
                        "let": {"cid": "$cid", "loc": "$all_orders.customDataFeld10"},
                        "pipeline": [
                            {"$match": {
                                "$expr": {"$and": [{"$eq": ["$cid", "$$cid"]}, {"$eq": ["$gstorecode", "$$loc"]}]}}},
                            {"$project": {"_id": 0, 'storecode': 1}},
                            {"$limit": 1}
                        ],
                        "as": "storedata"}
                },
                {
                    "$unwind": "$storedata"
                },
                {
                    "$addFields": {
                        "docdateformat": {
                            "$regexMatch": {"input": "$all_orders.customDataFeld9", "regex": regx, "options": "i"}
                        },
                        "receiver": {
                            "$cond": [
                                {"$or": [{"$eq": ["$all_orders.orderLocation", "M01"]},
                                         {"$eq": ["$all_orders.orderLocation", "M03"]},
                                         {"$eq": ["$all_orders.orderLocation", "A74"]}]},
                                "SKECHERS",
                                "$storedata.storecode"
                            ]
                        }
                    }
                },
                {
                    "$project": {
                        "orderNo": "$odno",
                        "receiver": "$receiver",
                        "billrefno": {
                            "$cond": [
                                {
                                    "$eq": [
                                        "$all_orders.orderLocation",
                                        "M02"
                                    ]
                                },
                                {
                                    "$arrayElemAt": [
                                        "$all_orders.paymentItems.docNo",
                                        0.0
                                    ]
                                },
                                "$all_orders.customDataFeld7"
                            ]
                        },
                        "receiptdt": {
                            "$cond": [
                                {
                                    "$eq": [
                                        "$docdateformat",
                                        True
                                    ]
                                },
                                {
                                    "$dateFromString": {
                                        "dateString": "$all_orders.customDataFeld9",
                                        "format": "%d/%m/%Y %H:%M:%S"
                                    }
                                },
                                ""
                            ]
                        },
                        "_id": 0.0
                    }
                }
            ]

            client = MongoDBConnector(DBURL.fetch_data_isolate_url, DB.fetch_data_isolate_db)
            result = client.aggregate_raw_batch_query(Collections.Orders, pipeline)
            if result.alive:

                bulk_write = []
                count = 0
                for batch in result:
                    batch = bson.decode_all(batch)
                    for record in batch:
                        bulk_write.append(UpdateMany({"cid": self.client_id, 'chid': 1,
                                                      "odno": record.get('orderNo')},
                                                     {"$set": {
                                                         "odrcvr": record.get('receiver'),
                                                         "billrefno": record.get("billrefno"),
                                                         "receiptdt": record.get("receiptdt")
                                                     }}
                                                     ))

                        if count > 5000:
                            client.bulk_write(Collections.Orders, bulk_write)
                            bulk_write = []
                            count = 0
                        count += 1
                    if count > 0:
                        client.bulk_write(Collections.Orders, bulk_write)
            else:
                Logger('Skechers').info(f"There is non record found with odrcvr id | client id : {self.client_id}")
        except Exception as err:
            exception_message = str(err)
            exception_type, _, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            Logger('Skechers').error(
                f"{exception_message} {exception_type} {filename}, Line {exception_traceback.tb_lineno}")
            raise err

    def update_hsn_code(self):
        try:
            pipeline = [
                {
                    "$match": {
                        "cid": self.client_id,
                        "chid": 1.0,
                        "loc":{'$in':self.location_to_process},
                        "$or": [
                            {
                                "hsncode": ""
                            },
                            {
                                "hsncode": None
                            },
                            {
                                "hsncode": {
                                    "$exists": False
                                }
                            },
                            {
                                "mrp": ""
                            },
                            {
                                "mrp": None
                            },
                            {
                                "mrp": {
                                    "$exists": False
                                }
                            }
                        ]
                    }
                },
                {
                    "$project": {
                        "cid": "$cid",
                        "_id": 0.0,
                        "sku": "$sku",
                        "odno": "$odno"
                    }
                },
                {
                    "$lookup": {
                        "from": Collections.ERetailListings,
                        "let": {
                            "cid": "$cid",
                            "sku": "$sku"
                        },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": {
                                        "$and": [
                                            {
                                                "$eq": [
                                                    "$cid",
                                                    "$$cid"
                                                ]
                                            },
                                            {
                                                "$eq": [
                                                    "$skuCode",
                                                    "$$sku"
                                                ]
                                            }
                                        ]
                                    }
                                }
                            },
                            {
                                "$project": {
                                    "category": "$categories.category.category_desc",
                                    "hsncode": "$taxCategory",
                                    "mrp": "$mrp",
                                    "_id": 0.0
                                }
                            },
                            {
                                "$limit": 1.0
                            }
                        ],
                        "as": "listing_data"
                    }
                },
                {
                    "$unwind": "$listing_data"
                },
                {
                    "$project": {
                        "category": "$listing_data.category",
                        "hsncode": "$listing_data.hsncode",
                        "mrp": "$listing_data.mrp",
                        "skuCode": "$sku"
                    }}
            ]

            client = MongoDBConnector(DBURL.fetch_data_isolate_url, DB.fetch_data_isolate_db)
            result = client.aggregate_raw_batch_query(Collections.Orders, pipeline)
            if result.alive:

                bulk_write = []
                count = 0
                for batch in result:
                    batch = bson.decode_all(batch)
                    for record in batch:
                        bulk_write.append(UpdateMany({
                            "cid": self.client_id,
                            'chid': 1,
                            "sku": record.get("skuCode")
                        },
                            {
                                "$set": {
                                    "mrp": float(record.get("mrp")),
                                    "hsncode": record.get("hsncode"),
                                    "category": record.get("category")
                                }
                            }))

                        if count > 5000:
                            client.bulk_write(Collections.Orders, bulk_write)
                            bulk_write = []
                            count = 0
                        count += 1
                    if count > 0:
                        client.bulk_write(Collections.Orders, bulk_write)
            else:
                Logger('Skechers').info(
                    f"There is non record found with hsncode and mrp | client id : {self.client_id}")
        except Exception as err:
            exception_message = str(err)
            exception_type, _, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            Logger('Skechers').error(
                f"{exception_message} {exception_type} {filename}, Line {exception_traceback.tb_lineno}")
            raise err

    def update_transporter_code(self):
        try:
            pipeline = [
                {
                    "$match": {
                        "cid": self.client_id,
                        "chid": 1.0,
                        "loc":{'$in':self.location_to_process},
                        "$or": [
                            {
                                "transpcode": ""
                            },
                            {
                                "transpcode": None
                            },
                            {
                                "transpcode": {
                                    "$exists": False
                                }
                            }
                        ]
                    }
                },
                {
                    "$project": {
                        "cid": "$cid",
                        "odno": "$odno"
                    }
                },
                {
                    "$lookup": {
                        "from": Collections.ERetailShippingDetails,
                        "let": {
                            "cid": "$cid",
                            "odno": "$odno"
                        },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": {
                                        "$and": [
                                            {
                                                "$eq": [
                                                    "$cid",
                                                    "$$cid"
                                                ]
                                            },
                                            {
                                                "$eq": [
                                                    "$order_no",
                                                    "$$odno"
                                                ]
                                            }
                                        ]
                                    }
                                }
                            },
                            {
                                "$project": {
                                    "order_no": "$order_no",
                                    "transportercode": "$transportercode",
                                    "_id": 0.0
                                }
                            },
                            {
                                "$limit": 1.0
                            }
                        ],
                        "as": "transporter_data"
                    }
                },
                {
                    "$unwind": "$transporter_data"
                },
                {
                    "$project": {
                        "cid": "$cid",
                        "odno": "$odno",
                        "order_no": "$transporter_data.order_no",
                        "transportercode": "$transporter_data.transportercode",
                        "_id": 0
                    }
                }
            ]

            client = MongoDBConnector(DBURL.fetch_data_isolate_url, DB.fetch_data_isolate_db)
            result = client.aggregate_raw_batch_query(Collections.Orders, pipeline)
            if result.alive:
                bulk_write = []
                count = 0
                for batch in result:
                    batch = bson.decode_all(batch)
                    for record in batch:
                        bulk_write.append(UpdateMany({"cid": self.client_id, "odno": record.get("order_no")},
                                                     {"$set": {"transpcode": record.get("transportercode")}}))

                        if count > 5000:
                            client.bulk_write(Collections.Orders, bulk_write)
                            bulk_write = []
                            count = 0
                        count += 1
                    if count > 0:
                        client.bulk_write(Collections.Orders, bulk_write)
            else:
                Logger('Skechers').info(f"There is non record found with transportcode | client id : {self.client_id}")
        except Exception as err:
            exception_message = str(err)
            exception_type, _, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            Logger('Skechers').error(
                f"{exception_message} {exception_type} {filename}, Line {exception_traceback.tb_lineno}")
            raise err
