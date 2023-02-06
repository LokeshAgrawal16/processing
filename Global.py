import os



print(os.getenv('STAGE'))

if os.getenv('STAGE') == 'dev':
    os.environ["FETCH_DATA_URL"] = "mongodb://localhost:27017/Vinstore?authSource=admin"
    os.environ["FETCH_DATA_DB"] = "vinstore_local"
    os.environ["FETCH_DATA_ISOLATE_URL"] = "mongodb://localhost:27017/Vinstore?authSource=admin"
    os.environ["FETCH_DATA_ISOLATE_DB"] = "IsolatedDB"
    os.environ["PUSH_DATA_ISOLATE_URL"] = "mongodb://localhost:27017/Vinstore?authSource=admin"
    os.environ["PUSH_DATA_ISOLATE_DB"] = "IsolatedDB"
    # os.environ["FETCH_DATA_URL"] = "mongodb://adminuser:stoaG7gW3hU7xJ9rX9nre@10.120.21.251:27017/admin"
    # os.environ["FETCH_DATA_DB"] = "vinstore_prod"
    # os.environ["FETCH_DATA_ISOLATE_URL"] = "mongodb://adminuser:stoaG7gW3hU7xJ9rX9nre@10.120.21.251:27017/admin"
    # os.environ["FETCH_DATA_ISOLATE_DB"] = "vinstore_prod_isolated"
    # os.environ["PUSH_DATA_ISOLATE_URL"] = "mongodb://adminuser:stoaG7gW3hU7xJ9rX9nre@10.120.21.251:27017/admin"
    # os.environ["PUSH_DATA_ISOLATE_DB"] = "vinstore_prod_isolated"
    # os.environ["CELERY_DEFAULT_QUEUE"] = "eretail_processing_testing"
    os.environ["REDIS_CONNECTION"] = "prd-reco-client-redis-ec.l9iuhd.ng.0001.aps1.cache.amazonaws.com"



class DBURL:
    fetch_data_url = os.getenv('FETCH_DATA_URL')
    fetch_data_isolate_url = os.getenv('FETCH_DATA_ISOLATE_URL')
    push_data_isolate_url = os.getenv('PUSH_DATA_ISOLATE_URL')


class DB:
    fetch_data_db = os.getenv('FETCH_DATA_DB')
    fetch_data_isolate_db = os.getenv('FETCH_DATA_ISOLATE_DB')
    push_data_isolate_db = os.getenv('PUSH_DATA_ISOLATE_DB')


class eRetail:
    LISTINGS = "LISTINGS"
    ALL_ORDERS = "ALL_ORDERS"
    INVOICE = "INVOICE"
    SHIPPING_DETAILS = "SHIPPING_DETAILS"
    LOCATIONS = "LOCATIONS"
    ORDERS_RETURNS = "RETURNS"

ProcessType = [
    eRetail.LISTINGS,
    eRetail.ALL_ORDERS,
    eRetail.INVOICE,
    eRetail.SHIPPING_DETAILS,
    eRetail.LOCATIONS,
    eRetail.ORDERS_RETURNS
    ]

ProcessCollNames = {
    eRetail.LISTINGS: "eretaillistings",
    eRetail.ALL_ORDERS: "eretailallorders",
    eRetail.INVOICE: "eretailinvoice",
    eRetail.SHIPPING_DETAILS: "eretailshippingdetails",
    eRetail.ORDERS_RETURNS: "eretailreturns",
    eRetail.LOCATIONS: "eretaillocations"
}

IsolateCollNames = {
    eRetail.LISTINGS: "eretaillistings",
    eRetail.ALL_ORDERS: "eretailallorders",
    eRetail.INVOICE: "eretailinvoice",
    eRetail.SHIPPING_DETAILS: "eretailshippingdetails",
    eRetail.ORDERS_RETURNS: "eretailreturns",
    eRetail.LOCATIONS: "eretailstores"
}


class Collections:
    Connectors = 'connectors'
    OmsOperations = 'omsoperations'
    Notifications = 'notifications'
    ReportsLogTracking = 'reportslogtracking'
    Clients = 'clients'
    Orders = 'orders'
    OrdersReturns = 'returns'
    OmsMasters = 'omsmasters'
    ERetailStore = 'eretailstores'
    ERetailInvoice = IsolateCollNames[eRetail.INVOICE]  # 'eretailinvoice'
    ERetailOrder = IsolateCollNames[eRetail.ALL_ORDERS]  # 'eretailallorders'
    Crontracking = 'crontracking'
    OmsRulesHeads = 'omsrulesheads'
    OmsRulesHeadsNew = 'omsrulesheadsnew'
    OmsRules = 'omsrulesxx'  # todo:need to change the collection name to (omsrules)
    OmsRulesNew = "omsrulesxxnew"
    OmsCommisionLogs = "omscommisionlogs"
    ProductTaxLogs = "producttaxlogs"
    HsnMaster = "hsnmaster"
    TaxMaster = "taxmaster"
    ERetailListings = IsolateCollNames[eRetail.LISTINGS]  # "eretaillistings"
    ERetailShippingDetails = IsolateCollNames[eRetail.SHIPPING_DETAILS]  # "eretailshippingdetails"
    ReportsConfig = "reportsconfig"
    Stores = "stores"
    Communications = "notifsncomm"
    ERetailReturn = IsolateCollNames[eRetail.ORDERS_RETURNS]  # 'eretailreturns'


class Calltype:
    Login = 'Login'
    DateEntries = 'DateEntries'
    RequestReport = 'RequestReport'
    GetReport = 'GetReport'
    ProcessOrders = 'ProcessOrders'
    Reports = "Reports"
    Rulebox = "Rulebox"
    ScheduledReports = "ScheduledReports"


class ErrorCodes:
    Unset = 100
    DatabaseError = 101
    WrongCredentials = 102
    InternalError = 103
    EmptyCred = 104
    Blocked = 105
    CookieExpired = 106
    ParamErr = 107
    LoginSucessfull = 108
    EmptyResponse = 109
    NoEntriesToProcess = 110
    EntriesFound = 111
    Successful = 200
    NotFound = 404
    InvalidCredentials = 112
    ApiHitQuotaExceeded = 113


class Message:
    PROCESSING='PROCESSING'
    DATA_TRANSFER='DATA_TRANSFER'
    