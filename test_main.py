from Global import eRetail
from processing_tasks import vinreco_works
import os

dates = [
    '2022-12-24',
    '2022-12-25'
    ]

pull_types = [
    eRetail.ORDERS_RETURNS,
    # eRetail.ALL_ORDERS,
    # eRetail.INVOICE,
    # eRetail.LISTINGS,
    # eRetail.SHIPPING_DETAILS,
    # eRetail.LOCATIONS
    ]
print("==>> os.environ['CELERY_DEFAULT_QUEUE']: ", os.environ["CELERY_DEFAULT_QUEUE"])
# # exit(0)
for date in dates:
    for pull_type in pull_types:
        message = {
            'message_id': 'uuid',
            'client_id': 1,
            'operation': pull_type,
            'subdomain': 'skechers',
            'date': date,
            'call_for': 'DATA_TRANSFER'
        }
        task_id = vinreco_works.delay(message)
        print("==>> message: ", message)
        print("==>> task_id: ", task_id)


message = {
    'client_id': 1,
    'subdomain': 'skechers',
    'account_id': 1,
    'call_for': 'PROCESSING'
}

# task_id = vinreco_works.delay(message)
# print("==>> task_id: ", task_id)