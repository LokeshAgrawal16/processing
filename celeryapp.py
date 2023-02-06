from celery import Celery

app = Celery('vinstore', include=['processing_tasks'])
app.config_from_object("celeryconfig")
app.conf.update(
    result_expires=3600,
)

# celery -A celeryapp worker --loglevel=info
if __name__ == '__main__':
    app.Worker(optimization='fair',loglevel='INFO').start()