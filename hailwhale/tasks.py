from celery.decorators import task

@task
def flush_expired_data():
    from hailwhale import Whale
    Whale().cleanup()
@task
def flush_hail():
    from hailwhale import Hail
    Hail.dump_now()
