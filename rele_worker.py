# worker.py
# https://mercadonarele.readthedocs.io/en/latest/guides/basics.html

from time import sleep
from rele import Worker

from rele_settings import config
from rele_subs import photo_uploaded

if __name__ == '__main__':
    worker = Worker(
        [photo_uploaded],
        config.gc_project_id,
        config.credentials,
        config.ack_deadline,
    )
    worker.run_forever()