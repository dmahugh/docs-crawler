# publisher.py
# https://mercadonarele.readthedocs.io/en/latest/guides/basics.html

import rele
from rele_settings import config  # we need this for initializing the global Publisher singleton

data = {"jobtype": "cloud-sql-docs"}

rele.publish(topic="job-requests", data=data)
