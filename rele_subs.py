# subs.py
# https://mercadonarele.readthedocs.io/en/latest/guides/basics.html

from rele import sub

@sub(topic="job-requests")
def photo_uploaded(data, **kwargs):
    print(f"Crawler job requested: {data['jobtype']}")
