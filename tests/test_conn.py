import os
from eanalytics_api_py import Conn
from datetime import date, timedelta
import pytest
import re

gridpool_name = os.environ.get("PYTEST_GRIDPOOL_NAME")
datacenter_name = os.environ.get("PYTEST_DATACENTER_NAME")
website_name = os.environ.get("PYTEST_WEBSITE_NAME")
api_token = os.environ.get("PYTEST_API_TOKEN")
ip = os.environ.get("PYTEST_IP")

dt_today = date.today()
dt_delta = date.today() - timedelta(days=3)
today = dt_today.strftime("%m/%d/%Y")
delta = dt_delta.strftime("%m/%d/%Y")
epoch_today = dt_today.strftime("%s")
epoch_today_1min = int(epoch_today)+60

print_log = True
datamining_type = "order"

payload = {
    "date-from" : delta,
    "date-to" : today
}

query = """GET {{
  TIMERANGE {{ {epoch_today} {epoch_today_1min} }}
  READERS {{
    ea:pageview@{website_name} AS pageview
  }}
  OUTPUTS_ROW( pageview ) {{
    pageview.timestamp, pageview.uid, pageview.url
  }}
}};
""".format(
    epoch_today=epoch_today,
    epoch_today_1min=epoch_today_1min,
    website_name=website_name
)

assert( isinstance(gridpool_name, str) )
assert( isinstance(datacenter_name, str) )
assert( isinstance(website_name, str) )
assert( isinstance(api_token, str) )
assert( isinstance(ip, str) )

conn = Conn(
    gridpool_name=gridpool_name,
    datacenter=datacenter_name,
    api_key=api_token,
    log_level="DEBUG"
)

def test_get_view_id_name_map():
    view_id_name_map = conn.get_view_id_name_map(
        website_name=website_name
    )

    assert ( isinstance(view_id_name_map, dict) )
    assert ( len(view_id_name_map) > 1 )

def test_get_website_by_name():
    website = conn.get_website_by_name(
        website_name=website_name
    )
    assert( isinstance(website, dict) )

    for prop in [
    "website_id", "website_domain", "website_name", "websitegrid_id",
    "website_active", "website_tzone", "websitecategory_id"
    ]:
        assert( prop in website )

def test_conn_download_datamining():
    l_path2file = conn.download_datamining(
        website_name=website_name,
        datamining_type=datamining_type,
        payload=payload,
        override_file=True,
        n_days_slice=2,
    )
    assert( isinstance(l_path2file, list) )
    assert( len(l_path2file) == 2 )
    for path2file in l_path2file:
        assert( os.path.isfile(path2file) )
        os.remove(path2file)


    payload['view-id'] = 10

    with pytest.raises(
        ValueError,
        match=re.escape("view-id should match ^[0-9]$")
    ):
        l_path2file = conn.download_datamining(
            website_name=website_name,
            datamining_type=datamining_type,
            payload=payload,
            override_file=True,
            n_days_slice=2,
        )


def test_conn_download_edw():
    path2file = conn.download_edw(
        query=query,
        override_file=True,
        ip=ip
    )
    assert( isinstance(path2file, str) )
    assert( os.path.isfile(path2file) )
    os.remove(path2file)