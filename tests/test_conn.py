import os
from datetime import date, timedelta
import pytest
import re
import logging

from eanalytics_api_py import Conn, eaload, earequest
from eanalytics_api_py.internal.realtime_overview.path import _insummary

import pandas as pd

LOGGER = logging.getLogger(__name__)
LOGGER.info("Importing environments variables")

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
epoch_today_1min = int(epoch_today) + 60

print_log = True
datamining_type = "order"

payload = {
    "date-from": delta,
    "date-to": today,
    **earequest.datamining.payload.product,
    **earequest.datamining.payload.channel
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


class TestInitialConfig:
    def test_gridpool_name(self):
        assert (isinstance(gridpool_name, str))

    def test_datacenter_name(self):
        assert (isinstance(datacenter_name, str))

    def test_website_name(self):
        assert (isinstance(website_name, str))

    def test_api_token(self):
        assert (isinstance(api_token, str))

    def test_ip(self):
        assert (isinstance(ip, str))


conn = Conn(
    gridpool_name=gridpool_name,
    datacenter=datacenter_name,
    api_key=api_token,
    print_log=print_log
)

view_map = conn.get_view_id_name_map(
    website_name=website_name)

mdevice_map = conn.get_mdevicetype_id_name_map(
    website_name=website_name)

profile_map = conn.get_profile_id_name_map(
    website_name=website_name)

ordertype_map = conn.get_ordertype_id_name_map(
    website_name=website_name)

orderpayment_map = conn.get_orderpayment_id_name_map(
    website_name=website_name)

estimatetype_map = conn.get_estimatetype_id_name_map(
    website_name=website_name)

ordertypecustom_map = conn.get_ordertypecustom_id_name_map(
    website_name=website_name)


class TestConnGetViewIdNameMap:
    def test_is_dict(self):
        assert (isinstance(view_map, dict))

    def test_length(self):
        assert (len(view_map) > 1)


class TestConnGetMdeviceIdNameMap:
    def test_is_dict(self):
        assert (isinstance(mdevice_map, dict))

    def test_length(self):
        assert (len(mdevice_map) > 1)


class TestConnGetProfileIdNameMap:
    def test_is_dict(self):
        assert (isinstance(profile_map, dict))

    def test_length(self):
        assert (len(profile_map) > 0)

class TestConnGetOrdertypeIdNameMap:
    def test_is_dict(self):
        assert (isinstance(ordertype_map, dict))

    def test_length(self):
        assert (len(ordertype_map) > 0)

class TestConnGetOrderPaymentIdNameMap:
    def test_is_dict(self):
        assert (isinstance(orderpayment_map, dict))

class TestConnGetEstimatetypeIdNameMap:
    def test_is_dict(self):
        assert (isinstance(estimatetype_map, dict))

class TestConnGetOrdertypecustomIdNameMap:
    def test_is_dict(self):
        assert (isinstance(ordertypecustom_map, dict))

    def test_length(self):
        assert (len(ordertypecustom_map) > 0)


website = conn.get_website_by_name(
    website_name=website_name
)

props = [
    "website_id", "website_domain", "website_name", "websitegrid_id",
    "website_active", "website_tzone", "websitecategory_id"
]


class TestConnGetWebsiteByName:
    def test_is_dict(self):
        assert (isinstance(website, dict))

    def test_has_all_properties(self):
        assert (all(prop in website for prop in props))


l_path2file = conn.download_datamining(
    website_name=website_name,
    datamining_type=datamining_type,
    payload=payload,
    override_file=True,
    n_days_slice=2,
)


class TestConnDownloadDatamining:
    def test_is_list(self):
        assert (isinstance(l_path2file, list))

    def test_is_length_two(self):
        assert (len(l_path2file) == 2)

    def test_is_file(self):
        assert (all(os.path.isfile(path2file) for path2file in l_path2file))

    def test_is_incorrect_view_id(self):
        payload['view-id'] = '10'
        with pytest.raises(
                ValueError,
                match=re.escape("view-id should match ^[0-9]$")
        ):
            conn.download_datamining(
                website_name=website_name,
                datamining_type=datamining_type,
                payload=payload,
                override_file=True,
                n_days_slice=2,
            )


df_gen = eaload.generic.csv_files_2_df(l_path2file)
df_dedup_touch = eaload.datamining.deduplicate_touchpoints(l_path2file)
df_dedup_prod = eaload.datamining.deduplicate_products(l_path2file)


class TestEaload:
    def test_is_df_gen(self):
        assert (isinstance(df_gen, pd.DataFrame))

    def test_is_df_dedup_touch(self):
        assert (isinstance(df_dedup_touch, pd.DataFrame))

    def test_cols_df_dedup_touch(self):
        assert (any(not col_name.endswith("_") for col_name in df_dedup_touch))

    def test_is_df_dedup_prod(self):
        assert (isinstance(df_dedup_prod, pd.DataFrame))

    def test_cols_df_dedup_prod(self):
        assert (any(not col_name.endswith("_") for col_name in df_dedup_prod))

    def test_remove_l_path2file(self):
        for path2file in l_path2file:
            os.remove(path2file)


path2file = conn.download_edw(
    query=query,
    override_file=True,
    ip=ip
)

df = eaload.generic.csv_files_2_df(path2file)


class TestConnDownloadEdw:
    def test_is_str(self):
        assert (isinstance(path2file, str))

    def test_is_file(self):
        assert (os.path.isfile(path2file))

    def test_is_df_gen(self):
        assert (isinstance(df, pd.DataFrame))

    def test_remove_path2file(self):
        os.remove(path2file)


df_flat_overview = conn.download_flat_overview_realtime_report(
    website_name=website_name,
    date_from=delta,
    date_to=today,
    report_name="insummary",
    kpi=["click"],
)


class TestConnDownloadFlatOverview:
    def test_cols(self):
        assert (any(col_name in df_flat_overview.columns)
                for col_name in _insummary.dim_px_map.values())
