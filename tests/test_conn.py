import os
from eanalytics_api_py import Conn

gridpool_name = os.environ.get("PYTEST_GRIDPOOL_NAME")
datacenter_name = os.environ.get("PYTEST_DATACENTER_NAME")
website_name = os.environ.get("PYTEST_WEBSITE_NAME")
api_token = os.environ.get("PYTEST_API_TOKEN")
print_log = True

assert( isinstance(gridpool_name, str) )
assert( isinstance(datacenter_name, str) )
assert( isinstance(website_name, str) )
assert( isinstance(api_token, str) )

conn = Conn(
    gridpool_name=gridpool_name,
    datacenter=datacenter_name,
    api_key=api_token,
    print_log=print_log,
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