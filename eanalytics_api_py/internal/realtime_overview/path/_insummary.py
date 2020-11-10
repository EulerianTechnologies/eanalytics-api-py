""" List of path for insumnary realtime report
Drill down by channel """

dim_px_map = {
    "media_key": "p0",
    "ope_name": "p1",
    "opealias_alias": "p1",
    "publisher_name": "p2",
    "publisheralias_alias": "p2",
    "mediaplan_name": "p6",
    "mediaplangroup_name": "p7",
    "submedia_name": "p8",
    "publishergroup_name": "p18",
    "publishertype_name": "p19",
    "submediagroup_name": "p20",

}

override_dim_map = {
    "publisher_name": "publisheralias_alias",
    "ope_name": "opealias_alias",
}

d_path = {
    "ADVERTISING": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIAAD.mcOPE"],
        "dim": list(dim_px_map.keys())
    },
    "MAILING": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIAML.mcOPE"],
        "dim": list(dim_px_map.keys()),
    },
    "AFFILIATION": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIAAF.mcOPE"],
        "dim": list(dim_px_map.keys()),
    },
    "SOCIAL": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIASC.mcOPE"],
        "dim": list(dim_px_map.keys()),
    },
    "PAID INCLUSION": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIAPI.mcOPE"],
        "dim": list(dim_px_map.keys()),
    },
    "TRUSTED FEEDS": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIATF.mcOPE"],
        "dim": list(dim_px_map.keys()),
    },
    "PARTNER": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIAPT.mcOPE"],
        "dim": list(dim_px_map.keys()),
    },
    "SPONSORED LINKS": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIASL.mcOPE"],
        "dim": list(dim_px_map.keys()),
    },
}
