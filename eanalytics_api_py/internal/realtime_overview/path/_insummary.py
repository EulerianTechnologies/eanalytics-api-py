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
        "dim": list(dim_px_map.keys()),
    },
    "AFFILIATION": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIAAF.mcOPE"],
        "dim": list(dim_px_map.keys()),
    },
    "BRANDING": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIABR.mcOPEDATASEARCHENGINE.mcOPEDATASEARCHENGINE"],
        "dim": ["name"],
        "rename_dim_map": {"name": "ope_name"},  # map to p1
        "add_dim_value_map": {"media_key": "BRANDING"},
    },
    "INTERNAL": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIAIN.mcOPE"],
        "dim": list(dim_px_map.keys()),
    },
    "MAILING": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIAML.mcOPE"],
        "dim": list(dim_px_map.keys()),
    },
    "NATURAL": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIANA"],
        "dim": ["name"],
        "rename_dim_map": {"name": "media_key"},  # p0
    },
    "OFFLINE": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIAOF.mcOPE"],
        "dim": list(dim_px_map.keys()),
    },
    "PAID INCLUSION": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIAPI.mcOPE"],
        "dim": list(dim_px_map.keys()),
    },
    "PARTNER": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIAPT.mcOPE"],
        "dim": list(dim_px_map.keys()),
    },
    "REFERRER": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIARF.mcMEDIARF"],
        "dim": ["name"],
        "rename_dim_map": {"name": "ope_name"},  # p1
        "add_dim_value_map": {"media_key": "REFERRER"},
    },
    "SEARCHENGINE": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIASE.mcMEDIASE"],
        "dim": ["name"],
        "rename_dim_map": {"name": "ope_name"},  # map to p1
        "add_dim_value_map": {"media_key": "SEARCHENGINE"},
    },
    "SOCIAL": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIASC.mcOPE"],
        "dim": list(dim_px_map.keys()),
    },
    "SOCIAL FREE": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIASCF.mcOPE"],
        "dim": list(dim_px_map.keys()),
    },
    "SPONSORED LINKS": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIASL.mcOPE"],
        "dim": list(dim_px_map.keys()),
    },
    "TRUSTED FEEDS": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIATF.mcOPE"],
        "dim": list(dim_px_map.keys()),
    },
    "TRUSTED FEEDS FREE": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIATFF.mcOPE"],
        "dim": list(dim_px_map.keys()),
    }
}
