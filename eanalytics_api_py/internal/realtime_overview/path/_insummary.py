""" List of path for insumnary realtime report
Drill down by channel """

dim_px_map = {
    "media_key": "p0",
    "ope_name": "p1",
    "opealias_alias": "p1",
    "semcampaign_name": "p1",
    "publisher_name": "p2",
    "publisheralias_alias": "p2",
    "mediaplan_name": "p6",
    "mediaplangroup_name": "p7",
    "submedia_name": "p8",
    "publishergroup_name": "p18",
    "publishertype_name": "p19",
    "submediagroup_name": "p20",
}

gen_dim = [
    "media_key",
    "ope_name",
    "opealias_alias",
    "publisher_name",
    "publisheralias_alias",
    "mediaplan_name",
    "mediaplangroup_name",
    "submedia_name",
    "publishergroup_name",
    "publishertype_name",
    "submediagroup_name"
]

override_dim_map = {
    "publisher_name": "publisheralias_alias",
    "ope_name": "opealias_alias",
}

d_path = {
    "ADVERTISING": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIAAD.mcOPE"],
        "dim": gen_dim,
    },
    "AFFILIATION": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIAAF.mcOPE"],
        "dim": gen_dim,
    },
    "BRANDING": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIABR.mcOPEDATASEARCHENGINE.mcOPEDATASEARCHENGINE"],
        "dim": ["name"],
        "rename_dim_map": {"name": "ope_name"},  # map to p1
        "add_dim_value_map": {"media_key": "BRANDING"},
    },
    "INTERNAL": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIAIN.mcOPE"],
        "dim": gen_dim,
    },
    "MAILING": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIAML.mcOPE"],
        "dim": gen_dim,
    },
    "NATURAL": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIANA"],
        "dim": ["name"],
        "rename_dim_map": {"name": "media_key"},  # p0
    },
    "OFFLINE": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIAOF.mcOPE"],
        "dim": gen_dim,
    },
    "PAID INCLUSION": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIAPI.mcOPE"],
        "dim": gen_dim,
    },
    "PARTNER": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIAPT.mcOPE"],
        "dim": gen_dim,
    },
    "REFERER": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIARF.mcMEDIARF"],
        "dim": ["name"],
        "rename_dim_map": {"name": "ope_name"},  # p1
        "add_dim_value_map": {"media_key": "REFERER"},
    },
    "SEARCHENGINE": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIASE.mcMEDIASE"],
        "dim": ["name"],
        "rename_dim_map": {"name": "ope_name"},  # map to p1
        "add_dim_value_map": {"media_key": "SEARCHENGINE"},
    },
    "SOCIAL": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIASC.mcOPE"],
        "dim": gen_dim,
    },
    "SOCIALFREE": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIASCF.mcOPE"],
        "dim": gen_dim,
    },
    "SPONSOREDLINKS": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIASL.mcSEMCAMPAIGN"],
        "dim": ["media_key", "semcampaign_name", "publisher_name", "publisheralias_alias", "publishergroup_name",
                "publishertype_name", "mediaplan_name", "mediaplangroup_name", "submedia_name", "submediagroup_name"],
    },
    "TRUSTEDFEED": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIATF.mcOPE"],
        "dim": gen_dim,
    },
    "TRUSTEDFREE": {
        "path": ["mcMEDIAINCOMING[%d]", "mcMEDIATFF.mcOPE"],
        "dim": gen_dim,
    }
}
