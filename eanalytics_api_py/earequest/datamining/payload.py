"""Http params grouped into dicts of category"""

product = {
    "with-orderproduct" : 1, #  purchased products
    "with-productparam" : 1, # purchased products parameters
    "with-productgroup" : 1, # purchased products groups
}

channel = {
    "with-channel-count" : 1, # number of marketing interactions
    "with-channel-level" : 1, # with previous marketing interactions
    "max-channel-info" : 14,  # marketing interactions detail level
    "max-channel-level" : 40, # marketing interaction history length
    "with-channel-via" : 1,   # attibution type pc/pw/r...
    "with-channel-date" : 1,  # marketing interaction date
    "with-channel-profile" : 1, # marketing interaction profile
    "with-channel-mdevicetype" : 1, # marketing interaction device type
    "with-first-channel" : 1, # with first marketing interaction
    "with-last-channel" : 1, # with last marketing interaction
    "with-firstlast-channel-lag" : 1, # delay (seconds) between first and last interaction
}

tpchannel = {
    "with-tpchannel-count" : 1,
    "with-tpchannel-level" : 1,
    "max-tpchannel-info" : 9,
    "max-tpchannel-level" : 40,
    "with-tpchannel-via" : 1,
    "with-tpchannel-date" : 1,
    "with-tpchannel-profile" : 1,
    "with-tpchannel-mdevicetype" : 1,
    "with-first-tpchannel" : 1,
    "with-last-tpchannel" : 1,
    "with-firstlast-tpchannel-lag" : 1,
}

order = {
    "with-ordertype" : 1, # order type
    "with-ordertypecustom" : 1, # order custom type
    "with-orderpayment" : 1, # order payment
    "with-orderpage" : 1, # path seen during the order session
    "with-orderpageurl" : 1, # url seen during the order session
    "with-orderevent" : 1, #  event triggered during the order session
    "with-scartdate" : 1, # shopping cart date
    "with-scartvalidmargin" : 1, # order margin
    "with-realscartvalidmargin" : 1, # real order margin
    "with-scartvalidamount" : 1, # order amount
    "with-realscartvalidamount" : 1, # real order amount
}

analytics = {
    "with-extparam" : 1,
    "with-geoloc" : 1, # country Coded on 2 ISO-3166-1 characters
    "with-browser" : 1, # browser name
    "with-lang" : 1, # browser language
    "with-os" : 1, # operating system
    "with-isp" : 1, # internet service provider
    "with-action" : 1, # action executed before purchasing
    "with-mdevicetype" : 1, # type of device
    "with-blockpromo" : 1, # last clicked auto promotion
    "with-visitdate" : 1, # visit date
    "with-visitlag" : 1, # duration of the visit
    "with-cgiparam" : 1, # custome parameters
    "with-rebuyer-prevnb" : 1, # previous number of orders
    "with-rebuyer-prevamount" : 1, # previous revenue amount
    "with-rebuyer-prevtm" : 1, # date of previous order
    "with-rebuyer-prevtm-epoch" : 1, # date EPOCH of previous order
}

user = {
    "with-email" : 1,
    "with-ip" : 1,
    "with-buyerid" : 1,
    "with-userid" : 1,
    "with-last-rp-ea-android-adid" : 1,
    "with-last-rp-ea-ios-idfa" : 1,
}

user_info = {
    "with-cluster" : 1, # Segment names
    "with-iduserparam" :1, # CRM parameters
}
