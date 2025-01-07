"""
Format:
{
    <component>: [
        {
            "suite": [<>, <>, ..],  <-- Valid selector for suite
            "serverPoolId": ""      <-- Default serverPoolId to use for triggers
            "extraParameters": ".." <-- Default extraParams to pass for the com
            "token": "",            <-- Token to be used for triggers
        },
        {
            # Can have multiple entries per component like above
            # Refer '2i' / 'fts' for example
        }
    ]
}
"""
component = {
    "2i": [
        {
            "suite": ["12hour", "weekly", "12hr_weekly"],
            "serverPoolId": "regression",
            "extraParameters": "get-cbcollect-info=True",
            "token": "extended_sanity",
        },
        {
            "suite": ["ce"],
            "serverPoolId": "regression",
            "executor_job_parameters": "installParameters=edition=community",
            "token": "extended_sanity",
        },
        {
            "suite": ["12hr_upgrade"],
            "serverPoolId": "upgrade",
            "addPoolId": "elastic-fts",
            "token": "extended_sanity",
        },
    ],

    "analytics": [
        {
            "suite": ["12hour", "weekly", "12hr_weekly"],
            "serverPoolId": "regression",
            "extraParameters": "get-cbcollect-info=True",
            "token": "extended_sanity",
        },
        {
            "suite": ["12hr_upgrade"],
            "serverPoolId": "upgrade",
            "addPoolId": "elastic-fts",
            "token": "extended_sanity",
        },
    ],

    "backup_recovery": [
        {
            "suite": ["12hour", "weekly", "12hr_weekly"],
            "serverPoolId": "regression",
            "extraParameters": "get-cbcollect-info=True",
            "token": "extended_sanity",
        },
        {
            "suite": ["12hr_upgrade"],
            "serverPoolId": "upgrade",
            "addPoolId": "elastic-fts",
            "token": "extended_sanity",
        },
    ],

    "ce_only": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "subcomponent": "1a,1b",
        "serverPoolId": "regression",
        "executor_job_parameters": "installParameters=edition=community",
        "token": "extended_sanity",
    }],

    "cli": [
        {
            "suite": ["12hour", "weekly", "12hr_weekly"],
            "serverPoolId": "regression",
            "extraParameters": "get-cbcollect-info=True",
            "token": "extended_sanity",
        },
        {
            "suite": ["12hr_upgrade"],
            "serverPoolId": "upgrade",
            "addPoolId": "elastic-fts",
            "token": "extended_sanity",
        },
    ],

    "cli_imex": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "regression",
        "extraParameters": "get-cbcollect-info=True",
        "token": "extended_sanity",
    }],

    "collections": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "regression",
        "extraParameters": "get-cbcollect-info=True",
        "token": "extended_sanity",
    }],

    "durability": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "regression",
        "extraParameters": "get-cbcollect-info=True",
        "token": "extended_sanity",
    }],

    "epeng": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "regression",
        "extraParameters": "get-cbcollect-info=True",
        "token": "extended_sanity",
    }],

    "ephemeral": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "regression",
        "extraParameters": "get-cbcollect-info=True",
        "token": "extended_sanity",
    }],

    "eventing": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "regression",
        "extraParameters": "get-cbcollect-info=True",
        "token": "extended_sanity",
    }],

    "failover_network": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "failover",
        "extraParameters": "get-cbcollect-info=True",
        "token": "extended_sanity",
    }],

    "fts": [
        {
            "suite": ["12hour", "weekly", "12hr_weekly"],
            "serverPoolId": "regression",
            "addPoolId": "elastic-fts",
            "extraParameters": "get-cbcollect-info=True",
            "token": "extended_sanity",
        },
        {
            "suite": ["vector_search_large"],
            "serverPoolId": "magmanew",
            "addPoolId": "None",
            "extraParameters": "get-cbcollect-info=True",
            "token": "extended_sanity",
        },
        {
            "suite": ["12hr_upgrade"],
            "serverPoolId": "upgrade",
            "addPoolId": "elastic-fts",
            "token": "extended_sanity",
        },
    ],

    "geo": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "regression",
        "extraParameters": "get-cbcollect-info=True",
        "token": "extended_sanity",
    }],

    "logredaction": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "regression",
        "extraParameters": "get-cbcollect-info=True",
        "token": "extended_sanity",
    }],

    "nserv": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "regression",
        "extraParameters": "get-cbcollect-info=True",
        "token": "extended_sanity",
    }],

    "plasma": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "magmanew",
        "extraParameters": "get-cbcollect-info=True",
        "token": "extended_sanity",
    }],

    "query": [
        {
            "suite": ["12hour", "weekly", "12hr_weekly"],
            "serverPoolId": "regression",
            "extraParameters": "get-cbcollect-info=True",
            "token": "extended_sanity",
        },
        {
            "suite": ["ce"],
            "serverPoolId": "regression",
            "executor_job_parameters": "installParameters=edition=community",
            "token": "extended_sanity",
        },
        {
            "suite": ["12hr_upgrade"],
            "serverPoolId": "upgrade",
            "addPoolId": "elastic-fts",
            "token": "extended_sanity",
        },
    ],

    "rbac": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "regression",
        "extraParameters": "get-cbcollect-info=True",
        "token": "extended_sanity",
    }],

    "rqg": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "regression",
        "token": "extended_sanity",
    }],

    "rza": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "regression",
        "extraParameters": "get-cbcollect-info=True",
        "token": "extended_sanity",
    }],

    "security": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "regression",
        "extraParameters": "get-cbcollect-info=True",
        "token": "extended_sanity",
    }],

    "subdoc": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "regression",
        "extraParameters": "get-cbcollect-info=True",
        "token": "extended_sanity",
    }],

    "transaction": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "regression",
        "extraParameters": "get-cbcollect-info=True",
        "token": "extended_sanity",
    }],

    "tunable": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "regression",
        "extraParameters": "get-cbcollect-info=True",
        "token": "extended_sanity",
    }],

    "upgrade": [
        {
            "suite": ["12hr_upgrade"],
            "serverPoolId": "upgrade",
            "addPoolId": "elastic-fts",
            "token": "extended_sanity",
        },
        {
            "suite": ["12hr_upgrade_TAF"],
            "serverPoolId": "upgrade",
            "addPoolId": "elastic-fts",
            "token": "extended_sanity",
        }
    ],

    "view": [{
        "suite": ["12hour", "weekly", "12hr_weekly"],
        "serverPoolId": "regression",
        "extraParameters": "get-cbcollect-info=True",
        "token": "extended_sanity",
    }],

    "xdcr": [
        {
            "suite": ["12hour", "weekly", "12hr_weekly"],
            "retries": 2,
            "serverPoolId": "regression",
            "extraParameters": "get-cbcollect-info=True",
            "token": "extended_sanity",
        },
        {
            "suite": ["12hr_upgrade"],
            "serverPoolId": "upgrade",
            "addPoolId": "elastic-fts",
            "token": "extended_sanity",
        },
    ],
}
