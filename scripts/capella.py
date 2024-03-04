from uuid import uuid4
import requests, json
import urllib3
import sys
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
sys.path.extend(('.', 'lib'))
from lib.capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI

def seed_email(email):
    uuid = uuid4()
    name, domain = email.split("@")
    if "+" in name:
        prefix, _ = name.split("+")
    else:
        prefix = name
    return "{}+{}@{}".format(prefix, uuid, domain)

def invite_user(api_url, user, password, tenant):
    user_email = seed_email(user)
    user_name = user_email.split("@")[0]
    # 1+ lowercase 1+ uppercase 1+ symbols 1+ numbers
    user_password = str(uuid4()) + "!1Aa"

    # invite user
    try:
        api = CapellaAPI(api_url, None, None, user, password)
        resp = api.create_user(
            tenant, user_name, user_email, user_password)
        if resp.status_code != 200:
            print("Creating capella User {0} failed: {1}".format(
                user_name, resp.content))
            return None, None
        result = json.loads(resp.content).get("data")
        # Verify whether the user is marked as verified.
        if not result.get("addresses")[0].get("status") == "verified":
            print("User {0} was not verified. Verification bypass flag might "
                  "not be active.".format(user_email))
            return None, None
        return user_email, user_password
    except Exception as err:
        print("ERROR: failed to invite user")
        print(str(err))
        return None, None
