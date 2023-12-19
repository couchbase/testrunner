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

def invite_user(token, api_url, user, password, tenant):
    user_email = seed_email(user)
    # 1+ lowercase 1+ uppercase 1+ symbols 1+ numbers
    user_password = str(uuid4()) + "!1Aa"

    # invite user
    try:
        api = CapellaAPI(api_url, None, None, user, password)
        resp = api.invite_new_user(tenant, user_email, token)
        resp.raise_for_status()
        verify_token = resp.headers["Vnd-project-Avengers-com-e2e-token"]
        user_id = resp.json()["userId"]
    except Exception as err:
        print("ERROR: failed to invite user")
        print(str(err))
        return None, None

    # verify user
    try:
        resp = api.verify_email(verify_token)
        resp.raise_for_status()
        jwt = resp.json()["jwt"]
    except:
        print("ERROR: failed to verify user")
        return None, None

    # update user password
    headers = {
        "Authorization": "Bearer {}".format(jwt),
        "Content-Type": "application/json"
    }
    body = {
        "password": user_password
    }
    internal_url = api_url.replace("https://cloud", "https://", 1)
    try:
        resp = requests.patch("{}/users/{}".format(internal_url, user_id),
                              data=json.dumps(body), headers=headers)
        resp.raise_for_status()
    except:
        print("ERROR: failed to set password")
        return None, None

    return user_email, user_password
