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
    user_name = user_email.split("@")[0]
    # 1+ lowercase 1+ uppercase 1+ symbols 1+ numbers
    user_password = str(uuid4()) + "!1Aa"
    # invite user
    try:
        api = CapellaAPI(api_url, None, None, user, password)
        resp = api.signup_user(
            user_name, user_email, user_password, tenant, token)
        resp.raise_for_status()
        verify_token = resp.headers["Vnd-project-Avengers-com-e2e-token"]
        user_id = resp.json()["userId"]
    except Exception as e:
        print("ERROR: failed to invite user")
        return None, None
        # verify user
    try:
        resp = api.verify_email(verify_token)
        resp.raise_for_status()
        return user_email, user_password
    except:
        print("ERROR: failed to verify user")
        return None, None
        # update user password
