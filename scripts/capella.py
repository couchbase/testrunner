from uuid import uuid4
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

        # Sign-Up a new user
        resp = api.signup_user(
            user_name, user_email, user_password, tenant, token)
        resp.raise_for_status()
        verify_token = resp.headers["Vnd-project-Avengers-com-e2e-token"]

        # Verify the user using the internal token
        resp = api.verify_email(verify_token)
        resp.raise_for_status()

        # Invite the user to the organisation passed in the tenant parameter.
        # it is required because when a user sign's up, he is assigned a new
        # organisation, but we need to perform tests in the organisation
        # mentioned in tenant parameter.
        resp = api.invite_new_user(tenant, user_email, user_password)
        resp.raise_for_status()

        # Reassign the username and password for capella API object to that
        # of the user that was created above.
        api.user = user_email
        api.pwd = user_password
        api.jwt = None
        resp = api.fetch_all_invitations()
        resp.raise_for_status()
        invitations = resp.json()
        invitation_id = ""
        for invitation in invitations:
            if invitation["tenant"]["id"] == tenant:
                invitation_id = invitation["id"]

        resp = api.manage_invitation(invitation_id, "accept")
        resp.raise_for_status()
        return user_email, user_password
    except Exception as err:
        print(str(err))
        return None, None
