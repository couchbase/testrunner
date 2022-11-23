from uuid import uuid4
import argparse
import requests, json
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from lib.capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI

def seed_email(email):
    uuid = uuid4()
    a, b = email.split("@")
    return "{}+{}@{}".format(a, uuid, b)

def main():
    all_args = argparse.ArgumentParser()

    all_args.add_argument("-e", "--email", required=True, help="Email of the user")
    all_args.add_argument("-x", "--token", required=True, help="Token for bypassing email validation")
    all_args.add_argument("-a", "--api-url", required=True, help="Internal API URL to use")
    all_args.add_argument("-u", "--user", required=True, help="Capella user")
    all_args.add_argument("-p", "--password", required=True, help="Capella password")
    all_args.add_argument("-t", "--tenant", required=True, help="Tenant to use")

    args = vars(all_args.parse_args())

    email = seed_email(args['email'])

    # 1+ lowercase 1+ uppercase 1+ symbols 1+ numbers
    password = str(uuid4()) + "!1Aa"

    api = CapellaAPI(args['api_url'], None, None, args['user'], args['password'])

    resp = api.invite_new_user(args['tenant'], email, args['token'])
    resp.raise_for_status()
    verify_token = resp.headers["Vnd-project-Avengers-com-e2e-token"]
    user_id = resp.json()["userId"]

    resp = api.verify_email(verify_token)
    resp.raise_for_status()
    jwt = resp.json()["jwt"]

    # update password
    headers = {
        "Authorization": "Bearer {}".format(jwt),
        "Content-Type": "application/json"
    }
    body = {
        "password": password
    }
    resp = requests.patch("{}/users/{}".format(args['api_url'], user_id), data=json.dumps(body), headers=headers)
    resp.raise_for_status()

    user_info = {
        "pod": args["api_url"],
        "tenant_id": args["tenant"],
        "capella_user": email,
        "capella_pwd": password
    }
    # print(f"{email} {password} {user_id}")
    print(json.dumps(user_info))


if __name__ == '__main__':
    main()
