from uuid import uuid4
import argparse
import requests, json
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from lib.capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI

def seed_email(email):
    uuid = uuid4()
    a, b = email.split("@")
    return f"{a}", f"{a}+{uuid}@{b}"

def main():
    all_args = argparse.ArgumentParser()

    all_args.add_argument("-e", "--email", required=True, help="Email of the user")
    all_args.add_argument("-x", "--token", required=True, help="Token for bypassing email validation")
    all_args.add_argument("-a", "--api-url", required=True, help="Internal API URL to use")
    all_args.add_argument("-r", "--region", required=True, help="region to use (for information only)")
    args = vars(all_args.parse_args())

    full_name, email = seed_email(args["email"])
    # 1+ lowercase 1+ uppercase 1+ symbols 1+ numbers
    password = str(uuid4()) + "!1Aa"

    api = CapellaAPI(args['api_url'], None, None, args['email'], password)
    resp = api.signup_user(full_name, email, password, full_name, args['token'])
    resp.raise_for_status()
    verify_token = resp.headers["Vnd-project-Avengers-com-e2e-token"]
    user_id = resp.json()["userId"]
    tenant_id = resp.json()["tenantId"]

    resp = api.verify_email(verify_token)
    resp.raise_for_status()

    api = CapellaAPI(args['api_url'], None, None, email, password)
    resp = api.create_project(tenant_id, "job_executor")
    resp.raise_for_status()
    project_id = resp.json()["id"]

    user_info = {
        "pod": args["api_url"],
        "tenant_id": tenant_id,
        "capella_user": email,
        "capella_pwd": password,
        "project_id": project_id,
        "region": args["region"]
    }
    print(json.dumps(user_info))

if __name__ == '__main__':
    main()