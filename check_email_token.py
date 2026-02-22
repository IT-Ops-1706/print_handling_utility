import os, base64, json, msal
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

tenant_id = os.environ.get("EMAIL_TENANT_ID")
client_id  = os.environ.get("EMAIL_CLIENT_ID")
client_secret = os.environ.get("EMAIL_CLIENT_SECRET")

print(f"Tenant : {tenant_id}")
print(f"Client : {client_id}")

app = msal.ConfidentialClientApplication(
    client_id,
    authority=f"https://login.microsoftonline.com/{tenant_id}",
    client_credential=client_secret,
)
result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])

if "access_token" not in result:
    print(f"Token acquisition failed: {result.get('error_description')}")
else:
    token = result["access_token"]
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    claims = json.loads(base64.b64decode(payload))

    roles = claims.get("roles", [])
    print(f"App permission roles in token: {roles}")

    if "Mail.Send" in roles:
        print("RESULT: Mail.Send application permission IS granted — code is correct, something else is wrong.")
    else:
        print("RESULT: Mail.Send application permission is NOT in token.")
        print("        This confirms the 403: admin consent for Mail.Send (application) has not been granted.")
