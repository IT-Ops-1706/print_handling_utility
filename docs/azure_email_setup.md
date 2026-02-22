# Azure App Registration: Granting Email Permissions

The "Flatten Orchestrator" uses **Application Permissions** (Client Credentials Flow) to send emails. This is different from "Delegated Permissions" (where a user logs in).

**Even if your account can send email, satisfy this check to allow the *application* to send email.**

## Step-by-Step Guide

1. **Log in to Azure Portal**
   - Go to [portal.azure.com](https://portal.azure.com).
   - Ensure you are in the correct directory/tenant.

2. **Navigate to App Registrations**
   - Search for **"App registrations"** in the top search bar.
   - Click on **App registrations**.

3. **Select Your Application**
   - Find and click on the application name that matches your `EMAIL_CLIENT_ID`.
   - *If you don't know which one, check the Client ID column against your `.env` file.*

4. **Go to API Permissions**
   - In the left sidebar, under **Manage**, click **API permissions**.

5. **Add the Permission**
   - Click the **+ Add a permission** button.
   - Select **Microsoft Graph** (large box at the top).
   - Select **Application permissions** (NOT Delegated permissions).
     - *Crucial Step: The application runs as a background service, so it needs Application permissions.*
   - In the search bar, type `Mail.Send`.
   - Expand **Mail** and check the box for **`Mail.Send`** (Send mail as any user).
   - Click **Add permissions** at the bottom.

6. **Grant Admin Consent (REQUIRED)**
   - You will see `Mail.Send` in the list, likely with a "Not granted" status or a warning.
   - Click the **Grant admin consent for [Your Organization]** button next to "Add a permission".
     - *Note: Only an Global Administrator or Application Administrator can click this.*
   - Confirm by clicking **Yes**.
   - The "Status" column should turn into a green checkmark saying **"Granted for [Your Organization]"**.

## Troubleshooting

- **"Grant admin consent" button is grayed out?**
  - You do not have sufficient permissions. You must ask an IT Administrator to do Step 6 for you.

- **Still getting 403 Forbidden?**
  - Wait 5-10 minutes. Azure permissions can take a moment to propagate.
  - Restart the application (`python main.py`) to force a fresh token fetch.
