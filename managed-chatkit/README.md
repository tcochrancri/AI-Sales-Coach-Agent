# Managed ChatKit starter

Vite + React UI that talks to a FastAPI session backend for creating ChatKit
workflow sessions.

## Quick start

```bash
npm install           # installs root deps (concurrently)
npm run dev           # runs FastAPI on :8000 and Vite on :3001
```

What happens:

- `npm run dev` runs the backend via `backend/scripts/run.mjs` (which dispatches
  to platform-native scripts) and the frontend via `npm --prefix frontend run dev`.
- The backend exposes `/api/create-session`, exchanging your workflow id and
  `OPENAI_API_KEY` for a ChatKit client secret. The Vite dev server proxies
  `/api/*` to `127.0.0.1:8000`.
- The backend also exposes `/api/grant-campaign/generate` for structured
  grant-award outreach generation.
- The backend also exposes `/api/apollo/enrich-recipients` to enrich campaign
  stakeholder emails from Apollo.

## Required environment

- `OPENAI_API_KEY`
- `APOLLO_API_KEY` (required for `/api/apollo/enrich-recipients`)
- `VITE_CHATKIT_WORKFLOW_ID`
- (optional) `GRANT_CAMPAIGN_MODEL` (default `gpt-4.1`)
- (optional) `GRANT_RESEARCH_MODEL` (default `gpt-4.1-mini`)
- (optional) `SHAREPOINT_MCP_BASE_URL` (used by `/api/case-studies/recommend` to call `sharepoint_matchBidFiles` and return top matched presentation assets)
- (optional) `SHAREPOINT_MCP_BEARER` (if your SharePoint MCP server protects POST `/tools/*`)
- (optional) `SHAREPOINT_MATCH_SHARE_URL` (override root folder; otherwise tool default share URL is used)
- (optional) `SHAREPOINT_MATCH_FILE_EXTENSIONS` (comma-separated, default `pptx,ppt`)
- (optional) `SHAREPOINT_PREPARE_PACKAGE_TOOL` (MCP tool used by `/api/assets/prepare-package`; default `sharepoint_prepareAttachmentPackage`)
- (optional) `CHATKIT_API_BASE` or `VITE_CHATKIT_API_BASE` (defaults to `https://api.openai.com`)
- (optional) `VITE_API_URL` (override the dev proxy target for `/api`)
- (optional) `VITE_APOLLO_APP_URL` (default `https://app.apollo.io/`; used by "Open Apollo" button)
- (optional) `CORS_ORIGINS` (comma-separated; backend only; default includes production frontend)

Set the env vars in your shell (or process manager) before running. Use a
workflow id from Agent Builder (starts with `wf_...`) and an API key from the
same project and organization.

## Railway deployment (CORS)

When frontend and backend are separate Railway services:

1. **Backend** (`railway.json` in `backend/`): set `CORS_ORIGINS` to your frontend URL(s), e.g.  
   `CORS_ORIGINS=https://ai-sales-coach-agent-production.up.railway.app`
2. **Frontend**: set `VITE_API_URL` at build time to your backend URL, e.g.  
   `VITE_API_URL=https://selfless-laughter-production-969a.up.railway.app`

If you see "blocked by CORS policy: No 'Access-Control-Allow-Origin' header":

- Confirm the backend is running: `curl https://<backend-url>/health`
- Ensure `CORS_ORIGINS` exactly matches the frontend origin (protocol, host, no trailing slash)
- Redeploy the backend after changing `CORS_ORIGINS`

## Customize

- UI: `frontend/src/components/ChatKitPanel.tsx`
- Grant tool UI: `frontend/src/components/GrantCampaignTool.tsx`
- Session logic: `backend/app/main.py`

## Grant Campaign API

Endpoint:

`POST /api/grant-campaign/generate`

Purpose:

- Accept a strict `grant_awardee_outreach` schema payload.
- Generate a consultant-implementation outreach campaign from grant context.
- Return both structured campaign JSON and flattened text (`campaign_text`).

Minimum input behavior:

- Only `mode` is strictly required.
- Missing optional fields are normalized to defaults (`N/A`, `NONE`, `0`, or generated `lead_id`) so generation can proceed with partial data.

## Apollo Recipient Enrichment API

Endpoint:

`POST /api/apollo/enrich-recipients`

Purpose:

- Accept stakeholder names/titles from campaign output.
- Call Apollo People Enrichment (`/api/v1/people/match`) per recipient.
- Return matched business email and profile metadata when available.

Sample payload:

```json
{
  "organization_name": "Arizona Secretary of State (SOS)",
  "organization_website": "https://azsos.gov",
  "reveal_personal_emails": false,
  "recipients": [
    { "full_name": "Kuru Mathew", "title": "CIO" },
    { "full_name": "Michael Moore", "title": "CISO" }
  ]
}
```
