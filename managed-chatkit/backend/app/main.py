"""FastAPI entrypoint for exchanging workflow ids for ChatKit client secrets."""

from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
import ipaddress
import base64
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal, Mapping
from urllib.parse import parse_qs, quote, unquote, urlparse

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError

DEFAULT_CHATKIT_BASE = "https://api.openai.com"
DEFAULT_OPENAI_BASE = "https://api.openai.com"
SESSION_COOKIE_NAME = "chatkit_session_id"
SESSION_COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24 * 30  # 30 days

LOG_LEVEL = (os.getenv("LOG_LEVEL") or "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("sales_coach_api")
HUBSPOT_RUNTIME_ACCESS_TOKEN: str | None = None
DOMAIN_DISCOVERY_CACHE_TTL_SECONDS = int(os.getenv("DOMAIN_DISCOVERY_CACHE_TTL_SECONDS", "21600"))
DOMAIN_DISCOVERY_CACHE: dict[str, tuple[str, float]] = {}
GOOGLE_SEARCH_DAILY_LIMIT = int(os.getenv("GOOGLE_SEARCH_DAILY_LIMIT", "100"))
BRAVE_SEARCH_MONTHLY_LIMIT = int(os.getenv("BRAVE_SEARCH_MONTHLY_LIMIT", "1000"))
DOMAIN_SEARCH_QUOTA: dict[str, dict[str, Any]] = {
    "google": {"bucket": "", "count": 0},
    "brave": {"bucket": "", "count": 0},
}

MODEL_PRICING_PER_MILLION: dict[str, dict[str, float]] = {
    "gpt-4.1": {"input": 2.0, "output": 8.0},
    "gpt-4.1-mini": {"input": 0.4, "output": 1.6},
    "gpt-4.1-nano": {"input": 0.1, "output": 0.4},
}
WEB_SEARCH_PREVIEW_COST_PER_CALL_USD = 0.025
DEFAULT_SHAREPOINT_MATCH_FILE_EXTENSIONS = ["pptx", "ppt"]

app = FastAPI(title="Managed ChatKit Session API")

# CORS: configurable via CORS_ORIGINS (comma-separated). Must match frontend origin exactly.
_default_origins = ["https://ai-sales-coach-agent-production.up.railway.app"]
_cors_origins = [
    o.strip()
    for o in (os.getenv("CORS_ORIGINS") or "").split(",")
    if o.strip()
]
if not _cors_origins:
    _cors_origins = _default_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=600,
)


@app.get("/health")
async def health() -> Mapping[str, str]:
    return {"status": "ok"}


@app.post("/api/create-session")
async def create_session(request: Request) -> JSONResponse:
    """Exchange a workflow id for a ChatKit client secret."""
    request_id = str(uuid.uuid4())[:8]
    logger.info("[session:%s] create-session request received", request_id)
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.error("[session:%s] OPENAI_API_KEY missing", request_id)
        return respond({"error": "Missing OPENAI_API_KEY environment variable"}, 500)

    body = await read_json_body(request)
    workflow_id = resolve_workflow_id(body)
    if not workflow_id:
        logger.warning("[session:%s] missing workflow id", request_id)
        return respond({"error": "Missing workflow id"}, 400)

    user_id, cookie_value = resolve_user(request.cookies)
    api_base = chatkit_api_base()

    try:
        async with httpx.AsyncClient(base_url=api_base, timeout=10.0) as client:
            upstream = await client.post(
                "/v1/chatkit/sessions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "OpenAI-Beta": "chatkit_beta=v1",
                    "Content-Type": "application/json",
                },
                json={"workflow": {"id": workflow_id}, "user": user_id},
            )
    except httpx.RequestError as error:
        logger.exception("[session:%s] upstream request error: %s", request_id, error)
        return respond(
            {"error": f"Failed to reach ChatKit API: {error}"},
            502,
            cookie_value,
        )

    payload = parse_json(upstream)
    if not upstream.is_success:
        message = None
        if isinstance(payload, Mapping):
            message = payload.get("error")
        message = message or upstream.reason_phrase or "Failed to create session"
        logger.error(
            "[session:%s] upstream non-success status=%s message=%s",
            request_id,
            upstream.status_code,
            message,
        )
        return respond({"error": message}, upstream.status_code, cookie_value)

    client_secret = None
    expires_after = None
    if isinstance(payload, Mapping):
        client_secret = payload.get("client_secret")
        expires_after = payload.get("expires_after")

    if not client_secret:
        logger.error("[session:%s] missing client_secret in response", request_id)
        return respond(
            {"error": "Missing client secret in response"},
            502,
            cookie_value,
        )

    logger.info("[session:%s] session created", request_id)
    return respond(
        {"client_secret": client_secret, "expires_after": expires_after},
        200,
        cookie_value,
    )


class OrganizationPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    website: str | None = None
    city: str | None = None
    state: str | None = None


class AwardPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: Literal["usaspending"] = "usaspending"
    award_id: str | None = None
    generated_internal_id: str | None = None
    agency: str | None = None
    amount: Decimal | None = Field(default=None, ge=0)
    award_date: str | None = None
    period_start: str | None = None
    period_end: str | None = None
    cfda_number: str | None = None
    cfda_title: str | None = None
    description: str | None = None
    place_of_performance: str | None = None


class EvidenceItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    label: str | None = None
    url: str | None = None
    source: Literal["usaspending", "sam", "grants_gov"]
    excerpt: str | None = None


class ConstraintsPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_recipients: int = Field(default=2, ge=1, le=5)
    version: Literal[1] = 1


class ProvidedProspect(BaseModel):
    model_config = ConfigDict(extra="forbid")

    full_name: str | None = None
    title: str | None = None
    organization: str | None = None
    linkedin_url: str | None = None
    note: str | None = None


class GrantCampaignGenerateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["grant_awardee_outreach"]
    lead_id: str | None = None
    organization: OrganizationPayload = Field(default_factory=OrganizationPayload)
    award: AwardPayload = Field(default_factory=AwardPayload)
    evidence: list[EvidenceItem] = Field(default_factory=list)
    prospects: list[ProvidedProspect] = Field(default_factory=list)
    constraints: ConstraintsPayload = Field(default_factory=ConstraintsPayload)


class GrantRecipient(BaseModel):
    model_config = ConfigDict(extra="forbid")

    label: str
    persona: str
    rationale: str


class GrantEmail(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email_number: Literal[1, 2, 3, 4]
    subject: str
    body: str


class GrantCampaign(BaseModel):
    model_config = ConfigDict(extra="forbid")

    campaign_title: str
    strategy_summary: str
    recipients: list[GrantRecipient]
    prospect_campaigns: list["ProspectCampaign"] = Field(default_factory=list)


class ProspectCampaign(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recipient_label: str
    recipient_persona: str
    recipient_rationale: str
    emails: list[GrantEmail]


class ProspectSignal(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fact: str
    source_url: str


class ProspectBrief(BaseModel):
    model_config = ConfigDict(extra="forbid")

    full_name: str
    title: str
    organization: str
    linkedin_url: str
    research_confidence: Literal["HIGH", "MEDIUM", "LOW"]
    signals: list[ProspectSignal]
    personalization_angle: str


class ProjectEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fact: str
    source_url: str


class ProjectResearchBrief(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_summary: str
    urgency_drivers: list[str]
    implementation_risks: list[str]
    decision_triggers: list[str]
    evidence: list[ProjectEvidence]


class ApolloRecipientLookup(BaseModel):
    model_config = ConfigDict(extra="forbid")

    full_name: str
    title: str | None = None


class ApolloEnrichRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recipients: list[ApolloRecipientLookup]
    organization_name: str | None = None
    organization_website: str | None = None
    reveal_personal_emails: bool = False


class ApolloRecipientResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    full_name: str
    title: str | None = None
    found: bool
    email: str | None = None
    phone: str | None = None
    email_status: str | None = None
    linkedin_url: str | None = None
    apollo_person_id: str | None = None
    source: str = "apollo"
    detail: str | None = None


class HubspotContextRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    organization_name: str | None = None
    organization_website: str | None = None
    organization_industry: str | None = None
    organization_city: str | None = None
    organization_state: str | None = None
    max_items: int = Field(default=10, ge=1, le=50)
    years_back: int = Field(default=5, ge=1, le=25)
    closed_won_only: bool = True
    project_signal_text: str | None = None


class ApolloAccountSnapshotRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    organization_name: str | None = None
    organization_website: str | None = None


class CaseStudyRecommendRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    organization_name: str | None = None
    industry_vertical: str | None = None
    project_description: str | None = None
    max_items: int = Field(default=3, ge=1, le=10)


class AssetPackageItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str | None = None
    title: str | None = None
    url: str | None = None
    path: str | None = None
    score: float | None = None


class AssetPreparePackageRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    organization_name: str | None = None
    assets: list[AssetPackageItem] = Field(default_factory=list, min_length=1, max_length=10)


class ApolloAttachEmailStepAssetsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sequence_id: str = Field(min_length=1)
    email_step_number: int = Field(default=1, ge=1, le=10)
    organization_name: str | None = None
    max_total_bytes: int = Field(default=150_000_000, ge=1_000_000, le=250_000_000)
    max_zip_bytes: int = Field(default=75_000_000, ge=1_000_000, le=100_000_000)
    assets: list[AssetPackageItem] = Field(default_factory=list, min_length=1, max_length=10)


class GrantCampaignRegenerateEmailRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    lead_id: str | None = None
    organization: OrganizationPayload = Field(default_factory=OrganizationPayload)
    award: AwardPayload = Field(default_factory=AwardPayload)
    evidence: list[EvidenceItem] = Field(default_factory=list)
    recipient_label: str
    recipient_persona: str
    recipient_rationale: str
    target_email_number: Literal[1, 2, 3, 4]
    existing_sequence: list[GrantEmail] = Field(default_factory=list, min_length=1, max_length=4)


@app.post("/api/grant-campaign/generate")
async def generate_grant_campaign(request: Request) -> JSONResponse:
    """Generate consultant-implementation outreach campaign from grant award input."""
    request_id = str(uuid.uuid4())[:8]
    started = time.perf_counter()
    logger.info("[grant:%s] request received", request_id)
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.error("[grant:%s] OPENAI_API_KEY missing", request_id)
        return respond({"error": "Missing OPENAI_API_KEY environment variable"}, 500)

    body = await read_json_body(request)
    try:
        payload = GrantCampaignGenerateRequest.model_validate(body)
    except ValidationError as error:
        logger.warning("[grant:%s] payload validation failed: %s", request_id, error.errors())
        return respond(
            {"error": "Invalid request payload", "details": error.errors()},
            400,
        )

    payload = normalize_grant_payload(payload)
    model = os.getenv("GRANT_CAMPAIGN_MODEL", "gpt-4.1")
    research_model = os.getenv("GRANT_RESEARCH_MODEL", "gpt-4.1-mini")
    cost_tracker = init_cost_tracker(request_id)
    logger.info(
        "[grant:%s] normalized org=%s award=%s prospects=%s max_recipients=%s model=%s",
        request_id,
        payload.organization.name,
        payload.award.award_id,
        len(payload.prospects),
        payload.constraints.max_recipients,
        model,
    )
    try:
        if not payload.prospects:
            inferred = infer_prospects_from_context(payload)
            if inferred:
                payload.prospects = inferred
                logger.info(
                    "[grant:%s] inferred prospects from input context count=%s",
                    request_id,
                    len(inferred),
                )

        if not payload.prospects:
            discovered = await discover_organization_prospects(
                payload=payload,
                api_key=api_key,
                model=research_model,
                request_id=request_id,
                cost_tracker=cost_tracker,
            )
            if discovered:
                payload.prospects = discovered
                logger.info(
                    "[grant:%s] discovered prospects from web/team sources count=%s",
                    request_id,
                    len(discovered),
                )
            else:
                logger.info("[grant:%s] no named prospects discovered", request_id)

        project_research = await generate_project_research(
            payload=payload,
            api_key=api_key,
            model=research_model,
            request_id=request_id,
            cost_tracker=cost_tracker,
        )
        logger.info(
            "[grant:%s] project research evidence count=%s",
            request_id,
            len(project_research.evidence),
        )

        prospect_briefs = await generate_prospect_briefs(
            payload=payload,
            api_key=api_key,
            model=research_model,
            request_id=request_id,
            cost_tracker=cost_tracker,
        )
        logger.info(
            "[grant:%s] prospect briefs generated count=%s",
            request_id,
            len(prospect_briefs),
        )
        recipient_strategy = await generate_recipient_strategy(
            payload, api_key, model, request_id, prospect_briefs, cost_tracker
        )
        logger.info(
            "[grant:%s] recipients generated count=%s",
            request_id,
            len(recipient_strategy),
        )
        campaign = await generate_email_campaign(
            payload,
            recipient_strategy,
            prospect_briefs,
            project_research,
            api_key,
            model,
            request_id,
            cost_tracker,
        )
        campaign = sanitize_campaign(campaign, payload, recipient_strategy)
        campaign = enforce_source_bound_campaign(campaign, project_research, prospect_briefs)
        campaign = apply_sparse_signal_guardrails_to_campaign(campaign, payload)
    except RuntimeError as error:
        logger.exception("[grant:%s] generation failed: %s", request_id, error)
        return respond({"error": str(error)}, 502)

    elapsed_ms = int((time.perf_counter() - started) * 1000)
    logger.info(
        "[grant:%s] completed targeting_mode=%s elapsed_ms=%s",
        request_id,
        "provided_prospects" if payload.prospects else "role_discovery",
        elapsed_ms,
    )
    cost_summary = summarize_cost_tracker(cost_tracker)
    logger.info(
        "[grant:%s] cost_estimate total_usd=%.4f input_tokens=%s output_tokens=%s web_search_calls=%s",
        request_id,
        cost_summary["estimated_total_usd"],
        cost_summary["input_tokens"],
        cost_summary["output_tokens"],
        cost_summary["web_search_calls"],
    )
    response_payload: dict[str, Any] = {
        "mode": payload.mode,
        "lead_id": payload.lead_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "targeting_mode": "provided_prospects"
        if payload.prospects
        else "role_discovery",
        "minimum_fields_used": {
            "organization_name": payload.organization.name,
            "organization_website": payload.organization.website,
            "award_id": payload.award.award_id,
        },
        "campaign": campaign.model_dump(),
        "campaign_text": render_campaign_text(campaign),
    }
    if include_debug_fields():
        response_payload["debug_cost_estimate"] = cost_summary
        response_payload["debug_prospect_briefs"] = [
            brief.model_dump(mode="json") for brief in prospect_briefs
        ]
        response_payload["debug_project_research"] = project_research.model_dump(mode="json")
    return respond(response_payload, 200)


@app.post("/api/grant-campaign/regenerate-email")
async def regenerate_grant_campaign_email(request: Request) -> JSONResponse:
    request_id = str(uuid.uuid4())[:8]
    logger.info("[grant:%s] regenerate-email request received", request_id)
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return respond({"error": "Missing OPENAI_API_KEY environment variable"}, 500)

    body = await read_json_body(request)
    try:
        regen_request = GrantCampaignRegenerateEmailRequest.model_validate(body)
    except ValidationError as error:
        return respond({"error": "Invalid request payload", "details": error.errors()}, 400)

    payload = normalize_grant_payload(
        GrantCampaignGenerateRequest(
            mode="grant_awardee_outreach",
            lead_id=regen_request.lead_id,
            organization=regen_request.organization,
            award=regen_request.award,
            evidence=regen_request.evidence,
            prospects=[],
            constraints=ConstraintsPayload(max_recipients=1, version=1),
        )
    )
    recipient = GrantRecipient(
        label=compact_spaces(regen_request.recipient_label),
        persona=compact_spaces(regen_request.recipient_persona),
        rationale=compact_spaces(regen_request.recipient_rationale),
    )
    existing_sequence = sanitize_email_block(
        regen_request.existing_sequence, regen_request.recipient_label
    )
    research_model = os.getenv("GRANT_RESEARCH_MODEL", "gpt-4.1-mini")
    model = os.getenv("GRANT_CAMPAIGN_MODEL", "gpt-4.1")
    cost_tracker = init_cost_tracker(request_id)

    project_research = await generate_project_research(
        payload=payload,
        api_key=api_key,
        model=research_model,
        request_id=request_id,
        cost_tracker=cost_tracker,
    )
    regenerated = await regenerate_single_email(
        payload=payload,
        recipient=recipient,
        existing_sequence=existing_sequence,
        target_email_number=regen_request.target_email_number,
        project_research=project_research,
        api_key=api_key,
        model=model,
        request_id=request_id,
        cost_tracker=cost_tracker,
    )

    replaced = False
    updated_sequence: list[GrantEmail] = []
    for email in existing_sequence:
        if email.email_number == regen_request.target_email_number:
            updated_sequence.append(regenerated)
            replaced = True
        else:
            updated_sequence.append(email)
    if not replaced:
        updated_sequence.append(regenerated)

    provisional_campaign = GrantCampaign(
        campaign_title="Regenerated Sequence",
        strategy_summary="Single email regeneration.",
        recipients=[recipient],
        prospect_campaigns=[
            ProspectCampaign(
                recipient_label=recipient.label,
                recipient_persona=recipient.persona,
                recipient_rationale=recipient.rationale,
                emails=updated_sequence,
            )
        ],
    )
    cleaned_campaign = enforce_source_bound_campaign(
        provisional_campaign,
        project_research=project_research,
        prospect_briefs=[],
    )
    cleaned_campaign = apply_sparse_signal_guardrails_to_campaign(cleaned_campaign, payload)
    cleaned_sequence = sanitize_email_block(
        cleaned_campaign.prospect_campaigns[0].emails, recipient.label
    )
    current_email = next(
        (email for email in cleaned_sequence if email.email_number == regen_request.target_email_number),
        None,
    )
    if not current_email:
        return respond({"error": "Regenerated email could not be finalized."}, 500)

    return respond(
        {
            "recipient_label": recipient.label,
            "email": current_email.model_dump(),
            "sequence": [email.model_dump() for email in cleaned_sequence],
        },
        200,
    )


@app.post("/api/hubspot/context")
async def hubspot_context(request: Request) -> JSONResponse:
    global HUBSPOT_RUNTIME_ACCESS_TOKEN
    request_id = str(uuid.uuid4())[:8]
    logger.info("[hubspot:%s] context request received", request_id)

    body = await read_json_body(request)
    try:
        payload = HubspotContextRequest.model_validate(body)
    except ValidationError as error:
        return respond({"error": "Invalid request payload", "details": error.errors()}, 400)

    base_url = clean_optional(os.getenv("HUBSPOT_MCP_BASE_URL"))
    if not base_url:
        return respond({"error": "HUBSPOT_MCP_BASE_URL is not configured."}, 500)

    bearer = clean_env_secret_single_line(os.getenv("HUBSPOT_MCP_BEARER"))
    install_id = clean_env_secret_single_line(os.getenv("HUBSPOT_MCP_INSTALL_ID"))
    token_id = clean_env_secret_single_line(os.getenv("HUBSPOT_MCP_TOKEN_ID"))
    access_token = clean_env_secret_single_line(os.getenv("HUBSPOT_MCP_ACCESS_TOKEN"))
    refresh_token = clean_env_secret_single_line(os.getenv("HUBSPOT_MCP_REFRESH_TOKEN"))
    mcp_server_url = clean_optional(os.getenv("HUBSPOT_MCP_SERVER_URL")) or "https://mcp.hubspot.com"
    logger.info(
        "[hubspot:%s] config base_url_set=%s install_id_set=%s token_id_set=%s access_token_set=%s refresh_token_set=%s bearer_set=%s",
        request_id,
        bool(base_url),
        bool(install_id),
        bool(token_id),
        bool(access_token),
        bool(refresh_token),
        bool(bearer),
    )
    query = clean_optional(payload.organization_name)
    domain = derive_domain(payload.organization_website)
    if not domain and query:
        apollo_api_key = clean_env_secret_single_line(os.getenv("APOLLO_API_KEY"))
        if apollo_api_key:
            async with httpx.AsyncClient(base_url="https://api.apollo.io", timeout=15.0) as apollo_client:
                discovered_domain = await discover_apollo_domain(
                    client=apollo_client,
                    api_key=apollo_api_key,
                    organization_name=query,
                )
            if discovered_domain:
                domain = discovered_domain
                logger.info(
                    "[hubspot:%s] auto-resolved organization domain=%s for query=%s",
                    request_id,
                    domain,
                    query,
                )
    # Prefer name-based search to avoid stale-domain query poisoning.
    query_text = query or domain or ""
    query_tokens = split_query_tokens(query or query_text)
    fetch_limit = max(payload.max_items, 50)
    headers: dict[str, str] = {}
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"

    async with httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=20.0) as client:
        auth_source = "install_id"
        resolved_access_token = None
        if not install_id:
            # Fallback legacy path: explicit token or token_id lookup.
            auth_source = "env_access_token"
            resolved_access_token = HUBSPOT_RUNTIME_ACCESS_TOKEN or access_token
            if HUBSPOT_RUNTIME_ACCESS_TOKEN:
                auth_source = "runtime_refresh_cache"
        if not install_id and not resolved_access_token:
            auth_source = "token_id_lookup"
            resolved_access_token = await resolve_hubspot_access_token(
                client=client,
                headers=headers,
                token_id=token_id,
            )
        if not install_id and not resolved_access_token:
            auth_source = "none"
        if not install_id and not resolved_access_token:
            return respond(
                {
                    "error": "Missing HubSpot auth context for MCP calls.",
                    "details": [
                        "Set HUBSPOT_MCP_INSTALL_ID on the server (Railway env vars) for durable token refresh (recommended), or",
                        "Set HUBSPOT_MCP_ACCESS_TOKEN (expires; rotate when HubSpot invalidates it), or",
                        "Set HUBSPOT_MCP_TOKEN_ID and ensure HUBSPOT_MCP_BASE_URL serves GET /oauth/token/:tokenId with access_token.",
                    ],
                },
                400,
            )
        request_headers = build_hubspot_request_headers(headers, resolved_access_token)
        logger.info(
            "[hubspot:%s] auth source=%s token_fingerprint=%s token_id_present=%s header_auth_set=%s",
            request_id,
            auth_source,
            token_fingerprint(resolved_access_token),
            bool(token_id),
            "Authorization" in request_headers,
        )

        await ensure_hubspot_mcp_connection(
            client=client,
            headers=request_headers,
            install_id=install_id,
            access_token=resolved_access_token,
            mcp_server_url=mcp_server_url,
        )
        bundle = await fetch_hubspot_context_bundle(
            client=client,
            headers=request_headers,
            install_id=install_id,
            token_id=token_id,
            access_token=resolved_access_token,
            query_text=query_text,
            org_name=query,
            org_domain=domain,
            max_items=fetch_limit,
        )

        if bundle["auth_error"]:
            refreshed_token, refreshed_ok = await refresh_hubspot_access_token(
                client=client,
                headers=headers,
                install_id=install_id,
                token_id=token_id,
                refresh_token=refresh_token,
            )
            if refreshed_token or (install_id and refreshed_ok):
                rt = clean_env_secret_single_line(refreshed_token) if refreshed_token else None
                HUBSPOT_RUNTIME_ACCESS_TOKEN = rt or refreshed_token
                resolved_access_token = rt or refreshed_token
                auth_source = "oauth_refresh" if refreshed_token else "install_refresh"
                request_headers = build_hubspot_request_headers(headers, resolved_access_token)
                await ensure_hubspot_mcp_connection(
                    client=client,
                    headers=request_headers,
                    install_id=install_id,
                    access_token=resolved_access_token,
                    mcp_server_url=mcp_server_url,
                )
                bundle = await fetch_hubspot_context_bundle(
                    client=client,
                    headers=request_headers,
                    install_id=install_id,
                    token_id=token_id,
                    access_token=resolved_access_token,
                    query_text=query_text,
                    org_name=query,
                    org_domain=domain,
                    max_items=fetch_limit,
                )
                logger.info(
                    "[hubspot:%s] retried with refreshed token fingerprint=%s",
                    request_id,
                    token_fingerprint(refreshed_token),
                )
            else:
                logger.warning(
                    "[hubspot:%s] auth error detected but token refresh did not return a token",
                    request_id,
                )

        summary_data = bundle["summary_data"]
        search_data = bundle["search_data"]
        companies_data = bundle["companies_data"]
        contacts_data = bundle["contacts_data"]
        deals_data = bundle["deals_data"]
        summary_error = bundle["summary_error"]
        search_error = bundle["search_error"]
        companies_error = bundle["companies_error"]
        contacts_error = bundle["contacts_error"]
        deals_error = bundle["deals_error"]
        companies_data = await extend_hubspot_companies_until_match(
            client=client,
            headers=request_headers,
            install_id=install_id,
            token_id=token_id,
            access_token=resolved_access_token,
            query_text=query_text,
            org_name=query,
            org_domain=domain,
            companies_data=companies_data,
        )

    companies_list = mapping_list(companies_data, "companies")
    search_company_candidates = extract_company_candidates_from_hubspot_search(
        search_data,
        org_name=query,
        org_domain=domain,
    )
    company_candidates = merge_company_candidates(companies_list, search_company_candidates)
    contacts_list = mapping_list(contacts_data, "contacts")
    deals_list = mapping_list(deals_data, "deals")
    account_match = await resolve_hubspot_account_match(
        org_name=query,
        org_domain=domain,
        org_industry=payload.organization_industry,
        org_city=payload.organization_city,
        org_state=payload.organization_state,
        search_data=search_data,
        companies=company_candidates,
    )
    recommended_action = build_recommended_action(
        account_match=account_match,
        deals=deals_list,
        project_signal_text=payload.project_signal_text,
    )
    exact_company_matches = filter_exact_company_matches(company_candidates, query, domain)
    exact_contact_matches = filter_exact_contact_matches(contacts_list, query, domain)
    exact_deal_matches = filter_exact_deal_matches(deals_list, query, query_tokens)
    relationship_history = build_relationship_history(
        exact_company_matches,
        exact_contact_matches,
        exact_deal_matches,
    )
    similar_wins = build_similar_closed_wins(
        deals=deals_list,
        years_back=payload.years_back,
        closed_won_only=payload.closed_won_only,
        max_items=payload.max_items,
    )

    errors = [
        message
        for message in [
            summary_error,
            search_error,
            companies_error,
            contacts_error,
            deals_error,
        ]
        if message
    ]
    if (
        summary_data is None
        and search_data is None
        and companies_data is None
        and contacts_data is None
        and deals_data is None
    ):
        return respond(
            {
                "error": "HubSpot context lookup failed.",
                "details": errors or ["No response data returned from HubSpot MCP routes."],
            },
            502,
        )

    return respond(
        {
            "retrieval_source": clean_optional(str(bundle.get("retrieval_source") or "")) or "unknown",
            "organization_name": query,
            "organization_domain": domain,
            "summary": summary_data,
            "search": search_data,
            "companies": companies_data,
            "contacts": contacts_data,
            "deals": deals_data,
            "errors": errors,
            "exact_matches": {
                "companies": exact_company_matches,
                "contacts": exact_contact_matches,
                "deals": exact_deal_matches,
                "found_any": bool(
                    exact_company_matches or exact_contact_matches or exact_deal_matches
                ),
            },
            "account_match": account_match,
            "recommended_action": recommended_action,
            "relationship_history": {
                "events": relationship_history,
                "total_events": len(relationship_history),
                "scope": "all_time",
            },
            "similar_closed_won": {
                "deals": similar_wins,
                "count": len(similar_wins),
                "filters": {
                    "years_back": payload.years_back,
                    "closed_won_only": payload.closed_won_only,
                },
            },
            "debug_auth": {
                "source": auth_source,
                "token_fingerprint": token_fingerprint(resolved_access_token),
                "install_id_present": bool(install_id),
                "token_id_present": bool(token_id),
                "access_token_present": bool(access_token),
                "bearer_present": bool(bearer),
            },
        },
        200,
    )


@app.post("/api/apollo/enrich-recipients")
async def apollo_enrich_recipients(request: Request) -> JSONResponse:
    request_id = str(uuid.uuid4())[:8]
    logger.info("[apollo:%s] enrich request received", request_id)
    api_key = clean_env_secret_single_line(os.getenv("APOLLO_API_KEY"))
    if not api_key:
        return respond({"error": "Missing APOLLO_API_KEY environment variable"}, 500)
    logger.info("[apollo:%s] api key fingerprint=%s", request_id, token_fingerprint(api_key))

    body = await read_json_body(request)
    try:
        payload = ApolloEnrichRequest.model_validate(body)
    except ValidationError as error:
        return respond({"error": "Invalid request payload", "details": error.errors()}, 400)

    if not payload.recipients:
        return respond({"results": [], "matched_count": 0, "requested_count": 0}, 200)

    domain = derive_domain(payload.organization_website)
    results: list[ApolloRecipientResult] = []
    async with httpx.AsyncClient(base_url="https://api.apollo.io", timeout=20.0) as client:
        if not domain and payload.organization_name:
            domain = await discover_apollo_domain(
                client=client,
                api_key=api_key,
                organization_name=payload.organization_name,
            )
        for recipient in payload.recipients:
            first_name, last_name = split_name(recipient.full_name)
            req: dict[str, Any] = {
                "first_name": first_name,
                "last_name": last_name,
            }
            if domain:
                req["domain"] = domain
            if payload.reveal_personal_emails:
                req["reveal_personal_emails"] = True

            try:
                upstream = await client.post(
                    "/api/v1/people/match",
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                        "X-Api-Key": api_key,
                        "Cache-Control": "no-cache",
                    },
                    params=req,
                )
                parsed = parse_json(upstream)
            except httpx.RequestError as error:
                results.append(
                    ApolloRecipientResult(
                        full_name=recipient.full_name,
                        title=recipient.title,
                        found=False,
                        detail=f"Apollo request error: {error}",
                    )
                )
                continue

            if not upstream.is_success:
                detail = None
                if isinstance(parsed.get("error"), Mapping):
                    detail = parsed["error"].get("message")
                results.append(
                    ApolloRecipientResult(
                        full_name=recipient.full_name,
                        title=recipient.title,
                        found=False,
                        detail=detail or f"Apollo HTTP {upstream.status_code}",
                    )
                )
                continue

            person = parsed.get("person")
            if not isinstance(person, Mapping):
                results.append(
                    ApolloRecipientResult(
                        full_name=recipient.full_name,
                        title=recipient.title,
                        found=False,
                        detail="No Apollo match found",
                    )
                )
                continue

            results.append(
                ApolloRecipientResult(
                    full_name=recipient.full_name,
                    title=recipient.title,
                    found=True,
                    email=clean_optional(str(person.get("email") or "")),
                    phone=extract_apollo_phone(person),
                    email_status=clean_optional(str(person.get("email_status") or "")),
                    linkedin_url=clean_optional(str(person.get("linkedin_url") or "")),
                    apollo_person_id=clean_optional(str(person.get("id") or "")),
                )
            )

    matched = sum(1 for r in results if r.found and (r.email or r.phone))
    return respond(
        {
            "results": [r.model_dump(mode="json") for r in results],
            "matched_count": matched,
            "requested_count": len(payload.recipients),
            "organization_domain": domain,
        },
        200,
    )


@app.get("/api/apollo/health")
async def apollo_health() -> JSONResponse:
    api_key = clean_env_secret_single_line(os.getenv("APOLLO_API_KEY"))
    has_api_key = bool(api_key)
    if not has_api_key:
        return respond(
            {
                "ok": True,
                "route": "apollo_enrich_recipients",
                "has_api_key": False,
                "upstream_ok": False,
                "message": "APOLLO_API_KEY is not configured on backend.",
            },
            200,
        )

    upstream_ok = False
    upstream_status = None
    upstream_error = None
    try:
        async with httpx.AsyncClient(base_url="https://api.apollo.io", timeout=12.0) as client:
            health = await client.get(
                "/v1/auth/health",
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "X-Api-Key": api_key,
                    "Cache-Control": "no-cache",
                },
            )
            upstream_status = health.status_code
            upstream_ok = health.is_success
            if not health.is_success:
                body = parse_json(health)
                if isinstance(body.get("error"), Mapping):
                    upstream_error = body["error"].get("message")
                elif body.get("error"):
                    upstream_error = str(body.get("error"))
                else:
                    upstream_error = f"Apollo HTTP {health.status_code}"
    except httpx.RequestError as error:
        upstream_error = str(error)

    return respond(
        {
            "ok": True,
            "route": "apollo_enrich_recipients",
            "has_api_key": True,
            "key_fingerprint": token_fingerprint(api_key),
            "upstream_ok": upstream_ok,
            "upstream_status": upstream_status,
            "message": None if upstream_ok else (upstream_error or "Apollo auth health check failed."),
        },
        200,
        )


@app.get("/api/apollo/sequences")
async def apollo_sequences(request: Request) -> JSONResponse:
    limit_raw = request.query_params.get("limit")
    query = clean_optional(request.query_params.get("q"))
    active_raw = (request.query_params.get("active") or "").strip().lower()
    active_only = active_raw in {"1", "true", "yes", "y", "on"}
    try:
        limit = max(1, min(100, int(limit_raw or "50")))
    except ValueError:
        limit = 50

    tool_args: dict[str, Any] = {"max_results": limit}
    if query:
        tool_args["query"] = query
    result, error = await call_cri_mcp_tool(
        "apollo_listSequences",
        tool_args,
    )
    if error:
        return respond({"error": error}, 502)
    sequences_raw = result.get("sequences") if isinstance(result, Mapping) else None
    sequences = sequences_raw if isinstance(sequences_raw, list) else []
    if active_only:
        filtered: list[Any] = []
        for row in sequences:
            if not isinstance(row, Mapping):
                continue
            status_value = clean_optional(str(row.get("status") or "")) or ""
            status_key = status_value.strip().lower()
            if status_key in {"active", "running", "enabled", "in_progress"}:
                filtered.append(row)
        sequences = filtered
    return respond({"sequences": sequences, "count": len(sequences)}, 200)


@app.post("/api/apollo/attach-email-step-assets")
async def apollo_attach_email_step_assets(request: Request) -> JSONResponse:
    request_id = str(uuid.uuid4())[:8]
    body = await read_json_body(request)
    try:
        payload = ApolloAttachEmailStepAssetsRequest.model_validate(body)
    except ValidationError as error:
        logger.warning("[apollo-attach:%s] invalid payload details=%s", request_id, error.errors())
        return respond({"error": "Invalid request payload", "details": error.errors()}, 400)

    selected_files: list[dict[str, Any]] = []
    for asset in payload.assets:
        item: dict[str, Any] = {}
        if clean_optional(asset.id):
            item["id"] = clean_optional(asset.id)
        if clean_optional(asset.url):
            item["web_url"] = clean_optional(asset.url)
        if clean_optional(asset.title):
            item["name"] = clean_optional(asset.title)
        if item:
            selected_files.append(item)

    if not selected_files:
        logger.warning("[apollo-attach:%s] no valid assets after normalization", request_id)
        return respond({"error": "No valid assets selected for attachment."}, 400)

    result, error = await call_cri_mcp_tool(
        "apollo_attachAssetsToEmailStep",
        {
            "sequence_id": payload.sequence_id,
            "email_step_number": payload.email_step_number,
            "organization_name": payload.organization_name,
            "max_total_bytes": payload.max_total_bytes,
            "max_zip_bytes": payload.max_zip_bytes,
            "selected_files": selected_files,
        },
    )
    if error:
        logger.error("[apollo-attach:%s] mcp tool error=%s", request_id, error)
        return respond({"error": error}, 502)
    if isinstance(result, Mapping) and result.get("success") is False:
        message = clean_optional(str(result.get("message") or "")) or "Apollo attach tool returned unsuccessful result."
        logger.warning(
            "[apollo-attach:%s] mcp returned unsuccessful result message=%s keys=%s",
            request_id,
            message,
            sorted(result.keys()),
        )
        return respond({"error": message, "result": result}, 400)
    logger.info("[apollo-attach:%s] success sequence_id=%s step=%s files=%s", request_id, payload.sequence_id, payload.email_step_number, len(selected_files))
    return respond({"success": True, "result": result}, 200)


@app.post("/api/apollo/account-snapshot")
async def apollo_account_snapshot(request: Request) -> JSONResponse:
    request_id = str(uuid.uuid4())[:8]
    logger.info("[apollo:%s] account snapshot request received", request_id)
    api_key = clean_env_secret_single_line(os.getenv("APOLLO_API_KEY"))
    if not api_key:
        return respond({"error": "Missing APOLLO_API_KEY environment variable"}, 500)
    logger.info("[apollo:%s] api key fingerprint=%s", request_id, token_fingerprint(api_key))

    body = await read_json_body(request)
    try:
        payload = ApolloAccountSnapshotRequest.model_validate(body)
    except ValidationError as error:
        return respond({"error": "Invalid request payload", "details": error.errors()}, 400)

    org_name = clean_optional(payload.organization_name)
    org_domain = derive_domain(payload.organization_website)
    if not org_name and not org_domain:
        return respond({"error": "organization_name or organization_website is required."}, 400)

    domain_input_provided = bool(org_domain)
    async with httpx.AsyncClient(base_url="https://api.apollo.io", timeout=20.0) as client:
        # Respect user-provided domain as authoritative. Only discover when missing.
        if not org_domain and org_name:
            org_domain = await discover_apollo_domain(
                client=client,
                api_key=api_key,
                organization_name=org_name,
            )

        search_params: dict[str, Any] = {"page": 1, "per_page": 10}
        if org_name:
            search_params["q_organization_name"] = org_name
        if org_domain:
            search_params["q_organization_domains_list[]"] = org_domain
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Api-Key": api_key,
            "Cache-Control": "no-cache",
        }
        try:
            search_resp = await client.post(
                "/api/v1/organizations/search",
                headers=headers,
                params=search_params,
            )
            search_payload = parse_json(search_resp)
        except httpx.RequestError as error:
            return respond({"error": f"Apollo request error: {error}"}, 502)

        if not search_resp.is_success:
            detail = search_payload.get("error")
            return respond(
                {
                    "error": "Apollo organization search failed.",
                    "details": detail if isinstance(detail, str) else search_payload,
                },
                search_resp.status_code,
            )

        organizations = search_payload.get("organizations")
        if not isinstance(organizations, list):
            organizations = search_payload.get("accounts")
        if not isinstance(organizations, list):
            organizations = []
        if not organizations and org_name:
            fallback_names = organization_name_variants(org_name)
            for candidate in fallback_names:
                if normalize_text(candidate) == normalize_text(org_name):
                    continue
                retry_params: dict[str, Any] = {
                    "page": 1,
                    "per_page": 10,
                    "q_organization_name": candidate,
                }
                if org_domain:
                    retry_params["q_organization_domains_list[]"] = org_domain
                try:
                    retry_resp = await client.post(
                        "/api/v1/organizations/search",
                        headers=headers,
                        params=retry_params,
                    )
                except httpx.RequestError:
                    continue
                if not retry_resp.is_success:
                    continue
                retry_payload = parse_json(retry_resp)
                retry_orgs = retry_payload.get("organizations")
                if not isinstance(retry_orgs, list):
                    retry_orgs = retry_payload.get("accounts")
                if isinstance(retry_orgs, list) and retry_orgs:
                    organizations = retry_orgs
                    break

        best, best_score, best_reasons = pick_best_apollo_org_match(
            organizations, org_name, org_domain
        )
        if not best or best_score < 6:
            return respond(
                {
                    "matched": False,
                    "message": "No high-confidence Apollo organization match found.",
                    "lookup": {
                        "query_name": org_name,
                        "query_domain": org_domain,
                        "lookup_confidence": "LOW",
                        "match_score": best_score,
                        "match_reason": best_reasons,
                    },
                },
                200,
            )

        org_id = clean_optional(str(best.get("id") or ""))
        detailed: Mapping[str, Any] | None = None
        detailed_status: int | None = None
        if org_id:
            try:
                auth_header_attempts = (
                    ("x-api-key-only", {"X-Api-Key": api_key}),
                    ("bearer-only", {"Authorization": f"Bearer {api_key}"}),
                    (
                        "x-api-key-and-bearer",
                        {
                            "X-Api-Key": api_key,
                            "Authorization": f"Bearer {api_key}",
                        },
                    ),
                )
                attempted_statuses: list[str] = []
                for mode, auth_headers in auth_header_attempts:
                    detailed_resp = await client.get(
                        f"/api/v1/organizations/{org_id}",
                        headers={
                            "Content-Type": "application/json",
                            "Accept": "application/json",
                            "Cache-Control": "no-cache",
                            **auth_headers,
                        },
                    )
                    detailed_status = detailed_resp.status_code
                    attempted_statuses.append(f"{mode}:{detailed_status}")
                    if detailed_resp.is_success:
                        payload_obj = parse_json(detailed_resp)
                        org_obj = payload_obj.get("organization")
                        if isinstance(org_obj, Mapping):
                            detailed = org_obj
                        break
                if not detailed:
                    logger.info(
                        "[apollo:%s] detailed org call non-success status=%s attempts=%s; using search payload (401=auth header/key issue, 403=key lacks required master scope)",
                        request_id,
                        detailed_status,
                        ",".join(attempted_statuses),
                    )
            except httpx.RequestError:
                detailed = None

        snapshot = build_apollo_snapshot(best, detailed)
        apollo_domain_verified = bool(clean_optional(str(snapshot.get("domain") or "")))
        if org_domain and not apollo_domain_verified:
            snapshot["domain"] = org_domain

        lookup_confidence = "HIGH" if best_score >= 10 else "MEDIUM"
        return respond(
            {
                "matched": True,
                "organization": snapshot,
                "lookup": {
                    "query_name": org_name,
                    "query_domain": org_domain,
                    "query_domain_source": "user_input"
                    if domain_input_provided
                    else ("discovered" if org_domain else "none"),
                    "lookup_confidence": lookup_confidence,
                    "match_score": best_score,
                    "match_reason": best_reasons,
                    "apollo_domain_verified": apollo_domain_verified,
                    "detailed_status": detailed_status,
                    "used_detailed_endpoint": bool(detailed),
                },
            },
            200,
        )


@app.post("/api/case-studies/recommend")
async def case_study_recommendations(request: Request) -> JSONResponse:
    body = await read_json_body(request)
    try:
        payload = CaseStudyRecommendRequest.model_validate(body)
    except ValidationError as error:
        return respond({"error": "Invalid request payload", "details": error.errors()}, 400)

    org_name = normalize_text(payload.organization_name)
    vertical = normalize_text(payload.industry_vertical)

    sharepoint_items, sharepoint_meta = await fetch_sharepoint_recommended_assets(payload)
    if sharepoint_items:
        return respond(
            {
                "items": sharepoint_items[: payload.max_items],
                "source": "sharepoint_mcp",
                "filters": {
                    "industry_vertical": vertical or None,
                    "organization_name": payload.organization_name,
                    "project_description": clean_optional(payload.project_description),
                    "max_items": payload.max_items,
                },
                "sharepoint": sharepoint_meta,
            },
            200,
        )

    library = parse_case_study_library()
    if not library:
        return respond(
            {
                "items": [],
                "message": (
                    "No SharePoint matches returned and no CASE_STUDY_LIBRARY_JSON fallback configured."
                ),
                "source": "none",
                "sharepoint": sharepoint_meta,
            },
            200,
        )

    scored: list[tuple[int, dict[str, Any]]] = []
    for item in library:
        score = 0
        industry = normalize_text(str(item.get("industry") or ""))
        tags = [normalize_text(str(tag)) for tag in item.get("tags") or []]
        title = normalize_text(str(item.get("title") or ""))
        if vertical and industry and vertical in industry:
            score += 5
        if vertical and any(vertical in tag for tag in tags):
            score += 3
        if org_name and any(token in title for token in split_query_tokens(org_name)):
            score += 1
        scored.append((score, item))

    scored.sort(key=lambda tup: tup[0], reverse=True)
    top = [item for _, item in scored[: payload.max_items]]
    return respond(
        {
            "items": top,
            "source": "env_case_study_library",
            "filters": {
                "industry_vertical": vertical or None,
                "organization_name": payload.organization_name,
                "project_description": clean_optional(payload.project_description),
                "max_items": payload.max_items,
            },
            "sharepoint": sharepoint_meta,
        },
        200,
    )


@app.post("/api/assets/prepare-package")
async def prepare_asset_package(request: Request) -> Response:
    body = await read_json_body(request)
    try:
        payload = AssetPreparePackageRequest.model_validate(body)
    except ValidationError as error:
        return respond({"error": "Invalid request payload", "details": error.errors()}, 400)

    base_url, bearer = resolve_sharepoint_mcp_connection()
    if not base_url:
        return respond(
            {"error": "Missing SharePoint MCP base URL. Set SHAREPOINT_MCP_BASE_URL or CRI_MCP_BASE_URL."},
            500,
        )
    if not bearer:
        return respond(
            {"error": "Missing SharePoint MCP bearer token. Set SHAREPOINT_MCP_BEARER or CRI_MCP_BEARER."},
            500,
        )

    package_assets = []
    for asset in payload.assets:
        package_assets.append(
            {
                "id": clean_optional(asset.id),
                "title": clean_optional(asset.title),
                "url": clean_optional(asset.url),
                "path": clean_optional(asset.path),
                "score": float(asset.score) if asset.score is not None else None,
            }
        )

    mcp_request_body = {
        "organization_name": clean_optional(payload.organization_name),
        "assets": package_assets,
        "output_format": "zip",
    }
    package_tool_name = clean_optional(os.getenv("SHAREPOINT_PREPARE_PACKAGE_TOOL")) or "sharepoint_prepareAttachmentPackage"
    headers = {
        "Authorization": f"Bearer {bearer}",
        "x-api-key": bearer,
    }

    try:
        async with httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=90.0) as client:
            rpc_payload = {
                "jsonrpc": "2.0",
                "id": f"asset-package-{uuid.uuid4().hex[:8]}",
                "method": "tools/call",
                "params": {
                    "name": package_tool_name,
                    "arguments": mcp_request_body,
                },
            }
            rpc_response, rpc_error = await fetch_external_json(
                client=client,
                method="POST",
                path="/mcp",
                headers=headers,
                json_body=rpc_payload,
            )
    except httpx.RequestError as error:
        return respond({"error": f"Failed to reach MCP server: {error}"}, 502)

    if rpc_error or not isinstance(rpc_response, Mapping):
        return respond(
            {
                "error": "Asset package tool call failed.",
                "details": [rpc_error or "Invalid MCP response"],
            },
            502,
        )

    extracted = extract_tool_payload_from_mcp_response(rpc_response)
    if not isinstance(extracted, Mapping):
        return respond({"error": "Invalid package payload returned from MCP."}, 502)

    zip_base64 = first_non_empty_string(
        extracted,
        [
            "zip_base64",
            "package_base64",
            "content_base64",
            "data_base64",
            "base64",
        ],
    )
    if not zip_base64:
        return respond(
            {
                "error": "MCP package response did not include zip content.",
                "details": [str(extracted)[:500]],
            },
            502,
        )

    try:
        zip_bytes = base64.b64decode(zip_base64, validate=False)
    except (ValueError, TypeError):
        return respond({"error": "Invalid base64 zip content returned from MCP."}, 502)
    if not zip_bytes:
        return respond({"error": "Decoded zip package was empty."}, 502)

    raw_filename = first_non_empty_string(
        extracted,
        ["filename", "file_name", "package_name", "zip_name"],
    )
    fallback_name = f"{slugify(clean_optional(payload.organization_name) or 'sales-coach-assets')}.zip"
    filename = ensure_zip_filename(raw_filename or fallback_name)
    return Response(
        content=zip_bytes,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/assets/thumbnail")
async def get_asset_thumbnail(request: Request) -> Response:
    source_url = clean_optional(request.query_params.get("url"))
    if not source_url:
        return respond({"error": "Missing required query parameter: url"}, 400)
    if "sharepoint.com" not in source_url.lower():
        return respond({"error": "Only SharePoint URLs are supported for thumbnail proxy."}, 400)

    candidates = build_sharepoint_preview_candidates(source_url)
    if not candidates:
        return respond({"error": "Unable to derive SharePoint preview URL."}, 400)

    try:
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
            for candidate in candidates:
                preview_resp = await client.get(
                    candidate,
                    headers={
                        "Accept": "image/*,*/*;q=0.8",
                        "User-Agent": "AI-Sales-Coach/1.0",
                    },
                )
                content_type = (preview_resp.headers.get("content-type") or "").lower()
                if preview_resp.is_success and content_type.startswith("image/") and preview_resp.content:
                    return Response(
                        content=preview_resp.content,
                        media_type=content_type.split(";")[0],
                        headers={"Cache-Control": "private, max-age=900"},
                    )
                logger.info(
                    "[thumbnail] preview fetch non-image status=%s content_type=%s candidate=%s",
                    preview_resp.status_code,
                    content_type or "unknown",
                    candidate,
                )
    except httpx.RequestError as error:
        logger.warning("[thumbnail] request failed: %s", error)
        return respond({"error": f"Thumbnail proxy request failed: {error}"}, 502)

    return respond({"error": "Preview unavailable for this asset URL."}, 404)

@app.get("/api/web/embed")
async def embed_web_page(request: Request) -> Response:
    source_url = clean_optional(request.query_params.get("url"))
    if not source_url:
        return respond({"error": "Missing required query parameter: url"}, 400)

    parsed = urlparse(source_url)
    if parsed.scheme not in {"http", "https"}:
        return respond({"error": "Only http/https URLs are supported."}, 400)
    if not is_public_hostname(parsed.hostname):
        return respond({"error": "Blocked host for embedded web proxy."}, 400)

    safe_url = source_url
    try:
        async with httpx.AsyncClient(timeout=25.0, follow_redirects=True) as client:
            upstream = await client.get(
                safe_url,
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "User-Agent": "AI-Sales-Coach-Embed/1.0",
                },
            )
    except httpx.RequestError as error:
        logger.warning("[embed] request failed for %s: %s", safe_url, error)
        fallback = (
            "<html><body style=\"font-family:Arial,sans-serif;padding:16px\">"
            "<h3>Unable to load site in workspace</h3>"
            f"<p>{str(error)}</p>"
            f"<p><a href=\"{safe_url}\" target=\"_blank\" rel=\"noreferrer\">Open in new tab</a></p>"
            "</body></html>"
        )
        return HTMLResponse(content=fallback, status_code=502)

    content_type = (upstream.headers.get("content-type") or "").lower()
    if "text/html" not in content_type:
        media_type = content_type.split(";")[0] if content_type else "application/octet-stream"
        headers = {
            "Cache-Control": "no-store",
            "X-Frame-Options": "",
            "Content-Security-Policy": "frame-ancestors 'self' *",
        }
        return Response(content=upstream.content, media_type=media_type, headers=headers)

    html = upstream.text
    final_url = str(upstream.url)

    # Remove meta CSP/frame-busting from upstream page to improve iframe render success.
    html = re.sub(
        r'<meta[^>]+http-equiv=["\']Content-Security-Policy["\'][^>]*>',
        "",
        html,
        flags=re.IGNORECASE,
    )

    if "<head" in html.lower():
        html = re.sub(
            r"(<head[^>]*>)",
            rf"\\1<base href=\"{final_url}\"><meta name=\"referrer\" content=\"no-referrer\">",
            html,
            count=1,
            flags=re.IGNORECASE,
        )
    else:
        html = f"<base href=\"{final_url}\">" + html

    headers = {
        "Cache-Control": "no-store",
        "X-Frame-Options": "",
        "Content-Security-Policy": "frame-ancestors 'self' *",
    }
    return HTMLResponse(content=html, status_code=upstream.status_code, headers=headers)


def respond(
    payload: Mapping[str, Any], status_code: int, cookie_value: str | None = None
) -> JSONResponse:
    response = JSONResponse(payload, status_code=status_code)
    if cookie_value:
        response.set_cookie(
            key=SESSION_COOKIE_NAME,
            value=cookie_value,
            max_age=SESSION_COOKIE_MAX_AGE_SECONDS,
            httponly=True,
            samesite="lax",
            secure=is_prod(),
            path="/",
        )
    return response


def is_prod() -> bool:
    env = (os.getenv("ENVIRONMENT") or os.getenv("NODE_ENV") or "").lower()
    return env == "production"


def include_debug_fields() -> bool:
    raw = (os.getenv("GRANT_INCLUDE_DEBUG_FIELDS") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


async def read_json_body(request: Request) -> Mapping[str, Any]:
    raw = await request.body()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, Mapping) else {}


def resolve_workflow_id(body: Mapping[str, Any]) -> str | None:
    workflow = body.get("workflow", {})
    workflow_id = None
    if isinstance(workflow, Mapping):
        workflow_id = workflow.get("id")
    workflow_id = workflow_id or body.get("workflowId")
    env_workflow = os.getenv("CHATKIT_WORKFLOW_ID") or os.getenv(
        "VITE_CHATKIT_WORKFLOW_ID"
    )
    if not workflow_id and env_workflow:
        workflow_id = env_workflow
    if workflow_id and isinstance(workflow_id, str) and workflow_id.strip():
        return workflow_id.strip()
    return None


def resolve_user(cookies: Mapping[str, str]) -> tuple[str, str | None]:
    existing = cookies.get(SESSION_COOKIE_NAME)
    if existing:
        return existing, None
    user_id = str(uuid.uuid4())
    return user_id, user_id


def chatkit_api_base() -> str:
    return (
        os.getenv("CHATKIT_API_BASE")
        or os.getenv("VITE_CHATKIT_API_BASE")
        or DEFAULT_CHATKIT_BASE
    )


def openai_api_base() -> str:
    return os.getenv("OPENAI_API_BASE") or DEFAULT_OPENAI_BASE


def parse_json(response: httpx.Response) -> Mapping[str, Any]:
    try:
        parsed = response.json()
        return parsed if isinstance(parsed, Mapping) else {}
    except (json.JSONDecodeError, httpx.DecodingError):
        return {}


def normalize_grant_payload(
    payload: GrantCampaignGenerateRequest,
) -> GrantCampaignGenerateRequest:
    payload.lead_id = clean_optional(payload.lead_id) or str(uuid.uuid4())
    payload.organization.name = clean_optional(payload.organization.name) or "UNKNOWN_ORGANIZATION"
    payload.organization.website = clean_optional(payload.organization.website)
    payload.organization.city = clean_optional(payload.organization.city)
    payload.organization.state = clean_optional(payload.organization.state)

    payload.award.award_id = clean_optional(payload.award.award_id) or "UNKNOWN_AWARD_ID"
    payload.award.generated_internal_id = clean_optional(payload.award.generated_internal_id)
    payload.award.agency = clean_optional(payload.award.agency)
    payload.award.award_date = clean_optional(payload.award.award_date)
    payload.award.period_start = clean_optional(payload.award.period_start)
    payload.award.period_end = clean_optional(payload.award.period_end)
    payload.award.cfda_number = clean_optional(payload.award.cfda_number)
    payload.award.cfda_title = clean_optional(payload.award.cfda_title)
    payload.award.description = clean_optional(payload.award.description)
    payload.award.place_of_performance = clean_optional(payload.award.place_of_performance)
    if payload.award.amount is None:
        payload.award.amount = Decimal("0")

    cleaned_evidence: list[EvidenceItem] = []
    for item in payload.evidence:
        source = item.source
        label = clean_optional(item.label) or "N/A"
        url = clean_optional(item.url)
        excerpt = clean_optional(item.excerpt)
        if not url:
            url = "N/A"
        cleaned_evidence.append(
            EvidenceItem(
                label=label,
                url=url,
                source=source,
                excerpt=excerpt,
            )
        )
    payload.evidence = cleaned_evidence
    cleaned_prospects: list[ProvidedProspect] = []
    for prospect in payload.prospects:
        full_name = clean_optional(prospect.full_name)
        title = clean_optional(prospect.title)
        organization = clean_optional(prospect.organization) or payload.organization.name
        linkedin_url = clean_optional(prospect.linkedin_url)
        note = clean_optional(prospect.note)
        if not full_name and not title:
            continue
        cleaned_prospects.append(
            ProvidedProspect(
                full_name=full_name,
                title=title,
                organization=organization,
                linkedin_url=linkedin_url,
                note=note,
            )
        )
    payload.prospects = cleaned_prospects
    return payload


def clean_optional(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def first_non_empty_string(payload: Mapping[str, Any], keys: list[str]) -> str | None:
    for key in keys:
        value = clean_optional(str(payload.get(key) or ""))
        if value:
            return value
    return None


def slugify(value: str) -> str:
    base = normalize_text(value) or "assets"
    base = re.sub(r"[^a-z0-9]+", "-", base)
    base = base.strip("-")
    return base or "assets"


def ensure_zip_filename(value: str) -> str:
    cleaned = clean_optional(value) or "assets.zip"
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", cleaned)
    if not safe.lower().endswith(".zip"):
        safe = f"{safe}.zip"
    return safe


def clean_env_secret_single_line(value: str | None) -> str | None:
    """Use only the first line of a secret. Railway / copy-paste often merges two
    .env lines into one variable (e.g. access token + newline + HUBSPOT_MCP_REFRESH_TOKEN=...),
    which breaks HTTP headers (Illegal header value)."""
    if value is None:
        return None
    text = value.strip().replace("\r\n", "\n").replace("\r", "\n")
    if not text:
        return None
    first = text.split("\n", 1)[0].strip()
    if not first:
        return None
    if first.lower().startswith("bearer "):
        first = first[7:].strip()
    return first if first else None


def local_env_value(key: str) -> str | None:
    """Dev-only fallback for local runs when process env does not carry expected
    keys. Reads managed-chatkit/.env.local directly."""
    if is_prod():
        return None
    backend_dir = os.path.dirname(os.path.dirname(__file__))  # managed-chatkit/backend
    env_path = os.path.join(backend_dir, "..", ".env.local")
    if not os.path.exists(env_path):
        return None
    try:
        with open(env_path, "r", encoding="utf-8") as handle:
            for raw in handle:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                if k.strip() != key:
                    continue
                return clean_env_secret_single_line(v.strip().strip("'\""))
    except OSError:
        return None
    return None


def token_fingerprint(token: str | None) -> str:
    t = clean_optional(token)
    if not t:
        return "none"
    suffix = t[-8:] if len(t) >= 8 else t
    return f"len:{len(t)}:...{suffix}"



def load_hubspot_owner_overrides_from_env() -> dict[str, dict[str, str]]:
    raw = clean_optional(os.getenv("HUBSPOT_OWNER_ID_MAP_JSON"))
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        logger.warning("[hubspot] invalid HUBSPOT_OWNER_ID_MAP_JSON; expected JSON object")
        return {}
    if not isinstance(parsed, Mapping):
        logger.warning("[hubspot] HUBSPOT_OWNER_ID_MAP_JSON ignored; top-level value must be an object")
        return {}

    out: dict[str, dict[str, str]] = {}
    for key, value in parsed.items():
        owner_id = clean_optional(str(key))
        if not owner_id:
            continue
        if isinstance(value, str):
            name = clean_optional(value) or ""
            out[owner_id] = {"name": name, "email": ""}
            continue
        if not isinstance(value, Mapping):
            continue
        name = clean_optional(str(value.get("name") or "")) or ""
        email = clean_optional(str(value.get("email") or "")) or ""
        if not name and not email:
            continue
        out[owner_id] = {"name": name, "email": email}
    return out
def get_cached_domain_for_org(org_name: str | None) -> str | None:
    key = normalize_text(org_name)
    if not key:
        return None
    cached = DOMAIN_DISCOVERY_CACHE.get(key)
    if not cached:
        return None
    domain, expires_at = cached
    if time.time() > expires_at:
        DOMAIN_DISCOVERY_CACHE.pop(key, None)
        return None
    return domain


def set_cached_domain_for_org(org_name: str | None, domain: str | None) -> None:
    key = normalize_text(org_name)
    host = derive_domain(domain)
    if not key or not host:
        return
    DOMAIN_DISCOVERY_CACHE[key] = (host, time.time() + DOMAIN_DISCOVERY_CACHE_TTL_SECONDS)


def quota_bucket_id(provider: str) -> str:
    now = datetime.now(timezone.utc)
    if provider == "google":
        return now.strftime("%Y-%m-%d")
    if provider == "brave":
        return now.strftime("%Y-%m")
    return ""


def quota_limit(provider: str) -> int:
    if provider == "google":
        return GOOGLE_SEARCH_DAILY_LIMIT
    if provider == "brave":
        return BRAVE_SEARCH_MONTHLY_LIMIT
    return 0


def quota_can_use(provider: str) -> bool:
    state = DOMAIN_SEARCH_QUOTA.get(provider)
    if not state:
        return False
    bucket = quota_bucket_id(provider)
    if state["bucket"] != bucket:
        state["bucket"] = bucket
        state["count"] = 0
    return int(state["count"]) < quota_limit(provider)


def quota_register_use(provider: str) -> None:
    state = DOMAIN_SEARCH_QUOTA.get(provider)
    if not state:
        return
    bucket = quota_bucket_id(provider)
    if state["bucket"] != bucket:
        state["bucket"] = bucket
        state["count"] = 0
    state["count"] = int(state["count"]) + 1


def derive_domain(website: str | None) -> str | None:
    raw = clean_optional(website)
    if not raw:
        return None
    normalized = raw if "://" in raw else f"https://{raw}"
    try:
        parsed = urlparse(normalized)
    except ValueError:
        return None
    host = (parsed.netloc or "").lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host or None


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip().lower()


def normalize_entity_name(value: str | None) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    # If a trailing label is added for deal/context naming, strip it.
    parts = re.split(r"\s[-|:]\s", text, maxsplit=1)
    if len(parts) == 2 and any(
        token in parts[1]
        for token in [
            "implementation",
            "servicenow",
            "project",
            "grant-funded",
            "consulting",
            "outreach",
            "phase",
            "rollout",
        ]
    ):
        text = parts[0]
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(
        r"\b(inc|incorporated|llc|l l c|corp|corporation|co|company|ltd|plc|lp|llp|group|holdings)\b",
        " ",
        text,
    )
    return compact_spaces(text)


def text_contains(haystack: str | None, needle: str | None) -> bool:
    h = normalize_text(haystack)
    n = normalize_text(needle)
    if not h or not n:
        return False
    return n in h


def split_query_tokens(query: str | None) -> list[str]:
    q = normalize_text(query)
    if not q:
        return []
    return [token for token in re.split(r"[^a-z0-9]+", q) if len(token) >= 3]


def token_set(value: str | None) -> set[str]:
    return set(split_query_tokens(value))


def jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    intersection = len(left & right)
    union = len(left | right)
    if union == 0:
        return 0.0
    return intersection / union


def organization_aliases(org_name: str | None) -> set[str]:
    name = clean_optional(org_name)
    if not name:
        return set()
    aliases: set[str] = {normalize_text(name), normalize_entity_name(name)}
    paren = re.findall(r"\(([^)]+)\)", name)
    for item in paren:
        text = normalize_text(item)
        if text:
            aliases.add(text)
    words = [word for word in re.split(r"[^A-Za-z0-9]+", name) if word]
    if words:
        acronym = "".join(word[0] for word in words if word and word[0].isalnum())
        acronym_norm = normalize_text(acronym)
        if len(acronym_norm) >= 2:
            aliases.add(acronym_norm)
    return aliases


def organization_core_tokens(name: str | None) -> set[str]:
    stop = {
        "inc",
        "company",
        "corp",
        "corporation",
        "group",
        "services",
        "solutions",
        "technology",
        "technologies",
        "international",
        "partners",
        "partner",
    }
    raw = normalize_entity_name(name)
    tokens = {token for token in split_query_tokens(raw) if token not in stop}
    return tokens if tokens else token_set(raw)


def names_likely_same(left: str | None, right: str | None) -> bool:
    l = normalize_entity_name(left)
    r = normalize_entity_name(right)
    if not l or not r:
        return False
    if l == r:
        return True
    if l in r or r in l:
        return True
    left_tokens = organization_core_tokens(l)
    right_tokens = organization_core_tokens(r)
    if not left_tokens or not right_tokens:
        return False
    overlap = left_tokens.intersection(right_tokens)
    return len(overlap) >= 1


def domain_name_alignment_score(name: str | None, domain: str | None) -> float:
    host = derive_domain(domain)
    if not host:
        return 0.0
    root = re.sub(r"[^a-z0-9]", "", host.split(".", 1)[0].lower())
    if not root:
        return 0.0
    variants = [root]
    for prefix in ("go", "my", "app", "the", "portal"):
        if root.startswith(prefix) and len(root) - len(prefix) >= 4:
            variants.append(root[len(prefix) :])
    tokens = organization_core_tokens(name)
    if not tokens:
        return 0.0
    best = 0.0
    for variant in variants:
        token_hits = sum(1 for token in tokens if len(token) >= 4 and token in variant)
        if token_hits >= 2:
            best = max(best, 0.22)
        elif token_hits == 1:
            best = max(best, 0.14)
    return best


def compute_company_match_score(
    *,
    org_name: str | None,
    org_domain: str | None,
    org_industry: str | None,
    org_city: str | None,
    org_state: str | None,
    anchor_domains: set[str] | None,
    anchor_names: set[str] | None,
    company: Mapping[str, Any],
) -> tuple[float, dict[str, Any]]:
    props = mapping_value(company, "properties") or {}
    company_name = clean_optional(str(props.get("name") or ""))
    company_domain = derive_domain(str(props.get("domain") or ""))
    record_type = clean_optional(str(company.get("__entity_type") or "company")) or "company"
    company_city = normalize_text(clean_optional(str(props.get("city") or "")))
    company_state = normalize_text(clean_optional(str(props.get("state") or "")))
    company_industry = normalize_text(clean_optional(str(props.get("industry") or "")))
    org_industry_norm = normalize_text(org_industry)
    org_city_norm = normalize_text(org_city)
    org_state_norm = normalize_text(org_state)
    org_name_norm = normalize_entity_name(org_name)
    aliases = organization_aliases(org_name)

    score = 0.0
    reasons: list[str] = []

    if org_domain and company_domain and company_domain == org_domain:
        score += 0.45
        reasons.append("domain_exact")
    elif org_domain and company_domain:
        alignment = domain_name_alignment_score(org_name, company_domain)
        if alignment > 0:
            score += alignment
            reasons.append(f"domain_name_align:{alignment:.2f}")
    if company_domain and anchor_domains and company_domain in anchor_domains:
        score += 0.18
        reasons.append("search_domain_anchor")

    org_tokens = token_set(org_name_norm)
    company_name_norm = normalize_entity_name(company_name)
    company_tokens = token_set(company_name_norm)
    name_similarity = jaccard_similarity(org_tokens, company_tokens)
    if name_similarity > 0:
        score += min(0.35, name_similarity * 0.5)
        reasons.append(f"name_sim:{name_similarity:.2f}")
    if names_likely_same(company_name_norm, org_name_norm):
        score += 0.2
        reasons.append("name_normalized_match")
    if anchor_names:
        for anchor_name in anchor_names:
            if names_likely_same(company_name_norm, anchor_name):
                score += 0.12
                reasons.append("search_name_anchor")
                break

    if aliases and any(alias and alias in company_name_norm for alias in aliases):
        score += 0.05
        reasons.append("alias_match")

    industry_similarity = jaccard_similarity(token_set(org_industry_norm), token_set(company_industry))
    if industry_similarity > 0:
        score += min(0.2, industry_similarity * 0.4)
        reasons.append(f"industry_sim:{industry_similarity:.2f}")

    if org_city_norm and company_city and org_city_norm == company_city:
        score += 0.06
        reasons.append("city_match")
    if org_state_norm and company_state and org_state_norm == company_state:
        score += 0.06
        reasons.append("state_match")

    score = max(0.0, min(1.0, score))
    owner_id = clean_optional(
        str(props.get("hubspot_owner_id") or props.get("hs_owner_id") or props.get("owner_id") or "")
    )
    owner_name = clean_optional(str(props.get("hubspot_owner_name") or ""))
    owner_email = clean_optional(str(props.get("hubspot_owner_email") or ""))
    owner_display = owner_name or ""

    return score, {
        "id": clean_optional(str(company.get("id") or "")),
        "record_type": record_type,
        "name": company_name,
        "domain": company_domain,
        "industry": clean_optional(str(props.get("industry") or "")),
        "city": clean_optional(str(props.get("city") or "")),
        "state": clean_optional(str(props.get("state") or "")),
        "country": clean_optional(str(props.get("country") or "")),
        "owner": owner_display,
        "owner_id": owner_id,
        "owner_name": owner_name,
        "owner_email": owner_email,
        "tier": clean_optional(
            str(props.get("account_tier") or props.get("tier") or props.get("segment") or "")
        ),
        "territory": clean_optional(
            str(
                props.get("territory")
                or props.get("sales_territory")
                or props.get("region")
                or props.get("state")
                or ""
            )
        ),
        "url": clean_optional(str(company.get("url") or "")),
        "score": round(score, 4),
        "reasons": reasons,
    }


def extract_hubspot_search_anchors(search_data: Mapping[str, Any] | None) -> dict[str, set[str]]:
    domains: set[str] = set()
    names: set[str] = set()
    if not isinstance(search_data, Mapping):
        return {"domains": domains, "names": names}

    name_keys = {
        "name",
        "company",
        "company_name",
        "associated_company",
        "associatedcompany",
        "organization",
        "account",
    }
    domain_keys = {"domain", "website", "company_domain", "url"}
    email_keys = {"email"}

    def walk(node: Any) -> None:
        if isinstance(node, Mapping):
            for key, value in node.items():
                key_norm = normalize_text(str(key))
                if key_norm in name_keys and isinstance(value, str):
                    nm = normalize_entity_name(value)
                    if nm:
                        names.add(nm)
                elif key_norm in domain_keys and isinstance(value, str):
                    dm = derive_domain(value)
                    if dm:
                        domains.add(dm)
                elif key_norm in email_keys and isinstance(value, str):
                    email = clean_optional(value)
                    if email and "@" in email:
                        dm = derive_domain(email.split("@", 1)[1])
                        if dm:
                            domains.add(dm)
                walk(value)
            return
        if isinstance(node, list):
            for item in node:
                walk(item)
            return
        if isinstance(node, str):
            dm = derive_domain(node)
            if dm:
                domains.add(dm)

    walk(search_data)
    return {"domains": domains, "names": names}


def build_company_candidate(
    *,
    candidate_id: str | None,
    name: str | None,
    domain: str | None,
    entity_type: str | None = None,
    industry: str | None = None,
    city: str | None = None,
    state: str | None = None,
    country: str | None = None,
    owner: str | None = None,
    url: str | None = None,
) -> dict[str, Any] | None:
    clean_name = clean_optional(name)
    clean_domain = derive_domain(domain)
    if not clean_name and not clean_domain:
        return None
    resolved_id = clean_optional(candidate_id)
    if not resolved_id:
        key = clean_domain or normalize_text(clean_name)
        resolved_id = f"search::{key or 'candidate'}"
    return {
        "id": resolved_id,
        "__entity_type": clean_optional(entity_type) or "company",
        "url": clean_optional(url),
        "properties": {
            "name": clean_name or "",
            "domain": clean_domain or "",
            "industry": clean_optional(industry) or "",
            "city": clean_optional(city) or "",
            "state": clean_optional(state) or "",
            "country": clean_optional(country) or "",
            "hubspot_owner_id": clean_optional(owner) or "",
        },
    }


def candidate_dedupe_key(company: Mapping[str, Any]) -> str:
    props = mapping_value(company, "properties") or {}
    company_id = clean_optional(str(company.get("id") or ""))
    company_domain = derive_domain(str(props.get("domain") or ""))
    company_name = normalize_entity_name(str(props.get("name") or ""))
    if company_id:
        return f"id:{company_id}"
    if company_domain:
        return f"domain:{company_domain}"
    return f"name:{company_name}"


def is_synthetic_company_id(value: str | None) -> bool:
    text = clean_optional(value)
    if not text:
        return True
    return text.startswith("search::") or text.startswith("search-discovery::")


def infer_search_entity_type(node: Mapping[str, Any], props: Mapping[str, Any]) -> str:
    tokens = " ".join(
        [
            normalize_text(str(node.get("type") or "")),
            normalize_text(str(node.get("object_type") or "")),
            normalize_text(str(node.get("objectType") or "")),
            normalize_text(str(node.get("entity_type") or "")),
            normalize_text(str(node.get("entityType") or "")),
            normalize_text(str(props.get("type") or "")),
            normalize_text(str(props.get("object_type") or "")),
            normalize_text(str(props.get("entity_type") or "")),
        ]
    )
    if "contact" in tokens or "person" in tokens:
        return "contact"
    if "deal" in tokens or "opportunit" in tokens:
        return "deal"
    if "company" in tokens or "organization" in tokens or "account" in tokens:
        return "company"
    return "unknown"


def extract_company_candidates_from_hubspot_search(
    search_data: Mapping[str, Any] | None,
    *,
    org_name: str | None,
    org_domain: str | None,
) -> list[dict[str, Any]]:
    if not isinstance(search_data, Mapping):
        return []

    name_keys = ("name", "company", "company_name", "associated_company", "organization", "account")
    domain_keys = ("domain", "company_domain", "website", "url")
    id_keys = ("id", "company_id", "hs_object_id", "object_id", "record_id")
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    def append_candidate(candidate: dict[str, Any] | None) -> None:
        if not candidate:
            return
        props = mapping_value(candidate, "properties") or {}
        entity_type = clean_optional(str(candidate.get("__entity_type") or "")) or "unknown"
        if entity_type not in {"company", "organization", "account", "unknown"}:
            return
        name = clean_optional(str(props.get("name") or ""))
        domain = derive_domain(str(props.get("domain") or ""))
        candidate_id = clean_optional(str(candidate.get("id") or ""))
        # Enterprise guardrail: never promote domain-only ghost candidates as concrete account records.
        if not name or not domain:
            return
        if not candidate_id and entity_type != "company":
            return
        relevant = False
        if org_domain and domain and domain == org_domain:
            relevant = True
        if not relevant and org_name and name and names_likely_same(name, org_name):
            relevant = True
        if not relevant and org_name and domain and domain_name_alignment_score(org_name, domain) >= 0.14:
            relevant = True
        if not relevant:
            return
        key = candidate_dedupe_key(candidate)
        if key in seen:
            return
        seen.add(key)
        out.append(candidate)

    def walk(node: Any) -> None:
        if isinstance(node, Mapping):
            props = mapping_value(node, "properties") or {}
            entity_type = infer_search_entity_type(node, props)
            node_name = first_non_empty(node, list(name_keys)) or first_non_empty(props, list(name_keys))
            node_domain = first_non_empty(node, list(domain_keys)) or first_non_empty(props, list(domain_keys))
            node_id = first_non_empty(node, list(id_keys)) or first_non_empty(props, list(id_keys))
            node_industry = first_non_empty(node, ["industry"]) or first_non_empty(props, ["industry"])
            node_city = first_non_empty(node, ["city"]) or first_non_empty(props, ["city"])
            node_state = first_non_empty(node, ["state"]) or first_non_empty(props, ["state"])
            node_country = first_non_empty(node, ["country"]) or first_non_empty(props, ["country"])
            node_owner = first_non_empty(
                node, ["hubspot_owner_id", "hs_owner_id", "owner_id"]
            ) or first_non_empty(props, ["hubspot_owner_id", "hs_owner_id", "owner_id"])
            node_url = first_non_empty(node, ["url"]) or first_non_empty(props, ["url"])
            append_candidate(
                build_company_candidate(
                    candidate_id=str(node_id or ""),
                    name=str(node_name or ""),
                    domain=str(node_domain or ""),
                    entity_type=entity_type,
                    industry=str(node_industry or ""),
                    city=str(node_city or ""),
                    state=str(node_state or ""),
                    country=str(node_country or ""),
                    owner=str(node_owner or ""),
                    url=str(node_url or ""),
                )
            )

            for value in node.values():
                walk(value)
            return

        if isinstance(node, list):
            for item in node:
                walk(item)

    walk(search_data)
    return out


def merge_company_candidates(
    primary: list[Mapping[str, Any]], extras: list[Mapping[str, Any]]
) -> list[Mapping[str, Any]]:
    merged: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for source in [primary, extras]:
        for company in source:
            if not isinstance(company, Mapping):
                continue
            key = candidate_dedupe_key(company)
            if key in seen:
                continue
            seen.add(key)
            merged.append(company)
    return merged


async def ai_rank_company_candidates(
    *,
    org_name: str | None,
    org_domain: str | None,
    org_industry: str | None,
    org_city: str | None,
    org_state: str | None,
    candidates: list[dict[str, Any]],
) -> dict[str, Any] | None:
    api_key = clean_optional(os.getenv("OPENAI_API_KEY"))
    if not api_key or not candidates or not org_name:
        return None
    try:
        request_body = {
            "model": os.getenv("HUBSPOT_MATCH_MODEL", "gpt-4.1-mini"),
            "temperature": 0.0,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a strict CRM entity matcher. Pick the best company candidate for the target org. "
                        "Return JSON with: best_candidate_id (string|null), confidence (0..1), reason (string). "
                        "Only choose a candidate if there is strong evidence."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "target": {
                            "organization_name": org_name,
                            "organization_domain": org_domain,
                            "organization_industry": org_industry,
                            "organization_city": org_city,
                            "organization_state": org_state,
                            },
                            "candidates": candidates,
                        }
                    ),
                },
            ],
        }
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        async with httpx.AsyncClient(base_url=openai_api_base(), timeout=20.0) as client:
            upstream = await client.post("/v1/chat/completions", headers=headers, json=request_body)
        payload = parse_json(upstream)
        if not upstream.is_success or not isinstance(payload, Mapping):
            return None
        choices = payload.get("choices")
        if not isinstance(choices, list) or not choices:
            return None
        message = choices[0].get("message") if isinstance(choices[0], Mapping) else None
        content = (
            message.get("content")
            if isinstance(message, Mapping) and isinstance(message.get("content"), str)
            else None
        )
        if not content:
            return None
        parsed = json.loads(content)
        if not isinstance(parsed, Mapping):
            return None
        candidate_id = clean_optional(str(parsed.get("best_candidate_id") or ""))
        confidence_raw = parsed.get("confidence")
        confidence = None
        if isinstance(confidence_raw, (int, float)):
            confidence = max(0.0, min(1.0, float(confidence_raw)))
        reason = clean_optional(str(parsed.get("reason") or "")) or "AI match"
        return {"candidate_id": candidate_id, "confidence": confidence, "reason": reason}
    except Exception:
        return None


async def resolve_hubspot_account_match(
    *,
    org_name: str | None,
    org_domain: str | None,
    org_industry: str | None,
    org_city: str | None,
    org_state: str | None,
    search_data: Mapping[str, Any] | None,
    companies: list[Mapping[str, Any]],
) -> dict[str, Any]:
    search_anchors = extract_hubspot_search_anchors(search_data)
    anchor_domains = search_anchors.get("domains") or set()
    anchor_names = search_anchors.get("names") or set()
    search_domain_hit = bool(org_domain and org_domain in anchor_domains)
    search_name_hit = bool(
        org_name
        and any(names_likely_same(org_name, candidate_name) for candidate_name in anchor_names)
    )

    scored: list[dict[str, Any]] = []
    for company in companies:
        score, details = compute_company_match_score(
            org_name=org_name,
            org_domain=org_domain,
            org_industry=org_industry,
            org_city=org_city,
            org_state=org_state,
            anchor_domains=anchor_domains,
            anchor_names=anchor_names,
            company=company,
        )
        if score <= 0:
            continue
        scored.append(details)
    scored.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    top_candidates = scored[:15]
    deterministic = top_candidates[0] if top_candidates else None
    deterministic_score = float(deterministic.get("score") or 0.0) if deterministic else 0.0

    selected = deterministic
    selected_confidence = deterministic_score
    selected_reason = "deterministic score"
    selected_method = "deterministic"
    ai_result = None
    if deterministic_score < 0.85 and top_candidates:
        ai_result = await ai_rank_company_candidates(
            org_name=org_name,
            org_domain=org_domain,
            org_industry=org_industry,
            org_city=org_city,
            org_state=org_state,
            candidates=top_candidates,
        )
        if ai_result and ai_result.get("candidate_id"):
            candidate_id = ai_result.get("candidate_id")
            ai_match = next(
                (item for item in top_candidates if str(item.get("id") or "") == str(candidate_id)),
                None,
            )
            ai_conf = ai_result.get("confidence")
            if ai_match and isinstance(ai_conf, (int, float)):
                selected = ai_match
                selected_confidence = float(ai_conf)
                selected_reason = str(ai_result.get("reason") or "AI candidate ranking")
                selected_method = "ai_assisted"

    selected_id = clean_optional(str(selected.get("id") or "")) if selected else None
    selected_name = clean_optional(str(selected.get("name") or "")) if selected else None
    selected_record_type = clean_optional(str(selected.get("record_type") or "")) if selected else None
    has_real_candidate = bool(
        selected_id
        and not is_synthetic_company_id(selected_id)
        and (selected_record_type or "company") == "company"
        and selected_name
    )

    matched = bool(selected and selected_confidence >= 0.5)
    confident_match = bool(selected and selected_confidence >= 0.85 and has_real_candidate)
    if not matched:
        strong_search_evidence = search_domain_hit or (search_name_hit and bool(anchor_domains))
        if strong_search_evidence:
            matched = True
            confident_match = False
            selected_confidence = max(selected_confidence, 0.55 if search_domain_hit else 0.52)
            selected_method = "search_discovery"
            evidence_terms: list[str] = []
            if search_domain_hit:
                evidence_terms.append("search_domain_hit")
            if search_name_hit:
                evidence_terms.append("search_name_hit")
            selected_reason = (
                "search evidence supports likely account match"
                + (f" ({', '.join(evidence_terms)})" if evidence_terms else "")
            )
            if not selected:
                selected = {
                    "id": f"search-discovery::{org_domain or normalize_text(org_name) or 'candidate'}",
                    "record_type": "synthetic",
                    "name": clean_optional(org_name) or "",
                    "domain": org_domain or "",
                    "industry": clean_optional(org_industry) or "",
                    "city": clean_optional(org_city) or "",
                    "state": clean_optional(org_state) or "",
                    "country": "",
                    "owner": "",
                    "tier": "",
                    "territory": "",
                    "url": "",
                    "score": round(selected_confidence, 4),
                    "reasons": ["search_discovery_fallback"],
                }
                selected_reason += " | fallback company candidate synthesized from search evidence"
    confirmable = bool(has_real_candidate)
    return {
        "matched": matched,
        "confident_match": confident_match,
        "confirmable": confirmable,
        "confidence": round(selected_confidence, 4),
        "method": selected_method,
        "reason": selected_reason,
        "selected_company": selected,
        "top_candidates": top_candidates[:5],
        "thresholds": {"match": 0.5, "confident": 0.85},
        "ai_used": bool(ai_result),
        "search_evidence": {
            "domain_hit": search_domain_hit,
            "name_hit": search_name_hit,
            "anchor_domains_count": len(anchor_domains),
            "anchor_names_count": len(anchor_names),
        },
    }


def parse_boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = normalize_text(str(value or ""))
    return text in {"true", "1", "yes", "y"}


def deal_is_open(props: Mapping[str, Any]) -> bool:
    if "hs_is_closed" in props:
        return not parse_boolish(props.get("hs_is_closed"))
    stage = normalize_text(str(props.get("dealstage") or ""))
    if "closedwon" in stage or "closed_won" in stage:
        return False
    if "closedlost" in stage or "closed_lost" in stage:
        return False
    return True


def deal_is_closed_lost(props: Mapping[str, Any]) -> bool:
    if "hs_is_closed" in props and parse_boolish(props.get("hs_is_closed")):
        if "hs_is_closed_won" in props:
            return not parse_boolish(props.get("hs_is_closed_won"))
    stage = normalize_text(str(props.get("dealstage") or ""))
    return "closedlost" in stage or "closed_lost" in stage


def deal_matches_selected_company(
    deal: Mapping[str, Any], selected_company: Mapping[str, Any] | None
) -> bool:
    if not isinstance(selected_company, Mapping):
        return True
    selected_id = clean_optional(str(selected_company.get("id") or ""))
    selected_name = normalize_entity_name(str(selected_company.get("name") or ""))
    selected_domain = derive_domain(str(selected_company.get("domain") or ""))
    props = mapping_value(deal, "properties") or {}

    if selected_id:
        for key in (
            "associatedcompanyid",
            "associated_company_id",
            "hs_company_id",
            "company_id",
            "hubspot_company_id",
        ):
            value = clean_optional(str(props.get(key) or ""))
            if value and value == selected_id:
                return True

    company_text = normalize_text(
        " ".join(
            [
                str(props.get("company") or ""),
                str(props.get("associatedcompanyname") or ""),
                str(props.get("associated_company_name") or ""),
                str(props.get("account_name") or ""),
                str(props.get("dealname") or ""),
            ]
        )
    )
    if selected_name and selected_name in company_text:
        return True

    if selected_domain:
        root = normalize_text(selected_domain.split(".", 1)[0])
        if root and root in company_text:
            return True
    return False


def build_recommended_action(
    *,
    account_match: Mapping[str, Any] | None,
    deals: list[Mapping[str, Any]],
    project_signal_text: str | None,
) -> dict[str, str]:
    selected = mapping_value(account_match, "selected_company")
    matched = bool(account_match and account_match.get("matched"))
    confirmable = bool(account_match and account_match.get("confirmable"))
    owner_name = clean_optional(str(selected.get("owner_name") or "")) if selected else None
    owner_id = clean_optional(str(selected.get("owner_id") or "")) if selected else None
    owner_present = bool(owner_name or owner_id)

    if not matched or not confirmable:
        return {
            "code": "needs_human_review",
            "label": "Needs Human Review",
            "rationale": "CRM signals are not confirmable enough for automated outreach guidance.",
            "next_step": "Review matched account and confirm the correct company record before outreach.",
            "do_not_do": "Do not send sequence emails until account confirmation is complete.",
        }

    matched_deals: list[Mapping[str, Any]] = [
        deal for deal in deals if deal_matches_selected_company(deal, selected)
    ]
    open_deals = [
        deal
        for deal in matched_deals
        if deal_is_open(mapping_value(deal, "properties") or {})
    ]

    if open_deals:
        if owner_present:
            return {
                "code": "coordinate_with_owner_first",
                "label": "Coordinate With Owner First",
                "rationale": "There is active opportunity context on this account and an internal owner is assigned.",
                "next_step": "Align with the account owner on contact strategy before customer outreach.",
                "do_not_do": "Do not run an independent outbound sequence without owner alignment.",
            }
        return {
            "code": "outreach_now_warm",
            "label": "Outreach Now (Warm)",
            "rationale": "Account is active and no owner conflict signal is available.",
            "next_step": "Send a warm outreach referencing active implementation context and propose a scope review.",
            "do_not_do": "Do not use cold-intro messaging that ignores existing account activity.",
        }

    # No open deals: inspect most recent matched deal state.
    def deal_ts(deal: Mapping[str, Any]) -> float:
        props = mapping_value(deal, "properties") or {}
        values = [
            clean_optional(str(props.get("hs_lastmodifieddate") or "")),
            clean_optional(str(props.get("closedate") or "")),
            clean_optional(str(deal.get("updatedAt") or "")),
            clean_optional(str(deal.get("createdAt") or "")),
        ]
        for value in values:
            dt = iso_to_dt(value)
            if dt:
                return dt.timestamp()
        return 0.0

    latest_deal = sorted(matched_deals, key=deal_ts, reverse=True)[0] if matched_deals else None
    latest_props = mapping_value(latest_deal, "properties") if latest_deal else None
    latest_closed_lost = bool(latest_props and deal_is_closed_lost(latest_props))
    signal_text = clean_optional(project_signal_text)
    has_new_trigger = bool(signal_text and len(signal_text) >= 20)

    if latest_closed_lost:
        if has_new_trigger:
            return {
                "code": "reopen_closed_lost_with_new_trigger",
                "label": "Reopen Closed-Lost With New Trigger",
                "rationale": "Latest matched deal is closed-lost, and a project signal was provided for this run.",
                "next_step": "Re-enter with trigger-led messaging and request a scoped reset conversation.",
                "do_not_do": "Do not re-send the previous sequence without explicitly referencing the new trigger.",
            }
        return {
            "code": "do_not_outreach_no_new_signal",
            "label": "Do Not Outreach (No New Signal)",
            "rationale": "Latest matched deal is closed-lost and no new trigger is present.",
            "next_step": "Pause outbound and capture a fresh trigger before re-engagement.",
            "do_not_do": "Do not push a standard campaign into a recently closed-lost account without new context.",
        }

    return {
        "code": "outreach_now_warm",
        "label": "Outreach Now (Warm)",
        "rationale": "Matched account has no blocking active-opportunity conflict.",
        "next_step": "Run warm outreach tied to known account context and propose a working session.",
        "do_not_do": "Do not use generic cold messaging.",
    }


def mapping_list(payload: dict[str, Any] | None, key: str) -> list[Mapping[str, Any]]:
    if not isinstance(payload, Mapping):
        return []
    raw = payload.get(key)
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, Mapping)]


def mapping_value(payload: Mapping[str, Any] | None, key: str) -> Mapping[str, Any] | None:
    if not isinstance(payload, Mapping):
        return None
    raw = payload.get(key)
    if isinstance(raw, Mapping):
        return raw
    return None


def iso_to_dt(value: str | None) -> datetime | None:
    text = clean_optional(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def closed_won_stage_ids() -> set[str]:
    raw = clean_optional(os.getenv("HUBSPOT_CLOSED_WON_STAGE_IDS"))
    if not raw:
        return set()
    return {item.strip() for item in raw.split(",") if item.strip()}


def is_closed_won_stage(stage: str | None) -> bool:
    value = clean_optional(stage)
    if not value:
        return False
    stage_norm = value.strip().lower()
    if "closedwon" in stage_norm or "closed_won" in stage_norm:
        return True
    return value in closed_won_stage_ids()


def filter_exact_company_matches(
    companies: list[Mapping[str, Any]],
    org_name: str | None,
    org_domain: str | None,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for company in companies:
        props = mapping_value(company, "properties") or {}
        name = clean_optional(str(props.get("name") or ""))
        domain = derive_domain(str(props.get("domain") or ""))
        matched = False
        if org_domain and domain and domain == org_domain:
            matched = True
        if not matched and org_name and names_likely_same(name, org_name):
            matched = True
        if not matched:
            continue
        matches.append(
            {
                "id": clean_optional(str(company.get("id") or "")),
                "name": name,
                "domain": domain,
                "industry": clean_optional(str(props.get("industry") or "")),
                "city": clean_optional(str(props.get("city") or "")),
                "state": clean_optional(str(props.get("state") or "")),
                "country": clean_optional(str(props.get("country") or "")),
                "created_at": clean_optional(str(company.get("createdAt") or "")),
                "updated_at": clean_optional(str(company.get("updatedAt") or "")),
                "url": clean_optional(str(company.get("url") or "")),
            }
        )
    return matches


def filter_exact_contact_matches(
    contacts: list[Mapping[str, Any]],
    org_name: str | None,
    org_domain: str | None,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for contact in contacts:
        props = mapping_value(contact, "properties") or {}
        email = clean_optional(str(props.get("email") or ""))
        company = clean_optional(str(props.get("company") or ""))
        full_name = " ".join(
            item
            for item in [
                clean_optional(str(props.get("firstname") or "")),
                clean_optional(str(props.get("lastname") or "")),
            ]
            if item
        )
        email_domain = derive_domain(email.split("@", 1)[1] if email and "@" in email else None)
        matched = False
        if org_domain and email_domain and email_domain == org_domain:
            matched = True
        if not matched and org_name and text_contains(company, org_name):
            matched = True
        if not matched:
            continue
        matches.append(
            {
                "id": clean_optional(str(contact.get("id") or "")),
                "full_name": full_name or None,
                "email": email,
                "phone": clean_optional(str(props.get("phone") or "")),
                "company": company,
                "created_at": clean_optional(str(contact.get("createdAt") or "")),
                "updated_at": clean_optional(str(contact.get("updatedAt") or "")),
                "url": clean_optional(str(contact.get("url") or "")),
            }
        )
    return matches


def filter_exact_deal_matches(
    deals: list[Mapping[str, Any]],
    org_name: str | None,
    query_tokens: list[str],
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for deal in deals:
        props = mapping_value(deal, "properties") or {}
        deal_name = clean_optional(str(props.get("dealname") or ""))
        if not deal_name:
            continue
        matched = False
        if org_name and text_contains(deal_name, org_name):
            matched = True
        if not matched and query_tokens:
            deal_name_norm = normalize_text(deal_name)
            matched = sum(1 for token in query_tokens if token in deal_name_norm) >= min(
                2, len(query_tokens)
            )
        if not matched:
            continue
        matches.append(
            {
                "id": clean_optional(str(deal.get("id") or "")),
                "deal_name": deal_name,
                "amount": clean_optional(str(props.get("amount") or "")),
                "dealstage": clean_optional(str(props.get("dealstage") or "")),
                "pipeline": clean_optional(str(props.get("pipeline") or "")),
                "close_date": clean_optional(str(props.get("closedate") or "")),
                "created_at": clean_optional(str(deal.get("createdAt") or "")),
                "updated_at": clean_optional(str(deal.get("updatedAt") or "")),
                "url": clean_optional(str(deal.get("url") or "")),
            }
        )
    return matches


def build_relationship_history(
    exact_companies: list[dict[str, Any]],
    exact_contacts: list[dict[str, Any]],
    exact_deals: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for company in exact_companies:
        for event_type, ts_key in [("company_created", "created_at"), ("company_updated", "updated_at")]:
            ts = company.get(ts_key)
            if not ts:
                continue
            events.append(
                {
                    "timestamp": ts,
                    "type": event_type,
                    "entity": "company",
                    "entity_id": company.get("id"),
                    "title": company.get("name"),
                    "detail": company.get("industry"),
                    "url": company.get("url"),
                }
            )
    for contact in exact_contacts:
        for event_type, ts_key in [("contact_created", "created_at"), ("contact_updated", "updated_at")]:
            ts = contact.get(ts_key)
            if not ts:
                continue
            events.append(
                {
                    "timestamp": ts,
                    "type": event_type,
                    "entity": "contact",
                    "entity_id": contact.get("id"),
                    "title": contact.get("full_name") or contact.get("email"),
                    "detail": contact.get("email") or contact.get("company"),
                    "url": contact.get("url"),
                }
            )
    for deal in exact_deals:
        for event_type, ts_key in [
            ("deal_created", "created_at"),
            ("deal_updated", "updated_at"),
            ("deal_close_date", "close_date"),
        ]:
            ts = deal.get(ts_key)
            if not ts:
                continue
            events.append(
                {
                    "timestamp": ts,
                    "type": event_type,
                    "entity": "deal",
                    "entity_id": deal.get("id"),
                    "title": deal.get("deal_name"),
                    "detail": deal.get("dealstage"),
                    "url": deal.get("url"),
                }
            )
    events.sort(
        key=lambda event: iso_to_dt(str(event.get("timestamp") or "")) or datetime.min.replace(
            tzinfo=timezone.utc
        ),
        reverse=True,
    )
    return events


def build_similar_closed_wins(
    deals: list[Mapping[str, Any]],
    years_back: int,
    closed_won_only: bool,
    max_items: int,
) -> list[dict[str, Any]]:
    cutoff_year = datetime.now(timezone.utc).year - years_back
    wins: list[dict[str, Any]] = []
    for deal in deals:
        props = mapping_value(deal, "properties") or {}
        stage = clean_optional(str(props.get("dealstage") or ""))
        if closed_won_only and not is_closed_won_stage(stage):
            continue
        close_date_raw = clean_optional(str(props.get("closedate") or ""))
        close_dt = iso_to_dt(close_date_raw)
        if close_dt and close_dt.year < cutoff_year:
            continue
        wins.append(
            {
                "id": clean_optional(str(deal.get("id") or "")),
                "deal_name": clean_optional(str(props.get("dealname") or "")),
                "amount": clean_optional(str(props.get("amount") or "")),
                "dealstage": stage,
                "pipeline": clean_optional(str(props.get("pipeline") or "")),
                "close_date": close_date_raw,
                "created_at": clean_optional(str(deal.get("createdAt") or "")),
                "updated_at": clean_optional(str(deal.get("updatedAt") or "")),
                "url": clean_optional(str(deal.get("url") or "")),
            }
        )
    wins.sort(
        key=lambda item: iso_to_dt(item.get("close_date")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return wins[:max_items]


def parse_case_study_library() -> list[dict[str, Any]]:
    raw = clean_optional(os.getenv("CASE_STUDY_LIBRARY_JSON"))
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    output: list[dict[str, Any]] = []
    for item in parsed:
        if not isinstance(item, Mapping):
            continue
        url = clean_optional(str(item.get("url") or ""))
        if not url:
            continue
        output.append(
            {
                "title": clean_optional(str(item.get("title") or "")) or "Case Study",
                "url": url,
                "industry": normalize_text(clean_optional(str(item.get("industry") or ""))),
                "tags": [
                    normalize_text(str(tag))
                    for tag in (item.get("tags") if isinstance(item.get("tags"), list) else [])
                    if clean_optional(str(tag))
                ],
            }
        )
    return output


def resolve_sharepoint_mcp_connection() -> tuple[str | None, str | None]:
    base_url = clean_optional(os.getenv("SHAREPOINT_MCP_BASE_URL")) or clean_optional(
        os.getenv("CRI_MCP_BASE_URL")
    )
    if not base_url:
        base_url = clean_optional(local_env_value("SHAREPOINT_MCP_BASE_URL")) or clean_optional(
            local_env_value("CRI_MCP_BASE_URL")
        )
    bearer = clean_env_secret_single_line(os.getenv("SHAREPOINT_MCP_BEARER")) or clean_env_secret_single_line(
        os.getenv("CRI_MCP_BEARER")
    )
    if not bearer:
        bearer = clean_env_secret_single_line(local_env_value("SHAREPOINT_MCP_BEARER")) or clean_env_secret_single_line(
            local_env_value("CRI_MCP_BEARER")
        )
    return base_url, bearer


async def call_cri_mcp_tool(tool_name: str, arguments: Mapping[str, Any]) -> tuple[Mapping[str, Any] | None, str | None]:
    base_url, bearer = resolve_sharepoint_mcp_connection()
    if not base_url:
        return None, "Missing SHAREPOINT_MCP_BASE_URL / CRI_MCP_BASE_URL for MCP tool call."
    if not bearer:
        return None, "Missing SHAREPOINT_MCP_BEARER / CRI_MCP_BEARER for MCP tool call."

    headers = {
        "Authorization": f"Bearer {bearer}",
        "x-api-key": bearer,
    }

    rpc_payload = {
        "jsonrpc": "2.0",
        "id": f"tool-{uuid.uuid4().hex[:8]}",
        "method": "tools/call",
        "params": {"name": tool_name, "arguments": dict(arguments)},
    }
    try:
        async with httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=60.0) as client:
            rpc_response, rpc_error = await fetch_external_json(
                client=client,
                method="POST",
                path="/mcp",
                headers=headers,
                json_body=rpc_payload,
            )
    except httpx.RequestError as error:
        return None, f"MCP request failed: {error}"

    if rpc_error or not isinstance(rpc_response, Mapping):
        return None, rpc_error or "Invalid MCP response payload."

    mcp_error = rpc_response.get("error")
    if isinstance(mcp_error, Mapping):
        error_message = clean_optional(str(mcp_error.get("message") or "")) or "unknown_mcp_error"
        error_code = mcp_error.get("code")
        if error_code is not None:
            return None, f"MCP tool call failed ({error_code}): {error_message}"
        return None, f"MCP tool call failed: {error_message}"
    if mcp_error:
        return None, f"MCP tool call failed: {mcp_error}"

    extracted = extract_tool_payload_from_mcp_response(rpc_response)
    if not isinstance(extracted, Mapping):
        return None, "Tool response could not be parsed from MCP response."
    return extracted, None


async def fetch_sharepoint_recommended_assets(
    payload: CaseStudyRecommendRequest,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    base_url, bearer = resolve_sharepoint_mcp_connection()
    if not base_url:
        return [], {
            "enabled": False,
            "reason": "Neither SHAREPOINT_MCP_BASE_URL nor CRI_MCP_BASE_URL is configured",
        }
    bearer_present = bool(bearer)
    bearer_fp = token_fingerprint(bearer)
    share_url = clean_optional(os.getenv("SHAREPOINT_MATCH_SHARE_URL"))
    ext_csv = clean_optional(os.getenv("SHAREPOINT_MATCH_FILE_EXTENSIONS"))
    extensions = parse_extensions_csv(ext_csv) or DEFAULT_SHAREPOINT_MATCH_FILE_EXTENSIONS

    project_description = build_sharepoint_project_description(payload)
    vertical_hint = infer_vertical_hint(payload.industry_vertical, project_description)

    request_body: dict[str, Any] = {
        "project_description": project_description,
        "file_extensions": extensions,
        "max_results": max(payload.max_items, 3),
    }
    if share_url:
        request_body["share_url"] = share_url
    if vertical_hint:
        request_body["vertical_hint"] = vertical_hint

    headers: dict[str, str] = {}
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
        headers["x-api-key"] = bearer

    output_status_code = 0
    try:
        async with httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=30.0) as client:
            tool_payload: Mapping[str, Any] | None = None
            protocol_used = "mcp_jsonrpc"
            rest_error_message: str | None = None

            # Prefer MCP JSON-RPC first for CRI server to avoid noisy /tools 404s.
            rpc_payload = {
                "jsonrpc": "2.0",
                "id": f"sharepoint-{uuid.uuid4().hex[:8]}",
                "method": "tools/call",
                "params": {"name": "sharepoint_matchBidFiles", "arguments": request_body},
            }
            rpc_response, rpc_error = await fetch_external_json(
                client=client,
                method="POST",
                path="/mcp",
                headers=headers,
                json_body=rpc_payload,
            )
            if not rpc_error and isinstance(rpc_response, Mapping):
                extracted = extract_tool_payload_from_mcp_response(rpc_response)
                if isinstance(extracted, Mapping):
                    tool_payload = extracted
                    output_status_code = 200
                else:
                    rest_error_message = "MCP /mcp call succeeded but returned unparseable tool payload"
            else:
                rest_error_message = rpc_error or "invalid /mcp response"

            # Fallback for non-MCP deployments that only expose /tools/<name>.
            if tool_payload is None:
                protocol_used = "rest_tools"
                response = await client.post("/tools/sharepoint_matchBidFiles", headers=headers, json=request_body)
                output_status_code = response.status_code
                raw_payload = parse_json(response)
                if response.is_success:
                    root_payload: Mapping[str, Any] = raw_payload if isinstance(raw_payload, Mapping) else {}
                    result_payload = root_payload.get("result")
                    tool_payload = result_payload if isinstance(result_payload, Mapping) else root_payload
                if not isinstance(tool_payload, Mapping):
                    return [], {
                        "enabled": True,
                        "status_code": output_status_code,
                        "error": summarize_external_error(raw_payload) or "sharepoint tool payload missing or invalid",
                        "mcp_error": rest_error_message,
                        "bearer_present": bearer_present,
                        "bearer_fingerprint": bearer_fp,
                    }
    except httpx.RequestError as error:
        return [], {
            "enabled": True,
            "error": f"request_failed: {error}",
            "bearer_present": bearer_present,
            "bearer_fingerprint": bearer_fp,
        }

    if not isinstance(tool_payload, Mapping):
        return [], {
            "enabled": True,
            "error": "sharepoint tool payload missing or invalid",
            "bearer_present": bearer_present,
            "bearer_fingerprint": bearer_fp,
        }

    matches = tool_payload.get("matches")
    if not isinstance(matches, list):
        return [], {
            "enabled": True,
            "error": "sharepoint_matchBidFiles returned no matches array",
            "bearer_present": bearer_present,
            "bearer_fingerprint": bearer_fp,
        }

    items: list[dict[str, Any]] = []
    for row in matches:
        if not isinstance(row, Mapping):
            continue
        url = clean_optional(str(row.get("web_url") or ""))
        if not url:
            continue
        ext = normalize_text(clean_optional(str(row.get("extension") or "")))
        if ext and ext not in {"pptx", "ppt"}:
            continue
        title = clean_optional(str(row.get("name") or "")) or "Recommended asset"
        score = row.get("score")
        reason = clean_optional(str(row.get("reason") or ""))
        path = clean_optional(str(row.get("path") or ""))
        matched_terms = row.get("matched_terms")
        last_modified = clean_optional(str(row.get("last_modified") or ""))
        item_id = clean_optional(str(row.get("id") or ""))
        thumbnail_url = clean_optional(
            str(
                row.get("thumbnail_url")
                or row.get("preview_url")
                or ""
            )
        )
        thumbnail_base64 = clean_optional(
            str(
                row.get("thumbnail_base64")
                or row.get("preview_base64")
                or ""
            )
        )
        items.append(
            {
                "id": item_id,
                "title": title,
                "url": url,
                "thumbnail_url": thumbnail_url,
                "thumbnail_base64": thumbnail_base64,
                "industry": clean_optional(payload.industry_vertical),
                "score": score if isinstance(score, (int, float)) else None,
                "reason": reason,
                "path": path,
                "matched_terms": matched_terms if isinstance(matched_terms, list) else [],
                "last_modified": last_modified,
                "source": "sharepoint_mcp",
            }
        )

    items.sort(key=sharepoint_asset_sort_key, reverse=True)
    deduped = dedupe_assets_by_url(items)
    return deduped[: payload.max_items], {
        "enabled": True,
        "status_code": output_status_code,
        "protocol": protocol_used,
        "bearer_present": bearer_present,
        "bearer_fingerprint": bearer_fp,
        "inferred_vertical": clean_optional(str(tool_payload.get("inferred_vertical") or "")),
        "scanned_file_count": tool_payload.get("scanned_file_count"),
        "matched_file_count": tool_payload.get("matched_file_count"),
    }


def build_sharepoint_project_description(payload: CaseStudyRecommendRequest) -> str:
    direct = clean_optional(payload.project_description)
    if direct:
        return direct
    parts = [
        clean_optional(payload.organization_name),
        clean_optional(payload.industry_vertical),
        "bid response presentation and customer story alignment for enterprise consulting services",
    ]
    return " ".join(part for part in parts if part)


def parse_extensions_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [
        token.strip().lower().lstrip(".")
        for token in raw.split(",")
        if token.strip()
    ]


def build_sharepoint_preview_candidates(source_url: str) -> list[str]:
    try:
        parsed = urlparse(source_url)
    except ValueError:
        return []
    if not parsed.scheme or not parsed.netloc:
        return []
    origin = f"{parsed.scheme}://{parsed.netloc}"
    encoded = quote(source_url, safe="")
    return [
        f"{origin}/_layouts/15/getpreview.ashx?path={encoded}&resolution=2",
        f"{origin}/_layouts/15/getpreview.ashx?path={encoded}&resolution=3&cropMode=1",
    ]


def infer_vertical_hint(industry_vertical: str | None, project_description: str | None) -> str | None:
    text = normalize_text(industry_vertical) or normalize_text(project_description)
    if not text:
        return None
    if any(token in text for token in ["sled", "state", "local", "government", "public sector"]):
        return "SLED"
    if any(token in text for token in ["healthcare", "hospital", "medical", "clinic", "payer"]):
        return "Healthcare"
    if any(token in text for token in ["manufacturing", "factory", "industrial", "plant"]):
        return "Manufacturing"
    if any(token in text for token in ["finserve", "finance", "financial", "bank", "fintech"]):
        return "Finserve"
    if any(token in text for token in ["telecom", "media", "technology", "tech", "software", "saas"]):
        return "Telecom, Media & Tech"
    if any(token in text for token in ["utility", "utilities", "energy", "electric", "water", "gas"]):
        return "Utilities"
    return None


def has_direct_company_candidate(
    companies: list[Mapping[str, Any]], org_name: str | None, org_domain: str | None
) -> bool:
    for company in companies:
        props = mapping_value(company, "properties") or {}
        name = clean_optional(str(props.get("name") or ""))
        domain = derive_domain(str(props.get("domain") or ""))
        if org_domain and domain and domain == org_domain:
            return True
        if org_name and name and names_likely_same(name, org_name):
            return True
    return False


async def extend_hubspot_companies_until_match(
    *,
    client: httpx.AsyncClient,
    headers: Mapping[str, str],
    install_id: str | None,
    token_id: str | None,
    access_token: str | None,
    query_text: str | None,
    org_name: str | None,
    org_domain: str | None,
    companies_data: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(companies_data, Mapping):
        return companies_data
    merged: list[Mapping[str, Any]] = list(mapping_list(companies_data, "companies"))
    if not merged:
        return companies_data
    if has_direct_company_candidate(merged, org_name, org_domain):
        return companies_data

    paging = mapping_value(companies_data, "paging")
    next_block = mapping_value(paging, "next")
    after = clean_optional(str(next_block.get("after") or "")) if next_block else None
    if not after:
        return companies_data

    max_pages_raw = clean_optional(os.getenv("HUBSPOT_COMPANY_SCAN_MAX_PAGES"))
    try:
        max_pages = max(1, min(20, int(max_pages_raw or "8")))
    except ValueError:
        max_pages = 8

    seen: set[str] = set()
    for item in merged:
        if isinstance(item, Mapping):
            seen.add(candidate_dedupe_key(item))

    page_count = 0
    latest_after = after
    while latest_after and page_count < max_pages and not has_direct_company_candidate(
        merged, org_name, org_domain
    ):
        params = {**build_hubspot_base_params(install_id, token_id, access_token), "limit": 50, "after": latest_after}
        if query_text:
            params.update({"q": query_text, "query": query_text, "search": query_text, "term": query_text})
        page_payload, page_error = await fetch_external_json(
            client=client,
            method="GET",
            path="/hubspot/companies",
            headers=headers,
            params=params,
        )
        if page_error or not isinstance(page_payload, Mapping):
            break
        page_companies = mapping_list(page_payload, "companies")
        for company in page_companies:
            key = candidate_dedupe_key(company)
            if key in seen:
                continue
            seen.add(key)
            merged.append(company)
        page_paging = mapping_value(page_payload, "paging")
        page_next = mapping_value(page_paging, "next")
        latest_after = clean_optional(str(page_next.get("after") or "")) if page_next else None
        page_count += 1

    if not merged:
        return companies_data
    updated = dict(companies_data)
    updated["companies"] = merged
    updated["count"] = len(merged)
    if latest_after:
        updated["paging"] = {"next": {"after": latest_after}}
    return updated


def dedupe_assets_by_url(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    seen_stems: set[str] = set()
    out: list[dict[str, Any]] = []
    for item in items:
        url = clean_optional(str(item.get("url") or ""))
        if not url:
            continue
        key = url.lower()
        if key in seen:
            continue
        stem = normalize_asset_stem(clean_optional(str(item.get("title") or "")))
        if stem and stem in seen_stems:
            continue
        seen.add(key)
        if stem:
            seen_stems.add(stem)
        out.append(item)
    return out


def sharepoint_asset_sort_key(item: Mapping[str, Any]) -> tuple[float, int, float]:
    score = float(item.get("score") or 0)
    terms = item.get("matched_terms")
    matched_count = len(terms) if isinstance(terms, list) else 0
    last_modified = clean_optional(str(item.get("last_modified") or ""))
    dt = iso_to_dt(last_modified)
    recency = dt.timestamp() if dt else 0.0
    return score, matched_count, recency


def normalize_asset_stem(title: str | None) -> str:
    value = normalize_text(title)
    if not value:
        return ""
    value = re.sub(r"\.(pptx|ppt|pdf|docx|doc)$", "", value)
    value = re.sub(r"\b(slide|deck|presentation)\b", " ", value)
    value = re.sub(r"[-_]+", " ", value)
    return compact_spaces(value)


def summarize_external_error(payload: Mapping[str, Any] | None) -> str:
    if not isinstance(payload, Mapping):
        return "unknown_error"
    for key in ("error", "message", "details"):
        value = payload.get(key)
        if isinstance(value, Mapping):
            msg = value.get("message") or value.get("error")
            if msg:
                return str(msg)
        if value:
            return str(value)
    return "unknown_error"


def extract_tool_payload_from_mcp_response(payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
    result = payload.get("result")
    if not isinstance(result, Mapping):
        return None

    nested_result = result.get("result")
    if isinstance(nested_result, Mapping):
        return nested_result

    content = result.get("content")
    if isinstance(content, list):
        for item in content:
            if not isinstance(item, Mapping):
                continue
            text = item.get("text")
            if not isinstance(text, str):
                continue
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, Mapping):
                return parsed

    return result if isinstance(result, Mapping) else None


def build_hubspot_request_headers(
    base_headers: Mapping[str, str], access_token: str | None
) -> dict[str, str]:
    request_headers = dict(base_headers)
    token = clean_env_secret_single_line(access_token)
    if not token:
        return request_headers
    request_headers["Authorization"] = f"Bearer {token}"
    request_headers["X-HubSpot-Access-Token"] = token
    request_headers["x-access-token"] = token
    request_headers["access-token"] = token
    request_headers["access_token"] = token
    return request_headers


def build_hubspot_base_params(
    install_id: str | None, token_id: str | None, access_token: str | None
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if install_id:
        params["install_id"] = install_id
        params["installId"] = install_id
    if token_id:
        params["tokenId"] = token_id
        params["token_id"] = token_id
    token = clean_env_secret_single_line(access_token)
    if token:
        params["accessToken"] = token
        params["access_token"] = token
    return params


def hubspot_error_is_auth(error_message: str | None) -> bool:
    if not error_message:
        return False
    lower = error_message.lower()
    if "http 401" in lower or "http 403" in lower:
        return True
    return "authentication" in lower or "missing access token" in lower


def hubspot_section_accessible(summary_data: dict[str, Any] | None, section: str) -> bool:
    if not isinstance(summary_data, Mapping):
        return True
    summary_block = summary_data.get("summary")
    if not isinstance(summary_block, Mapping):
        return True
    section_data = summary_block.get(section)
    if not isinstance(section_data, Mapping):
        return True
    return section_data.get("accessible") is not False


def hubspot_direct_search_enabled() -> bool:
    raw = clean_optional(os.getenv("HUBSPOT_DIRECT_SEARCH_ENABLED"))
    if not raw:
        return True
    return normalize_text(raw) not in {"0", "false", "off", "no"}


def hubspot_crm_headers(access_token: str | None) -> dict[str, str]:
    token = clean_env_secret_single_line(access_token)
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def extract_hubspot_results(payload: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(payload, Mapping):
        return []
    raw = payload.get("results")
    if not isinstance(raw, list):
        return []
    return [dict(item) for item in raw if isinstance(item, Mapping)]


def extract_hubspot_after(payload: Mapping[str, Any] | None) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    paging = mapping_value(payload, "paging")
    next_block = mapping_value(paging, "next")
    return clean_optional(str(next_block.get("after") or "")) if next_block else None


def append_unique_hubspot_records(target: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> None:
    seen_ids = {
        clean_optional(str(item.get("id") or ""))
        for item in target
        if clean_optional(str(item.get("id") or ""))
    }
    for item in incoming:
        item_id = clean_optional(str(item.get("id") or ""))
        if item_id and item_id in seen_ids:
            continue
        if item_id:
            seen_ids.add(item_id)
        target.append(item)


def collect_owner_ids_from_hubspot_records(records: list[dict[str, Any]]) -> set[str]:
    owner_ids: set[str] = set()
    for record in records:
        if not isinstance(record, Mapping):
            continue
        props = mapping_value(record, "properties") or {}
        for key in ("hubspot_owner_id", "hs_owner_id", "owner_id"):
            value = clean_optional(str(props.get(key) or ""))
            if value:
                owner_ids.add(value)
    return owner_ids


async def fetch_hubspot_owner_map(
    *, client: httpx.AsyncClient, access_token: str | None, owner_ids: set[str]
) -> dict[str, dict[str, str]]:
    if not owner_ids or not clean_env_secret_single_line(access_token):
        return {}
    headers = hubspot_crm_headers(access_token)
    endpoint = "https://api.hubapi.com/crm/v3/owners"
    after: str | None = None
    max_pages = 10
    pages = 0
    owners: dict[str, dict[str, str]] = {}

    while pages < max_pages:
        params: dict[str, Any] = {"limit": 100, "archived": "false"}
        if after:
            params["after"] = after
        payload, error = await fetch_external_json(
            client=client,
            method="GET",
            path=endpoint,
            headers=headers,
            params=params,
        )
        if error or not isinstance(payload, Mapping):
            break
        rows = payload.get("results")
        if not isinstance(rows, list) or not rows:
            break
        for item in rows:
            if not isinstance(item, Mapping):
                continue
            owner_id = clean_optional(str(item.get("id") or item.get("ownerId") or ""))
            if not owner_id:
                continue
            first = clean_optional(str(item.get("firstName") or ""))
            last = clean_optional(str(item.get("lastName") or ""))
            full_name = compact_spaces(" ".join(part for part in [first, last] if part))
            email = clean_optional(str(item.get("email") or ""))
            owners[owner_id] = {
                "name": full_name or "",
                "email": email or "",
            }
        if owner_ids.issubset(set(owners.keys())):
            break
        after = extract_hubspot_after(payload)
        pages += 1
        if not after:
            break

    missing = [owner_id for owner_id in owner_ids if owner_id not in owners]
    if missing:
        legacy_payload, legacy_error = await fetch_external_json(
            client=client,
            method="GET",
            path="https://api.hubapi.com/owners/v2/owners",
            headers=headers,
            params={},
        )
        if not legacy_error:
            legacy_rows: list[Any] = []
            if isinstance(legacy_payload, list):
                legacy_rows = legacy_payload
            elif isinstance(legacy_payload, Mapping):
                maybe = legacy_payload.get("results")
                if isinstance(maybe, list):
                    legacy_rows = maybe
            for item in legacy_rows:
                if not isinstance(item, Mapping):
                    continue
                legacy_id = clean_optional(
                    str(item.get("ownerId") or item.get("id") or item.get("owner_id") or "")
                )
                if not legacy_id or legacy_id in owners:
                    continue
                first = clean_optional(str(item.get("firstName") or ""))
                last = clean_optional(str(item.get("lastName") or ""))
                full_name = compact_spaces(" ".join(part for part in [first, last] if part))
                email = clean_optional(str(item.get("email") or ""))
                owners[legacy_id] = {
                    "name": full_name or "",
                    "email": email or "",
                }

    missing = [owner_id for owner_id in owner_ids if owner_id not in owners]
    for owner_id in missing:
        resolved_payload: Mapping[str, Any] | None = None
        for id_property in ["id", "userId"]:
            payload, error = await fetch_external_json(
                client=client,
                method="GET",
                path=f"https://api.hubapi.com/crm/v3/owners/{owner_id}",
                headers=headers,
                params={"idProperty": id_property},
            )
            if not error and isinstance(payload, Mapping):
                resolved_payload = payload
                break
        if not isinstance(resolved_payload, Mapping):
            continue
        first = clean_optional(str(resolved_payload.get("firstName") or ""))
        last = clean_optional(str(resolved_payload.get("lastName") or ""))
        full_name = compact_spaces(" ".join(part for part in [first, last] if part))
        email = clean_optional(str(resolved_payload.get("email") or ""))
        owners[owner_id] = {
            "name": full_name or "",
            "email": email or "",
        }
    overrides = load_hubspot_owner_overrides_from_env()
    if overrides:
        for owner_id in owner_ids:
            override = overrides.get(owner_id)
            if not isinstance(override, Mapping):
                continue
            existing = owners.get(owner_id) if isinstance(owners.get(owner_id), Mapping) else {}
            existing_name = clean_optional(str(existing.get("name") or "")) if isinstance(existing, Mapping) else None
            existing_email = clean_optional(str(existing.get("email") or "")) if isinstance(existing, Mapping) else None
            override_name = clean_optional(str(override.get("name") or ""))
            override_email = clean_optional(str(override.get("email") or ""))
            owners[owner_id] = {
                "name": override_name or existing_name or "",
                "email": override_email or existing_email or "",
            }

    return owners


async def fetch_deal_stage_map(
    *, client: httpx.AsyncClient, access_token: str | None
) -> dict[str, dict[str, str]]:
    if not clean_env_secret_single_line(access_token):
        return {}
    headers = hubspot_crm_headers(access_token)
    endpoint = "https://api.hubapi.com/crm/v3/pipelines/deals"
    payload, error = await fetch_external_json(
        client=client,
        method="GET",
        path=endpoint,
        headers=headers,
        params={"archived": "false"},
    )
    if error or not isinstance(payload, Mapping):
        return {}
    out: dict[str, dict[str, str]] = {}
    pipelines = payload.get("results")
    if not isinstance(pipelines, list):
        return out
    for pipeline in pipelines:
        if not isinstance(pipeline, Mapping):
            continue
        pipeline_id = clean_optional(str(pipeline.get("id") or ""))
        stages = pipeline.get("stages")
        if not isinstance(stages, list):
            continue
        for stage in stages:
            if not isinstance(stage, Mapping):
                continue
            stage_id = clean_optional(str(stage.get("id") or ""))
            if not stage_id:
                continue
            out[stage_id] = {
                "label": clean_optional(str(stage.get("label") or stage.get("displayOrder") or "")) or stage_id,
                "pipeline_id": pipeline_id or "",
                "pipeline_label": clean_optional(str(pipeline.get("label") or "")) or pipeline_id or "",
            }

    # Fallback for restricted list endpoints: resolve stages from commonly used pipeline IDs.
    if not out:
        for pipeline_id in ["default", "sales_pipeline", "pipeline", "0"]:
            payload, error = await fetch_external_json(
                client=client,
                method="GET",
                path=f"https://api.hubapi.com/crm/v3/pipelines/deals/{pipeline_id}",
                headers=headers,
                params={"archived": "false"},
            )
            if error or not isinstance(payload, Mapping):
                continue
            stages = payload.get("stages")
            if not isinstance(stages, list):
                continue
            pipeline_label = clean_optional(str(payload.get("label") or "")) or pipeline_id
            for stage in stages:
                if not isinstance(stage, Mapping):
                    continue
                stage_id = clean_optional(str(stage.get("id") or ""))
                if not stage_id:
                    continue
                out[stage_id] = {
                    "label": clean_optional(str(stage.get("label") or "")) or stage_id,
                    "pipeline_id": pipeline_id,
                    "pipeline_label": pipeline_label,
                }
            if out:
                break
    return out


def enrich_hubspot_records_with_owner(
    records: list[dict[str, Any]], owners: Mapping[str, Mapping[str, str]]
) -> None:
    if not records or not owners:
        return
    for record in records:
        props = mapping_value(record, "properties")
        if not isinstance(props, dict):
            continue
        owner_id = clean_optional(
            str(props.get("hubspot_owner_id") or props.get("hs_owner_id") or props.get("owner_id") or "")
        )
        if not owner_id:
            continue
        owner_info = owners.get(owner_id)
        if not isinstance(owner_info, Mapping):
            continue
        owner_name = clean_optional(str(owner_info.get("name") or ""))
        owner_email = clean_optional(str(owner_info.get("email") or ""))
        if owner_name:
            props["hubspot_owner_name"] = owner_name
        if owner_email:
            props["hubspot_owner_email"] = owner_email


def enrich_deals_with_stage_labels(
    deals: list[dict[str, Any]], stage_map: Mapping[str, Mapping[str, str]]
) -> None:
    if not deals:
        return
    for deal in deals:
        props = mapping_value(deal, "properties")
        if not isinstance(props, dict):
            continue
        stage_id = clean_optional(str(props.get("dealstage") or ""))
        info = stage_map.get(stage_id) if stage_id else None
        label = clean_optional(str(info.get("label") or "")) if isinstance(info, Mapping) else None
        pipeline_label = (
            clean_optional(str(info.get("pipeline_label") or "")) if isinstance(info, Mapping) else None
        )
        if not label:
            is_closed = normalize_text(str(props.get("hs_is_closed") or "")) in {"true", "1", "yes"}
            is_won = normalize_text(str(props.get("hs_is_closed_won") or "")) in {"true", "1", "yes"}
            if is_closed and is_won:
                label = "Closed Won"
            elif is_closed:
                label = "Closed Lost"
            elif stage_id:
                label = f"Open ({stage_id})"
            else:
                label = "Open"
        if label:
            props["dealstage_label"] = label
        if pipeline_label:
            props["pipeline_label"] = pipeline_label


def build_owner_map_from_payload(payload: Mapping[str, Any] | None) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    if not isinstance(payload, Mapping):
        return out
    candidates: list[Any] = []
    for key in ("owners", "users", "results", "items", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            candidates.extend(value)
    if not candidates and isinstance(payload.get("results"), Mapping):
        nested = payload.get("results")
        if isinstance(nested, Mapping):
            for key in ("owners", "users", "items"):
                value = nested.get(key)
                if isinstance(value, list):
                    candidates.extend(value)
    if not candidates and any(k in payload for k in ("id", "ownerId", "userId")):
        candidates = [payload]

    for item in candidates:
        if not isinstance(item, Mapping):
            continue
        owner_id = clean_optional(
            str(item.get("id") or item.get("ownerId") or item.get("owner_id") or item.get("userId") or "")
        )
        if not owner_id:
            continue
        first = clean_optional(str(item.get("firstName") or item.get("first_name") or ""))
        last = clean_optional(str(item.get("lastName") or item.get("last_name") or ""))
        full_name = compact_spaces(
            " ".join(
                part
                for part in [
                    first,
                    last,
                    clean_optional(str(item.get("name") or "")),
                    clean_optional(str(item.get("fullName") or "")),
                ]
                if part
            )
        )
        email = clean_optional(str(item.get("email") or item.get("userEmail") or ""))
        out[owner_id] = {"name": full_name or "", "email": email or ""}
    return out


async def fetch_owner_map_via_mcp(
    *,
    client: httpx.AsyncClient,
    headers: Mapping[str, str],
    base_params: Mapping[str, Any],
    owner_ids: set[str],
) -> dict[str, dict[str, str]]:
    if not owner_ids:
        return {}
    paths = ["/hubspot/owners", "/hubspot/users"]
    merged: dict[str, dict[str, str]] = {}
    for path in paths:
        payload, error = await fetch_external_json(
            client=client,
            method="GET",
            path=path,
            headers=headers,
            params=dict(base_params),
        )
        if error:
            continue
        parsed = build_owner_map_from_payload(payload)
        merged.update(parsed)
        if owner_ids.issubset(set(merged.keys())):
            break
    return merged


async def fetch_hubspot_crm_object_search(
    *,
    client: httpx.AsyncClient,
    access_token: str | None,
    object_type: Literal["companies", "contacts", "deals"],
    properties: list[str],
    query_text: str | None,
    filter_groups: list[dict[str, Any]] | None = None,
    max_items: int = 50,
) -> tuple[list[dict[str, Any]], str | None]:
    if not clean_env_secret_single_line(access_token):
        return [], "direct_crm_search: missing access token"

    endpoint = f"https://api.hubapi.com/crm/v3/objects/{object_type}/search"
    headers = hubspot_crm_headers(access_token)
    page_size = max(1, min(100, max_items))
    max_pages_raw = clean_optional(os.getenv("HUBSPOT_DIRECT_SEARCH_MAX_PAGES"))
    try:
        max_pages = max(1, min(10, int(max_pages_raw or "4")))
    except ValueError:
        max_pages = 4

    out: list[dict[str, Any]] = []
    after: str | None = None
    pages = 0

    while pages < max_pages and len(out) < max_items:
        body: dict[str, Any] = {"limit": page_size, "properties": properties}
        if query_text:
            body["query"] = query_text
        if filter_groups:
            body["filterGroups"] = filter_groups
        if after:
            body["after"] = after
        payload, error = await fetch_external_json(
            client=client,
            method="POST",
            path=endpoint,
            headers=headers,
            json_body=body,
        )
        if error:
            return out, error
        page_results = extract_hubspot_results(payload)
        append_unique_hubspot_records(out, page_results)
        after = extract_hubspot_after(payload)
        pages += 1
        if not after or not page_results:
            break

    return out[:max_items], None


async def fetch_hubspot_context_bundle_direct(
    *,
    client: httpx.AsyncClient,
    access_token: str | None,
    query_text: str | None,
    org_name: str | None,
    org_domain: str | None,
    max_items: int,
) -> dict[str, Any]:
    company_properties = [
        "name",
        "domain",
        "industry",
        "city",
        "state",
        "country",
        "hubspot_owner_id",
        "hs_lead_status",
        "lifecyclestage",
        "lastmodifieddate",
    ]
    contact_properties = [
        "firstname",
        "lastname",
        "email",
        "phone",
        "company",
        "jobtitle",
        "hubspot_owner_id",
        "hs_lead_status",
        "lastmodifieddate",
    ]
    deal_properties = [
        "dealname",
        "amount",
        "dealstage",
        "pipeline",
        "closedate",
        "createdate",
        "hs_lastmodifieddate",
        "hs_next_step",
        "hs_next_activity_date",
        "hs_last_activity_date",
        "hs_is_closed",
        "hs_is_closed_won",
        "hubspot_owner_id",
        "closed_lost_reason",
        "associatedcompanyid",
        "associated_company_id",
        "hs_object_id",
    ]

    companies: list[dict[str, Any]] = []
    contacts: list[dict[str, Any]] = []
    deals: list[dict[str, Any]] = []
    company_errors: list[str] = []
    contact_errors: list[str] = []
    deal_errors: list[str] = []

    if org_domain:
        domain_companies, domain_error = await fetch_hubspot_crm_object_search(
            client=client,
            access_token=access_token,
            object_type="companies",
            properties=company_properties,
            query_text=None,
            filter_groups=[
                {"filters": [{"propertyName": "domain", "operator": "EQ", "value": org_domain}]}
            ],
            max_items=max_items,
        )
        append_unique_hubspot_records(companies, domain_companies)
        if domain_error:
            company_errors.append(domain_error)

    company_query = query_text or org_name
    if company_query:
        name_companies, name_error = await fetch_hubspot_crm_object_search(
            client=client,
            access_token=access_token,
            object_type="companies",
            properties=company_properties,
            query_text=company_query,
            max_items=max_items,
        )
        append_unique_hubspot_records(companies, name_companies)
        if name_error:
            company_errors.append(name_error)

    contact_query = query_text or org_name or org_domain
    if contact_query:
        contact_results, contact_error = await fetch_hubspot_crm_object_search(
            client=client,
            access_token=access_token,
            object_type="contacts",
            properties=contact_properties,
            query_text=contact_query,
            max_items=max_items,
        )
        append_unique_hubspot_records(contacts, contact_results)
        if contact_error:
            contact_errors.append(contact_error)

    deal_query = query_text or org_name or org_domain
    if deal_query:
        deal_results, deal_error = await fetch_hubspot_crm_object_search(
            client=client,
            access_token=access_token,
            object_type="deals",
            properties=deal_properties,
            query_text=deal_query,
            max_items=max_items,
        )
        append_unique_hubspot_records(deals, deal_results)
        if deal_error:
            deal_errors.append(deal_error)

    owner_ids = (
        collect_owner_ids_from_hubspot_records(companies)
        .union(collect_owner_ids_from_hubspot_records(contacts))
        .union(collect_owner_ids_from_hubspot_records(deals))
    )
    owner_map = await fetch_hubspot_owner_map(client=client, access_token=access_token, owner_ids=owner_ids)
    stage_map = await fetch_deal_stage_map(client=client, access_token=access_token)
    enrich_hubspot_records_with_owner(companies, owner_map)
    enrich_hubspot_records_with_owner(contacts, owner_map)
    enrich_hubspot_records_with_owner(deals, owner_map)
    enrich_deals_with_stage_labels(deals, stage_map)

    combined_errors = company_errors + contact_errors + deal_errors
    search_data = {
        "source": "hubspot_crm_direct",
        "query": query_text,
        "results": {"companies": companies, "contacts": contacts, "deals": deals},
    }
    return {
        "search_data": search_data,
        "companies_data": {"companies": companies, "count": len(companies)},
        "contacts_data": {"contacts": contacts, "count": len(contacts)},
        "deals_data": {"deals": deals, "count": len(deals)},
        "search_error": "; ".join(combined_errors) if combined_errors else None,
        "companies_error": "; ".join(company_errors) if company_errors else None,
        "contacts_error": "; ".join(contact_errors) if contact_errors else None,
        "deals_error": "; ".join(deal_errors) if deal_errors else None,
    }
async def fetch_hubspot_context_bundle(
    *,
    client: httpx.AsyncClient,
    headers: Mapping[str, str],
    install_id: str | None,
    token_id: str | None,
    access_token: str | None,
    query_text: str | None,
    org_name: str | None,
    org_domain: str | None,
    max_items: int,
) -> dict[str, Any]:
    base_params = build_hubspot_base_params(install_id, token_id, access_token)
    summary_data, summary_error = await fetch_external_json(
        client=client,
        method="GET",
        path="/hubspot/summary",
        headers=headers,
        params={**base_params, **({"q": query_text} if query_text else {})},
    )

    search_data: dict[str, Any] | None = None
    search_error: str | None = None
    companies_data: dict[str, Any] | None = None
    companies_error: str | None = None
    contacts_data: dict[str, Any] | None = None
    contacts_error: str | None = None
    deals_data: dict[str, Any] | None = None
    deals_error: str | None = None
    retrieval_source = "hubspot_mcp"

    direct_enabled = hubspot_direct_search_enabled()
    direct_bundle: dict[str, Any] | None = None
    if direct_enabled and clean_env_secret_single_line(access_token):
        direct_bundle = await fetch_hubspot_context_bundle_direct(
            client=client,
            access_token=access_token,
            query_text=query_text,
            org_name=org_name,
            org_domain=org_domain,
            max_items=max_items,
        )
        search_data = direct_bundle.get("search_data") if isinstance(direct_bundle.get("search_data"), Mapping) else None
        search_error = clean_optional(str(direct_bundle.get("search_error") or ""))
        companies_data = direct_bundle.get("companies_data") if isinstance(direct_bundle.get("companies_data"), Mapping) else None
        companies_error = clean_optional(str(direct_bundle.get("companies_error") or ""))
        contacts_data = direct_bundle.get("contacts_data") if isinstance(direct_bundle.get("contacts_data"), Mapping) else None
        contacts_error = clean_optional(str(direct_bundle.get("contacts_error") or ""))
        deals_data = direct_bundle.get("deals_data") if isinstance(direct_bundle.get("deals_data"), Mapping) else None
        deals_error = clean_optional(str(direct_bundle.get("deals_error") or ""))
        retrieval_source = "hubspot_crm_direct"

    need_mcp_fallback = (
        not direct_bundle
        or (not companies_data and not contacts_data and not deals_data)
        or bool(search_error and companies_error and contacts_error and deals_error)
    )
    if need_mcp_fallback:
        if query_text:
            search_data, search_error = await fetch_external_json(
                client=client,
                method="GET",
                path="/hubspot/search",
                headers=headers,
                params={
                    **base_params,
                    "q": query_text,
                    "query": query_text,
                    "search": query_text,
                    "term": query_text,
                    "limit": max_items,
                },
            )

        company_params = {**base_params, "limit": max_items}
        if query_text:
            company_params.update(
                {
                    "q": query_text,
                    "query": query_text,
                    "search": query_text,
                    "term": query_text,
                }
            )
        companies_data, companies_error = await fetch_external_json(
            client=client,
            method="GET",
            path="/hubspot/companies",
            headers=headers,
            params=company_params,
        )

        contact_params = {**base_params, "limit": max_items}
        if query_text:
            contact_params.update(
                {
                    "q": query_text,
                    "query": query_text,
                    "search": query_text,
                    "term": query_text,
                }
            )
        contacts_data, contacts_error = await fetch_external_json(
            client=client,
            method="GET",
            path="/hubspot/contacts",
            headers=headers,
            params=contact_params,
        )

        deal_params = {**base_params, "limit": max_items}
        if query_text:
            deal_params.update(
                {
                    "q": query_text,
                    "query": query_text,
                    "search": query_text,
                    "term": query_text,
                }
            )
        deals_data, deals_error = await fetch_external_json(
            client=client,
            method="GET",
            path="/hubspot/deals",
            headers=headers,
            params=deal_params,
        )
        retrieval_source = "hubspot_mcp" if not direct_bundle else "hubspot_crm_direct+mcp_fallback"

    # Owner-name enrichment fallback through MCP when direct owner APIs do not return names.
    companies_list = [dict(item) for item in mapping_list(companies_data, "companies")]
    contacts_list = [dict(item) for item in mapping_list(contacts_data, "contacts")]
    deals_list = [dict(item) for item in mapping_list(deals_data, "deals")]
    owner_ids = (
        collect_owner_ids_from_hubspot_records(companies_list)
        .union(collect_owner_ids_from_hubspot_records(contacts_list))
        .union(collect_owner_ids_from_hubspot_records(deals_list))
    )
    if owner_ids:
        owner_map_mcp = await fetch_owner_map_via_mcp(
            client=client,
            headers=headers,
            base_params=base_params,
            owner_ids=owner_ids,
        )
        if owner_map_mcp:
            enrich_hubspot_records_with_owner(companies_list, owner_map_mcp)
            enrich_hubspot_records_with_owner(contacts_list, owner_map_mcp)
            enrich_hubspot_records_with_owner(deals_list, owner_map_mcp)
            if isinstance(companies_data, dict):
                companies_data["companies"] = companies_list
            if isinstance(contacts_data, dict):
                contacts_data["contacts"] = contacts_list
            if isinstance(deals_data, dict):
                deals_data["deals"] = deals_list

    errors = [summary_error, search_error, companies_error, contacts_error, deals_error]
    auth_error = any(hubspot_error_is_auth(message) for message in errors if message)
    return {
        "retrieval_source": retrieval_source,
        "summary_data": summary_data,
        "search_data": search_data,
        "companies_data": companies_data,
        "contacts_data": contacts_data,
        "deals_data": deals_data,
        "summary_error": summary_error,
        "search_error": search_error,
        "companies_error": companies_error,
        "contacts_error": contacts_error,
        "deals_error": deals_error,
        "auth_error": auth_error,
    }

async def fetch_external_json(
    client: httpx.AsyncClient,
    method: Literal["GET", "POST"],
    path: str,
    headers: Mapping[str, str] | None = None,
    params: Mapping[str, Any] | None = None,
    json_body: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    request_headers = dict(headers or {})
    request_params = dict(params or {})
    try:
        response = await client.request(
            method,
            path,
            headers=request_headers,
            params=request_params,
            json=dict(json_body or {}) if json_body is not None else None,
        )
    except httpx.RequestError as error:
        logger.error(
            "[external] request failed method=%s path=%s header_keys=%s param_keys=%s error=%s",
            method,
            path,
            sorted(request_headers.keys()),
            sorted(request_params.keys()),
            error,
        )
        return None, f"{path}: request failed ({error})"

    payload = parse_json(response)
    if response.is_success and isinstance(payload, dict):
        return payload, None
    if response.is_success and not payload:
        return {}, None
    detail = None
    if isinstance(payload, Mapping):
        error_value = payload.get("error")
        message_value = payload.get("message")
        details_value = payload.get("details")
        if isinstance(error_value, Mapping):
            detail = error_value.get("message") or error_value.get("error")
        elif error_value:
            detail = str(error_value)
        elif message_value:
            detail = str(message_value)
        elif details_value:
            detail = str(details_value)
    logger.error(
        "[external] non-success method=%s path=%s status=%s header_keys=%s param_keys=%s payload=%s",
        method,
        path,
        response.status_code,
        sorted(request_headers.keys()),
        sorted(request_params.keys()),
        payload if payload else "<empty>",
    )
    return None, f"{path}: HTTP {response.status_code}{f' - {detail}' if detail else ''}"


def extract_hubspot_access_token(payload: Mapping[str, Any] | None) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    for key in ("access_token", "accessToken"):
        value = clean_optional(str(payload.get(key) or ""))
        if value:
            return value
    nested = payload.get("token")
    if isinstance(nested, Mapping):
        for key in ("access_token", "accessToken"):
            value = clean_optional(str(nested.get(key) or ""))
            if value:
                return value
    return None


async def resolve_hubspot_access_token(
    client: httpx.AsyncClient,
    headers: Mapping[str, str],
    token_id: str | None,
) -> str | None:
    if not token_id:
        return None
    token_data, _ = await fetch_external_json(
        client=client,
        method="GET",
        path=f"/oauth/token/{token_id}",
        headers=headers,
    )
    return extract_hubspot_access_token(token_data)


async def refresh_hubspot_access_token(
    *,
    client: httpx.AsyncClient,
    headers: Mapping[str, str],
    install_id: str | None,
    token_id: str | None,
    refresh_token: str | None,
) -> tuple[str | None, bool]:
    attempts: list[dict[str, Any]] = []
    if install_id:
        attempts.append({"install_id": install_id})
        attempts.append({"installId": install_id})
        attempts.append({"install_id": install_id, "installId": install_id})
    if token_id:
        attempts.append({"tokenId": token_id})
        attempts.append({"token_id": token_id})
        attempts.append({"tokenId": token_id, "token_id": token_id})
    if refresh_token:
        attempts.append({"refresh_token": refresh_token})
        attempts.append({"refreshToken": refresh_token})
    if token_id and refresh_token:
        attempts.append(
            {
                "tokenId": token_id,
                "token_id": token_id,
                "refresh_token": refresh_token,
                "refreshToken": refresh_token,
            }
        )

    for payload in attempts:
        refresh_data, refresh_error = await fetch_external_json(
            client=client,
            method="POST",
            path="/oauth/refresh",
            headers=headers,
            json_body=payload,
        )
        refreshed = extract_hubspot_access_token(refresh_data)
        if refreshed:
            logger.info(
                "[hubspot] token refresh succeeded payload_keys=%s token_fingerprint=%s",
                sorted(payload.keys()),
                token_fingerprint(refreshed),
            )
            return refreshed, True
        if refresh_data is not None and install_id:
            logger.info(
                "[hubspot] install refresh succeeded payload_keys=%s (no direct token returned)",
                sorted(payload.keys()),
            )
            return None, True
        if refresh_error:
            logger.warning(
                "[hubspot] token refresh failed payload_keys=%s error=%s",
                sorted(payload.keys()),
                refresh_error,
            )
    return None, False


async def ensure_hubspot_mcp_connection(
    client: httpx.AsyncClient,
    headers: Mapping[str, str],
    install_id: str | None,
    access_token: str | None,
    mcp_server_url: str,
) -> None:
    if not access_token and not install_id:
        return
    status_params = build_hubspot_base_params(install_id, None, None)
    status_data, status_error = await fetch_external_json(
        client=client,
        method="GET",
        path="/mcp/status",
        headers=headers,
        params=status_params,
    )
    if status_data and isinstance(status_data, Mapping):
        connected = status_data.get("connected")
        if connected is True:
            return
    if status_error and "404" in status_error:
        return
    connect_body: dict[str, Any] = {"mcp_server_url": mcp_server_url}
    if install_id:
        connect_body["install_id"] = install_id
        connect_body["installId"] = install_id
    if access_token:
        connect_body["access_token"] = access_token
    await fetch_external_json(
        client=client,
        method="POST",
        path="/mcp/connect",
        headers=headers,
        json_body=connect_body,
    )


async def discover_domain_via_apollo_search(
    client: httpx.AsyncClient, api_key: str, organization_name: str
) -> str | None:
    query = clean_optional(organization_name)
    if not query:
        return None
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Api-Key": api_key,
        "Cache-Control": "no-cache",
    }
    best_domain = None
    best_score = -1
    for candidate_query in organization_name_variants(query):
        try:
            response = await client.post(
                "/api/v1/organizations/search",
                headers=headers,
                params={
                    "q_organization_name": candidate_query,
                    "page": 1,
                    "per_page": 8,
                },
            )
        except httpx.RequestError:
            continue

        if not response.is_success:
            continue

        parsed = parse_json(response)
        organizations = parsed.get("organizations")
        if not isinstance(organizations, list):
            organizations = parsed.get("accounts")
        if not isinstance(organizations, list) or not organizations:
            continue

        for item in organizations:
            if not isinstance(item, Mapping):
                continue
            derived = extract_domain_from_apollo_org(item)
            if derived:
                score = score_domain_for_organization(query, derived)
                if score > best_score:
                    best_score = score
                    best_domain = derived
    if best_domain and not await verify_domain_for_organization(client, query, best_domain):
        logger.info(
            "[domain:apollo] rejected unverified domain=%s for org=%s after verification",
            best_domain,
            query,
        )
        return None
    return best_domain


async def discover_apollo_domain(
    client: httpx.AsyncClient, api_key: str, organization_name: str
) -> str | None:
    query = clean_optional(organization_name)
    if not query:
        return None
    cached = get_cached_domain_for_org(query)
    if cached:
        logger.info("[domain] using cached domain=%s for org=%s", cached, query)
        return cached

    google_domain = await discover_domain_via_google_search_api(client, query)
    if google_domain:
        set_cached_domain_for_org(query, google_domain)
        logger.info("[domain] source=google domain=%s org=%s", google_domain, query)
        return google_domain

    brave_domain = await discover_domain_via_brave_search_api(client, query)
    if brave_domain:
        set_cached_domain_for_org(query, brave_domain)
        logger.info("[domain] source=brave domain=%s org=%s", brave_domain, query)
        return brave_domain

    apollo_domain = await discover_domain_via_apollo_search(client, api_key, query)
    if apollo_domain:
        set_cached_domain_for_org(query, apollo_domain)
        logger.info("[domain] source=apollo domain=%s org=%s", apollo_domain, query)
        return apollo_domain

    logger.info("[domain] no domain discovered for org=%s", query)
    return None


def organization_name_variants(name: str | None) -> list[str]:
    original = clean_optional(name)
    if not original:
        return []
    variants: list[str] = [original]
    without_parens = re.sub(r"\([^)]*\)", " ", original).strip()
    if without_parens:
        variants.append(compact_spaces(without_parens))
    normalized = re.sub(r"[^A-Za-z0-9& ]+", " ", original)
    normalized = compact_spaces(normalized)
    if normalized:
        variants.append(normalized)
    deduped: list[str] = []
    seen: set[str] = set()
    for variant in variants:
        key = normalize_text(variant)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(variant)
    return deduped


def organization_name_core_tokens(name: str | None) -> list[str]:
    tokens = split_query_tokens(name)
    stop = {
        "the",
        "and",
        "for",
        "city",
        "county",
        "department",
        "district",
        "school",
        "state",
        "office",
        "board",
        "authority",
    }
    core = [token for token in tokens if token not in stop]
    return core if core else tokens


def organization_acronym(name: str | None) -> str | None:
    raw = clean_optional(name)
    if not raw:
        return None
    parenthetical = re.findall(r"\(([^)]+)\)", raw)
    for item in parenthetical:
        token = re.sub(r"[^A-Za-z0-9]", "", item).lower()
        if len(token) >= 2:
            return token
    words = re.findall(r"[A-Za-z0-9]+", raw)
    if len(words) < 2:
        return None
    acronym = "".join(w[0] for w in words if w and w[0].isalnum()).lower()
    return acronym if len(acronym) >= 2 else None


def score_domain_for_organization(name: str | None, domain: str | None) -> int:
    host = derive_domain(domain)
    if not host:
        return -1
    root = host.split(".", 1)[0]
    root_norm = re.sub(r"[^a-z0-9]", "", root.lower())
    tokens = organization_name_core_tokens(name)
    acronym = organization_acronym(name)
    score = 0
    for token in tokens:
        if len(token) < 3:
            continue
        if token in root_norm:
            score += 3
        elif root_norm in token:
            score += 1
    if acronym and acronym in root_norm:
        score += 4
    return score


async def verify_domain_for_organization(
    client: httpx.AsyncClient, organization_name: str, domain: str
) -> bool:
    host = derive_domain(domain)
    if not host or not is_public_hostname(host):
        return False
    score = score_domain_for_organization(organization_name, host)
    if score >= 4:
        return True

    tokens = [t for t in organization_name_core_tokens(organization_name) if len(t) >= 4][:3]
    if not tokens:
        return score >= 2
    test_urls = [f"https://{host}", f"http://{host}"]
    for url in test_urls:
        try:
            resp = await client.get(url, follow_redirects=False, timeout=6.0)
        except httpx.RequestError:
            continue
        if resp.status_code in {301, 302, 303, 307, 308}:
            location = clean_optional(resp.headers.get("location"))
            if location:
                target = extract_redirect_target(location)
                target_host = derive_domain(target)
                if not target_host or not is_public_hostname(target_host):
                    continue
                try:
                    resp = await client.get(
                        f"https://{target_host}",
                        follow_redirects=False,
                        timeout=6.0,
                    )
                except httpx.RequestError:
                    continue
        if not resp.is_success:
            continue
        body = normalize_text(resp.text[:12000])
        hits = sum(1 for token in tokens if token in body)
        if hits >= 2:
            return True
    return False


def extract_redirect_target(url: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    for key in ("uddg", "u", "url", "target"):
        values = query.get(key)
        if values:
            candidate = clean_optional(unquote(values[0]))
            if candidate:
                return candidate
    return url


def domain_allowed_for_org_lookup(domain: str) -> bool:
    blocked_suffixes = (
        "duckduckgo.com",
        "google.com",
        "bing.com",
        "search.yahoo.com",
        "yahoo.com",
        "yandex.com",
        "baidu.com",
        "ecosia.org",
        "startpage.com",
        "linkedin.com",
        "facebook.com",
        "instagram.com",
        "x.com",
        "twitter.com",
        "youtube.com",
        "wikipedia.org",
        "zoominfo.com",
        "apollo.io",
        "crunchbase.com",
    )
    if any(domain.endswith(suffix) for suffix in blocked_suffixes):
        return False
    return is_public_hostname(domain)


def is_public_hostname(hostname: str | None) -> bool:
    host = clean_optional(hostname)
    if not host:
        return False
    host = host.lower().strip().strip(".")
    if host in {"localhost"}:
        return False
    if host.endswith(".local") or host.endswith(".localdomain") or host.endswith(".internal"):
        return False
    try:
        ip = ipaddress.ip_address(host)
        if ip.is_loopback or ip.is_private or ip.is_link_local or ip.is_reserved or ip.is_multicast:
            return False
        return True
    except ValueError:
        pass
    private_patterns = (".lan", ".home", ".corp", ".internal")
    if any(host.endswith(p) for p in private_patterns):
        return False
    return True


def extract_search_result_urls(html: str) -> list[str]:
    if not html:
        return []
    urls = re.findall(r'href="([^"]+)"', html, flags=re.IGNORECASE)
    out: list[str] = []
    seen: set[str] = set()
    for raw in urls:
        candidate = clean_optional(raw)
        if not candidate:
            continue
        if candidate.startswith("javascript:") or candidate.startswith("#"):
            continue
        if candidate.startswith("/"):
            # DDG result links are often relative and carry the real target in query params.
            candidate = extract_redirect_target(f"https://duckduckgo.com{candidate}")
        candidate = extract_redirect_target(candidate)
        if candidate.startswith("//"):
            candidate = f"https:{candidate}"
        if not candidate.startswith("http://") and not candidate.startswith("https://"):
            continue
        domain = derive_domain(candidate)
        if not domain:
            continue
        if domain.endswith("duckduckgo.com"):
            continue
        normalized = candidate.strip()
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


async def discover_domain_via_google_search_api(
    client: httpx.AsyncClient, organization_name: str
) -> str | None:
    api_key = clean_optional(os.getenv("GOOGLE_SEARCH_API_KEY"))
    cx = clean_optional(os.getenv("GOOGLE_SEARCH_ENGINE_ID"))
    if not api_key or not cx:
        return None
    queries = []
    for variant in organization_name_variants(organization_name):
        queries.append(f"{variant} official site")
        queries.append(f"{variant} website")
    seen_queries: set[str] = set()
    deduped_queries: list[str] = []
    for query in queries:
        key = normalize_text(query)
        if not key or key in seen_queries:
            continue
        seen_queries.add(key)
        deduped_queries.append(query)

    checked_domains: set[str] = set()
    for query in deduped_queries[:3]:
        if not quota_can_use("google"):
            logger.info("[domain:google] quota reached daily_limit=%s", GOOGLE_SEARCH_DAILY_LIMIT)
            break
        quota_register_use("google")
        try:
            resp = await client.get(
                "https://www.googleapis.com/customsearch/v1",
                params={
                    "key": api_key,
                    "cx": cx,
                    "q": query,
                    "num": 5,
                },
                timeout=8.0,
            )
        except httpx.RequestError:
            continue
        if not resp.is_success:
            continue
        payload = parse_json(resp)
        items = payload.get("items")
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, Mapping):
                continue
            url = clean_optional(str(item.get("link") or ""))
            domain = derive_domain(url)
            if not domain or not domain_allowed_for_org_lookup(domain):
                continue
            if domain in checked_domains:
                continue
            checked_domains.add(domain)
            if await verify_domain_for_organization(client, organization_name, domain):
                return domain
    return None


async def discover_domain_via_brave_search_api(
    client: httpx.AsyncClient, organization_name: str
) -> str | None:
    api_key = clean_optional(os.getenv("BRAVE_SEARCH_API_KEY"))
    if not api_key:
        return None
    queries = []
    for variant in organization_name_variants(organization_name):
        queries.append(f"{variant} official site")
        queries.append(f"{variant} website")
    seen_queries: set[str] = set()
    deduped_queries: list[str] = []
    for query in queries:
        key = normalize_text(query)
        if not key or key in seen_queries:
            continue
        seen_queries.add(key)
        deduped_queries.append(query)

    checked_domains: set[str] = set()
    for query in deduped_queries[:3]:
        if not quota_can_use("brave"):
            logger.info("[domain:brave] quota reached monthly_limit=%s", BRAVE_SEARCH_MONTHLY_LIMIT)
            break
        quota_register_use("brave")
        try:
            resp = await client.get(
                "https://api.search.brave.com/res/v1/web/search",
                params={"q": query, "count": 10},
                headers={"Accept": "application/json", "X-Subscription-Token": api_key},
                timeout=8.0,
            )
        except httpx.RequestError:
            continue
        if not resp.is_success:
            continue
        payload = parse_json(resp)
        web = payload.get("web")
        if not isinstance(web, Mapping):
            continue
        results = web.get("results")
        if not isinstance(results, list):
            continue
        for result in results:
            if not isinstance(result, Mapping):
                continue
            url = clean_optional(str(result.get("url") or ""))
            domain = derive_domain(url)
            if not domain or not domain_allowed_for_org_lookup(domain):
                continue
            if domain in checked_domains:
                continue
            checked_domains.add(domain)
            if await verify_domain_for_organization(client, organization_name, domain):
                return domain
    return None


async def discover_domain_via_web_search(client: httpx.AsyncClient, organization_name: str) -> str | None:
    # Legacy wrapper retained for compatibility.
    domain = await discover_domain_via_google_search_api(client, organization_name)
    if domain:
        return domain
    return await discover_domain_via_brave_search_api(client, organization_name)


def extract_domain_from_apollo_org(item: Mapping[str, Any]) -> str | None:
    candidate_keys = (
        "primary_domain",
        "domain",
        "website_url",
        "website",
    )
    for key in candidate_keys:
        value = item.get(key)
        if not value:
            continue
        text = clean_optional(str(value))
        if not text:
            continue
        if "." in text and "://" not in text and "/" not in text:
            return text.lower()
        derived = derive_domain(text)
        if derived:
            return derived

    org_obj = item.get("organization")
    if isinstance(org_obj, Mapping):
        for key in candidate_keys:
            value = org_obj.get(key)
            if not value:
                continue
            text = clean_optional(str(value))
            if not text:
                continue
            if "." in text and "://" not in text and "/" not in text:
                return text.lower()
            derived = derive_domain(text)
            if derived:
                return derived
    return None


def pick_best_apollo_org_match(
    organizations: list[Any], org_name: str | None, org_domain: str | None
) -> tuple[Mapping[str, Any] | None, int, list[str]]:
    best_score = -1
    best_reasons: list[str] = []
    best: Mapping[str, Any] | None = None
    query_tokens = token_set(org_name)
    for org in organizations:
        if not isinstance(org, Mapping):
            continue
        score = 0
        reasons: list[str] = []
        domain = extract_domain_from_apollo_org(org)
        name = clean_optional(str(org.get("name") or org.get("organization_name") or ""))
        if org_domain and domain and domain == org_domain:
            score += 12
            reasons.append("domain_exact")
        if org_domain and domain and domain.startswith(org_domain):
            score += 2
            reasons.append("domain_prefix")
        if org_name and name and normalize_text(name) == normalize_text(org_name):
            score += 8
            reasons.append("name_exact")
        elif org_name and text_contains(name, org_name):
            score += 4
            reasons.append("name_contains")
        name_tokens = token_set(name)
        overlap = len(query_tokens.intersection(name_tokens))
        if overlap:
            score += min(4, overlap)
            reasons.append(f"token_overlap:{overlap}")
        if org_domain and domain and org_domain in domain:
            score += 1
            reasons.append("domain_contains")
        if score > best_score:
            best_score = score
            best_reasons = reasons
            best = org
    return best, max(0, best_score), best_reasons


def first_non_empty(mapping: Mapping[str, Any], keys: list[str]) -> str | None:
    for key in keys:
        value = clean_optional(str(mapping.get(key) or ""))
        if value:
            return value
    return None


def build_apollo_snapshot(
    basic: Mapping[str, Any], detailed: Mapping[str, Any] | None
) -> dict[str, Any]:
    source = detailed if detailed else basic
    basic_name = clean_optional(str(basic.get("name") or basic.get("organization_name") or ""))
    source_name = clean_optional(str(source.get("name") or source.get("organization_name") or ""))
    domain = extract_domain_from_apollo_org(source) or extract_domain_from_apollo_org(basic)
    city = first_non_empty(source, ["city", "primary_city"])
    state = first_non_empty(source, ["state", "primary_state"])
    country = first_non_empty(source, ["country", "primary_country"])
    hq_parts = [part for part in [city, state, country] if part]
    tech_stack = source.get("technologies") or source.get("technology_names") or source.get("keywords")
    if not isinstance(tech_stack, list):
        tech_stack = []
    technologies = [clean_optional(str(item)) for item in tech_stack if clean_optional(str(item))]
    technologies = rank_tech_stack_for_ae(technologies)
    return {
        "apollo_org_id": clean_optional(str(source.get("id") or basic.get("id") or "")),
        "name": source_name or basic_name,
        "domain": domain,
        "industry": first_non_empty(source, ["industry", "industry_tag"]),
        "estimated_num_employees": first_non_empty(
            source,
            [
                "estimated_num_employees",
                "num_employees",
                "employee_count",
                "estimated_employee_count",
            ],
        ),
        "annual_revenue": first_non_empty(source, ["annual_revenue", "estimated_annual_revenue"]),
        "city": city,
        "state": state,
        "country": country,
        "hq_location": ", ".join(hq_parts) if hq_parts else None,
        "tech_stack": technologies[:12],
        "linkedin_url": first_non_empty(source, ["linkedin_url"]),
        "website_url": first_non_empty(source, ["website_url", "website"]),
    }


def tech_priority_score(name: str) -> int:
    text = normalize_text(name)
    if not text:
        return 0
    token_scores = {
        # 1) ITSM/ESM platform signal (highest relevance)
        "servicenow": 200,
        "jira service management": 188,
        "jira service desk": 186,
        "bmc helix": 184,
        "bmc remedy": 182,
        "remedy": 180,
        "ivanti": 176,
        "cherwell": 174,
        "freshservice": 172,
        "manageengine": 170,
        "service desk": 166,
        "itsm": 164,
        # 2) ITOM/observability/CMDB feeder stack
        "splunk": 160,
        "datadog": 158,
        "dynatrace": 156,
        "new relic": 154,
        "solarwinds": 152,
        "logicmonitor": 150,
        "appdynamics": 148,
        "grafana": 146,
        "prometheus": 144,
        "scom": 142,
        # 3) Security + identity stack
        "okta": 140,
        "entra": 138,
        "azure ad": 138,
        "active directory": 136,
        "sailpoint": 134,
        "cyberark": 132,
        "crowdstrike": 130,
        "defender": 128,
        "mimecast": 127,
        "sophos": 126,
        # 4) Cloud + infra estate
        "amazon aws": 126,
        "aws": 126,
        "azure": 124,
        "google cloud": 122,
        "gcp": 122,
        "vmware": 120,
        "kubernetes": 118,
        "openshift": 116,
        # 5) DevOps chain
        "github": 114,
        "gitlab": 112,
        "azure devops": 110,
        "jenkins": 108,
        "terraform": 106,
        "ansible": 104,
        # 6) Systems of record/integration anchors
        "salesforce": 102,
        "sap": 100,
        "oracle": 100,
        "workday": 98,
        "microsoft dynamics": 96,
        "hubspot": 94,
        # 7) endpoint/collab/email (supporting)
        "microsoft office 365": 90,
        "office 365": 90,
        "outlook": 88,
        "microsoft teams": 86,
        "slack": 84,
        "intune": 82,
        "sccm": 80,
    }
    for token, score in token_scores.items():
        if token in text:
            return score

    if any(token in text for token in ["cmdb", "incident", "change management", "asset management"]):
        return 92
    if any(token in text for token in ["iam", "siem", "soc", "xdr", "edr", "mdm"]):
        return 89
    if any(token in text for token in ["crm", "erp", "hris", "data warehouse", "analytics"]):
        return 74
    if any(
        token in text
        for token in [
            "doubleclick",
            "remarketing",
            "google tag manager",
            "facebook widget",
            "facebook login",
            "linkedin widget",
            "linkedin login",
            "linkedin marketing",
            "adform",
            "vimeo",
            "mobile friendly",
            "bootstrap",
            "cdn",
            "dns",
            "remote",
        ]
    ):
        return 18
    if any(token in text for token in ["dns", "cdn", "tag manager", "adform", "bootstrap"]):
        return 28
    return 50


def rank_tech_stack_for_ae(technologies: list[str | None]) -> list[str]:
    seen: set[str] = set()
    rows: list[tuple[int, str, str]] = []
    for raw in technologies:
        name = clean_optional(str(raw or ""))
        if not name:
            continue
        key = normalize_text(name)
        if not key or key in seen:
            continue
        seen.add(key)
        rows.append((tech_priority_score(name), key, name))
    rows.sort(key=lambda row: (-row[0], row[1]))
    return [name for _, _, name in rows]


def split_name(full_name: str) -> tuple[str, str]:
    tokens = compact_spaces(full_name).split(" ")
    if not tokens:
        return "", ""
    if len(tokens) == 1:
        return tokens[0], ""
    return tokens[0], " ".join(tokens[1:])


def extract_apollo_phone(person: Mapping[str, Any]) -> str | None:
    direct_candidates = [
        person.get("phone"),
        person.get("sanitized_phone"),
        person.get("mobile_phone"),
        person.get("direct_dial"),
    ]
    for value in direct_candidates:
        resolved = clean_optional(str(value or ""))
        if resolved:
            return resolved

    phone_numbers = person.get("phone_numbers")
    if isinstance(phone_numbers, list):
        for item in phone_numbers:
            if isinstance(item, Mapping):
                for key in ("sanitized_number", "number", "raw_number", "value"):
                    resolved = clean_optional(str(item.get(key) or ""))
                    if resolved:
                        return resolved
            else:
                resolved = clean_optional(str(item or ""))
                if resolved:
                    return resolved
    return None


def infer_prospects_from_context(payload: GrantCampaignGenerateRequest) -> list[ProvidedProspect]:
    text_sources = [payload.award.description or ""]
    for evidence in payload.evidence:
        if evidence.excerpt:
            text_sources.append(evidence.excerpt)
    text = " ".join(text_sources)
    if not text:
        return []

    # Example matched: Kuru Mathew (CIO)
    pattern = re.compile(
        r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s*\(([^)]+)\)",
    )
    title_signal = re.compile(
        r"\b(CIO|CISO|CTO|CFO|COO|CEO|Chief|Director|Secretary|Deputy|Program Manager|Head)\b",
        re.IGNORECASE,
    )
    found: list[ProvidedProspect] = []
    seen: set[str] = set()
    for match in pattern.finditer(text):
        full_name = compact_spaces(match.group(1))
        title = compact_spaces(match.group(2))
        if not title_signal.search(title):
            continue
        key = normalize_key(full_name)
        if not key or key in seen:
            continue
        seen.add(key)
        found.append(
            ProvidedProspect(
                full_name=full_name,
                title=title,
                organization=payload.organization.name,
                linkedin_url=None,
                note="Provided in the initiative context supplied for this campaign.",
            )
        )
    return found[: payload.constraints.max_recipients]


async def generate_recipient_strategy(
    payload: GrantCampaignGenerateRequest,
    api_key: str,
    model: str,
    request_id: str,
    prospect_briefs: list[ProspectBrief],
    cost_tracker: dict[str, Any],
) -> list[GrantRecipient]:
    if payload.prospects:
        return prospects_to_recipients(payload, prospect_briefs)

    role_discovery_max = payload.constraints.max_recipients
    system_prompt = (
        "You build B2G/B2SLED sales targeting strategy. Return JSON only. "
        "Create recipient targets for consulting implementation outreach. "
        "You must avoid inventing named individuals. If no named prospects are provided, output role titles only. "
        "Select up to max_recipients roles most likely to drive implementation decisions."
    )
    user_prompt = {
        "task": "Create top recipient personas for outreach",
        "max_recipients": role_discovery_max,
        "organization": payload.organization.model_dump(),
        "award": payload.award.model_dump(mode="json"),
        "evidence": [item.model_dump(mode="json") for item in payload.evidence],
        "rules": [
            "Do not invent person names.",
            "Prefer exact role titles tied to grant execution, procurement, security, IT, and operations.",
            "Each rationale must mention why this role influences implementation contract decisions.",
            "No duplicate recipients.",
            "No role-definition language (avoid 'this role oversees...').",
            "Prioritize decision authority + execution ownership.",
        ],
        "required_output_schema": {
            "recipients": [
                {
                    "label": "role title",
                    "persona": "short persona name",
                    "rationale": "why this role should receive the campaign",
                }
            ]
        },
    }
    content = await openai_json_completion(
        api_key=api_key,
        model=model,
        system_prompt=system_prompt,
        user_payload=user_prompt,
        temperature=0.2,
        stage="recipient_strategy",
        request_id=request_id,
        cost_tracker=cost_tracker,
    )
    recipients_raw = content.get("recipients")
    if not isinstance(recipients_raw, list):
        raise RuntimeError("Grant campaign generation failed: missing recipients output.")

    recipients: list[GrantRecipient] = []
    for item in recipients_raw[:role_discovery_max]:
        if not isinstance(item, Mapping):
            continue
        try:
            recipients.append(GrantRecipient.model_validate(item))
        except ValidationError:
            continue
    if not recipients:
        recipients = [
            GrantRecipient(
                label="Program Director, AI Election Lab",
                persona="Program Leadership",
                rationale="Owns implementation delivery, rollout sequencing, and accountability for project milestones.",
            ),
            GrantRecipient(
                label="Procurement Lead",
                persona="Acquisition",
                rationale="Controls contract pathway, vendor selection timing, and buying process for implementation support.",
            ),
        ]
    return dedupe_recipients(recipients, role_discovery_max)


async def generate_email_campaign(
    payload: GrantCampaignGenerateRequest,
    recipients: list[GrantRecipient],
    prospect_briefs: list[ProspectBrief],
    project_research: ProjectResearchBrief,
    api_key: str,
    model: str,
    request_id: str,
    cost_tracker: dict[str, Any],
) -> GrantCampaign:
    cri_credibility = get_cri_credibility_ammo()
    system_prompt = (
        "You are an enterprise sales coach generating concise outbound campaigns for SLED organizations. "
        "Return JSON only. Keep claims grounded in provided input. "
        "Output high-conversion, outcome-oriented emails focused on implementation execution. "
        "Never write generic summaries that could apply to any agency. "
        "Use implementation-partner tone, not advisory/problem-education tone."
    )
    user_prompt = {
        "task": "Generate one original 4-email outreach sequence per recipient for grant-funded implementation consulting",
        "constraints": {
            "email_count": 4,
            "no_em_dash": True,
            "tone": "professional, conversational, practical",
            "f_pattern": True,
            "short_scannable_copy": True,
            "max_words_per_email": 90,
        },
        "organization": payload.organization.model_dump(),
        "award": payload.award.model_dump(mode="json"),
        "evidence": [item.model_dump(mode="json") for item in payload.evidence],
        "project_research": project_research.model_dump(mode="json"),
        "cri_credibility_ammo": cri_credibility,
        "recipients": [recipient.model_dump() for recipient in recipients],
        "prospect_briefs": [brief.model_dump(mode="json") for brief in prospect_briefs],
        "prospects_supplied": bool(payload.prospects),
        "sparse_project_signal_inputs": has_sparse_project_signal_inputs(payload),
        "provided_prospects": [p.model_dump(mode="json") for p in payload.prospects],
        "rules": [
            "Do not repeat campaign title or summary content.",
            "Do not restate identical paragraphs.",
            "If recipients are provided prospects, keep those recipients only.",
            "Create one unique 4-email sequence for every recipient in recipients[].",
            "Every sequence must be tailored to that recipient's role and influence over this project.",
            "Use person-level signals as natural hooks. Do not write generic role definitions.",
            "Each sequence should feel original, not a lightly reworded template.",
            "Do NOT lecture recipients on fundamentals they already know (avoid baseline statements like 'HITL controls are non-negotiable').",
            "Write in natural AE voice: first-person singular/plural is allowed ('I saw', 'I was reading', 'we can walk through', 'we'd be happy to').",
            "Use conversational transitions that still sound professional, not robotic or policy-memo style.",
            "Short paragraphs with clear spacing for copy/paste into outbound tools.",
            "Open Email 1 by referencing the recipient's mandate/initiative ownership from research or provided context.",
            "Then pivot immediately to CRI as implementation partner and what execution support we provide.",
            "Assume no blockers; position discovery of blockers for a scope meeting.",
            "When prospects are supplied, strategy_summary must mention at least one provided person by name.",
            "When prospects are supplied, avoid role definitions as copy (for example: 'the CIO oversees...'). Focus on implementation outcomes and why the named person is relevant now.",
            "When prospects are supplied, each recipient sequence must include that recipient name and title in Email 1 or Email 2.",
            "Each recipient sequence should reference at least one concrete project-specific fact from project_research or source-backed prospect signals.",
            "When prospects are NOT supplied, keep recipients to at most max_recipients roles and write project-specific messaging without defining what the role does in general terms.",
            "strategy_summary must read like an internal management brief: what we are pitching, why now, who owns decision/execution, and expected implementation outcomes.",
            "Keep strategy_summary concise and executive-ready for PDF sharing internally.",
            "Use 1 concise CRI credibility proof point in each email from cri_credibility_ammo.",
            "Use CTA language as 'working session' or 'meeting to review scope'. Do not use a fixed time cap like 20 minutes.",
            "Do not make up facts not present in research or provided input.",
            "Anchor messaging on outcomes: governance, HITL controls, risk reduction, deployment readiness, and cross-office execution.",
            "Avoid vague phrasing like 'consulting support' without specific implementation focus.",
            "Do not use generic praise/filler phrasing such as 'your leadership is pivotal', 'your oversight is essential', or similar.",
            "Do not repeat the recipient name in body lines after the greeting (avoid patterns like 'Hi Kuru' followed by 'Kuru, ...').",
            "If a recipient has source-backed signals in prospect_briefs, reference at least one of those signals in Email 1 naturally.",
            "If source-backed recipient signals are unavailable, anchor Email 1 to a concrete project fact from project_research evidence.",
            "Never output 'Inferred from provided campaign context.' inside email bodies.",
            "Email 1 first sentence should feel human and direct; mandate language is preferred but not required verbatim.",
            "Avoid abstract leadership praise or motivational language.",
            "If recipient research_confidence is LOW, do not use biography-style claims. Use only provided role + project facts from project_research.",
            "Do not imply committee membership, speaking roles, or achievements unless explicitly present in that recipient's source-backed signals.",
            "Do not include speculative execution claims such as hiring plans, resource allocation, personnel onboarding, or partnership development unless explicitly source-backed.",
            "Every email must anchor at least one sentence in a concrete project fact or recipient signal from provided inputs.",
            "Keep each email body concise and under 90 words.",
            "When sparse_project_signal_inputs is true, treat initiative text as shorthand context. Do not assert recipient or organization ownership of that initiative.",
            "When sparse_project_signal_inputs is true, use conditional language like 'If <initiative> is a current priority' instead of 'your <initiative>'.",
        ],
        "email_framework": {
            "email_1": [
                "Line 1: recipient mandate/initiative ownership",
                "Line 2: CRI implementation credibility proof",
                "Line 3: what implementation process we can walk through",
                "Line 4: ask for working session or meeting",
            ],
            "email_2_to_4": [
                "advance implementation specifics",
                "include one concrete proof point",
                "close with meeting/scope review CTA",
            ],
        },
        "required_output_schema": {
            "campaign_title": "string",
            "strategy_summary": "string",
            "recipients": [
                {"label": "string", "persona": "string", "rationale": "string"}
            ],
            "prospect_campaigns": [
                {
                    "recipient_label": "string",
                    "recipient_persona": "string",
                    "recipient_rationale": "string",
                    "emails": [
                        {
                            "email_number": 1,
                            "subject": "string",
                            "body": "string",
                        }
                    ],
                }
            ],
        },
    }
    content = await openai_json_completion(
        api_key=api_key,
        model=model,
        system_prompt=system_prompt,
        user_payload=user_prompt,
        temperature=0.35,
        stage="email_campaign",
        request_id=request_id,
        cost_tracker=cost_tracker,
    )
    try:
        campaign = GrantCampaign.model_validate(content)
    except ValidationError as error:
        raise RuntimeError(
            f"Grant campaign generation failed: invalid model output ({error})."
        ) from error
    if not campaign.prospect_campaigns:
        raise RuntimeError("Grant campaign generation failed: missing per-recipient sequences.")
    return campaign


def get_cri_credibility_ammo() -> dict[str, Any]:
    return {
        "approved_statements": [
            "Three-time ServiceNow Partner of the Year winner (2024, 2025, 2026).",
            "ServiceNow 2024 Consulting and Implementation Partner of the Year (Premier segment, Americas).",
            "ServiceNow 2025 Reseller Partner of the Year (Specialist segment, Americas).",
            "ServiceNow 2026 Partner of the Year: Reseller Rising Star Americas.",
            "ServiceNow Elite Partner status (announced September 2025).",
            "Over 35 years of IT services experience.",
            "Over 700 customers supported across public and commercial sectors.",
            "Federal support includes U.S. Department of Energy and U.S. Department of Homeland Security programs.",
            "Commercial support includes Fortune 500 clients such as Hewlett Packard and General Electric.",
        ],
        "use_rules": [
            "Pick one or two proof points that best match recipient role and campaign context.",
            "Prefer public-sector proof points for SLED recipients.",
            "Keep credibility line short and specific.",
            "You may use the explicit three-time statement when it fits naturally.",
        ],
        "source_urls": [
            "https://criadvantage.com/2025-servicenow-partner-of-the-year-award-winner/",
            "https://criadvantage.com/cri-advantage-honored-as-servicenows-consulting-and-implementation-partner-of-the-year-premier-segment-americas-region/",
            "https://criadvantage.com/cri-advantage-servicenow-elite-partner/",
            "https://criadvantage.com/about-us/",
            "https://criadvantage.com/about-us/clients/",
            "https://criadvantage.com/service-now/grc/",
        ],
    }


def prospects_to_recipients_with_briefs(
    payload: GrantCampaignGenerateRequest, briefs: list[ProspectBrief]
) -> list[GrantRecipient]:
    brief_map = {normalize_key(b.full_name): b for b in briefs}
    recipients: list[GrantRecipient] = []
    for prospect in payload.prospects[: payload.constraints.max_recipients]:
        label = prospect.full_name or "Named Prospect"
        if prospect.title:
            label = f"{label} ({prospect.title})"
        persona = infer_persona_from_title(prospect.title)
        base_rationale = (
            "Named target provided by user for this campaign."
            if not prospect.note
            else prospect.note
        )
        if base_rationale.lower().startswith("inferred from provided campaign context"):
            base_rationale = "Named in provided initiative context."
        rationale = base_rationale
        recipients.append(GrantRecipient(label=label, persona=persona, rationale=rationale))
    return dedupe_recipients(recipients, payload.constraints.max_recipients)


def prospects_to_recipients(
    payload: GrantCampaignGenerateRequest, briefs: list[ProspectBrief]
) -> list[GrantRecipient]:
    return prospects_to_recipients_with_briefs(payload, briefs)


async def generate_prospect_briefs(
    payload: GrantCampaignGenerateRequest,
    api_key: str,
    model: str,
    request_id: str,
    cost_tracker: dict[str, Any],
) -> list[ProspectBrief]:
    if not payload.prospects:
        return []

    if should_skip_prospect_web_search(payload):
        logger.info(
            "[grant:%s] prospect research web search skipped: sufficient provided context",
            request_id,
        )
        return merge_context_signals_into_prospect_briefs(payload, [])

    system_prompt = (
        "You are a B2G prospect enrichment analyst. Use web search for person-specific professional signals. "
        "Return JSON only."
    )
    user_prompt = {
        "task": "Research provided prospects and produce concise, evidence-backed personalization briefs",
        "organization": payload.organization.model_dump(),
        "award": payload.award.model_dump(mode="json"),
        "prospects": [p.model_dump(mode="json") for p in payload.prospects],
        "query_plan": [
            "\"<full_name>\" \"<organization name>\"",
            "\"<full_name>\" \"<organization name>\" LinkedIn",
            "site:linkedin.com/in \"<full_name>\" \"<organization name>\"",
            "site:azsos.gov \"<full_name>\"",
            "site:azsos.gov \"<full_name>\" \"Chief Information\" OR \"Chief Information Security\"",
            "site:azsos.gov/file* \"<full_name>\"",
            "\"<full_name>\" \"<organization name>\" interview OR podcast OR speaker OR quote OR op-ed",
            "site:<organization domain if known> \"<full_name>\"",
        ],
        "rules": [
            "Preserve prospect order from input.",
            "Never invent people or titles.",
            "If ambiguous identity, set research_confidence LOW and signals empty.",
            "Signal facts must be specific and attributable, not generic biography filler.",
            "Every signal must include a real source_url.",
            "At most 2 signals per person.",
            "If no source-backed person-specific signal is found, leave signals empty and set research_confidence LOW.",
            "Keep output compact: no verbose summaries; prioritize concrete, attributable person facts only.",
        ],
        "required_output_schema": {
            "prospects": [
                {
                    "full_name": "string",
                    "title": "string",
                    "organization": "string",
                    "linkedin_url": "string",
                    "research_confidence": "HIGH|MEDIUM|LOW",
                    "signals": [{"fact": "string", "source_url": "string"}],
                    "personalization_angle": "string",
                }
            ]
        },
    }
    try:
        content = await openai_responses_json_completion(
            api_key=api_key,
            model=model,
            system_prompt=system_prompt,
            user_payload=user_prompt,
            temperature=0.2,
            stage="prospect_research",
            request_id=request_id,
            cost_tracker=cost_tracker,
            tools=[
                {
                    "type": "web_search_preview",
                    "search_context_size": "medium",
                }
            ],
        )
    except RuntimeError as error:
        logger.warning(
            "[grant:%s] prospect research fallback triggered: %s",
            request_id,
            error,
        )
        return []

    prospects_raw = content.get("prospects")
    if not isinstance(prospects_raw, list):
        return []

    briefs: list[ProspectBrief] = []
    for item in prospects_raw[: payload.constraints.max_recipients]:
        if not isinstance(item, Mapping):
            continue
        try:
            briefs.append(ProspectBrief.model_validate(item))
        except ValidationError:
            continue
    return merge_context_signals_into_prospect_briefs(payload, briefs)


async def generate_project_research(
    payload: GrantCampaignGenerateRequest,
    api_key: str,
    model: str,
    request_id: str,
    cost_tracker: dict[str, Any],
) -> ProjectResearchBrief:
    system_prompt = (
        "You are a public-sector project research analyst. "
        "Build an implementation-focused brief from public evidence. Return JSON only."
    )
    user_prompt = {
        "task": "Research the project context for grant/initiative implementation outreach",
        "organization": payload.organization.model_dump(),
        "award": payload.award.model_dump(mode="json"),
        "evidence": [item.model_dump(mode="json") for item in payload.evidence],
        "rules": [
            "Use concrete, attributable facts only.",
            "Focus on timeline pressure, governance requirements, execution risks, and decision triggers.",
            "Do not invent facts; if uncertain, omit.",
            "Include 2-3 evidence facts with source URLs where possible.",
            "Keep urgency_drivers, implementation_risks, and decision_triggers to 2 items each.",
            "Keep project_summary to 2-4 concise sentences.",
        ],
        "required_output_schema": {
            "project_summary": "string",
            "urgency_drivers": ["string"],
            "implementation_risks": ["string"],
            "decision_triggers": ["string"],
            "evidence": [{"fact": "string", "source_url": "string"}],
        },
    }
    if should_skip_project_web_search(payload):
        logger.info(
            "[grant:%s] project research web search skipped: sufficient provided evidence",
            request_id,
        )
        try:
            content = await openai_json_completion(
                api_key=api_key,
                model=model,
                system_prompt=system_prompt,
                user_payload=user_prompt,
                temperature=0.2,
                stage="project_research_no_web",
                request_id=request_id,
                cost_tracker=cost_tracker,
            )
            return trim_project_research(ProjectResearchBrief.model_validate(content))
        except Exception as error:
            logger.warning(
                "[grant:%s] project research no-web fallback failed: %s",
                request_id,
                error,
            )
            # Continue to regular fallback below.

    try:
        content = await openai_responses_json_completion(
            api_key=api_key,
            model=model,
            system_prompt=system_prompt,
            user_payload=user_prompt,
            temperature=0.2,
            stage="project_research",
            request_id=request_id,
            cost_tracker=cost_tracker,
            tools=[
                {
                    "type": "web_search_preview",
                    "search_context_size": "medium",
                }
            ],
        )
        return trim_project_research(ProjectResearchBrief.model_validate(content))
    except Exception as error:
        logger.warning("[grant:%s] project research fallback: %s", request_id, error)
        return ProjectResearchBrief(
            project_summary=(
                "Initiative requires coordinated AI governance implementation across stakeholders "
                "with clear controls, execution ownership, and timeline discipline."
            ),
            urgency_drivers=["Program timeline and public accountability expectations."],
            implementation_risks=["Fragmented ownership across teams and jurisdictions."],
            decision_triggers=["Need for auditable controls and implementation readiness."],
            evidence=[],
        )


async def discover_organization_prospects(
    payload: GrantCampaignGenerateRequest,
    api_key: str,
    model: str,
    request_id: str,
    cost_tracker: dict[str, Any],
) -> list[ProvidedProspect]:
    system_prompt = (
        "You are a public-sector prospecting researcher. "
        "Find real named decision-makers for implementation consulting outreach. "
        "Prioritize official organization team/leadership pages first, then reputable public sources. "
        "Return JSON only."
    )
    user_prompt = {
        "task": "Discover named prospects for SLED grant implementation outreach",
        "max_recipients": payload.constraints.max_recipients,
        "organization": payload.organization.model_dump(),
        "award": payload.award.model_dump(mode="json"),
        "evidence": [item.model_dump(mode="json") for item in payload.evidence],
        "discovery_order": [
            "official organization leadership/team pages",
            "official organization department pages",
            "official state/county directories",
            "linkedin and reputable media",
        ],
        "role_priority": [
            "CIO / IT leadership",
            "CISO / security leadership",
            "Program/elections operations leadership",
            "Procurement / contracts leadership",
            "Deputy/Executive sponsor",
        ],
        "rules": [
            "Return named people only. No generic role placeholders.",
            "Do not invent names or titles.",
            "If confidence is low for a person, exclude them.",
            "Prefer currently serving leaders tied to this project scope.",
        ],
        "required_output_schema": {
            "prospects": [
                {
                    "full_name": "string",
                    "title": "string",
                    "organization": "string",
                    "linkedin_url": "string|null",
                    "note": "string",
                }
            ]
        },
    }
    try:
        content = await openai_responses_json_completion(
            api_key=api_key,
            model=model,
            system_prompt=system_prompt,
            user_payload=user_prompt,
            temperature=0.2,
            stage="prospect_discovery",
            request_id=request_id,
            cost_tracker=cost_tracker,
            tools=[
                {
                    "type": "web_search_preview",
                    "search_context_size": "medium",
                }
            ],
        )
    except RuntimeError as error:
        logger.warning(
            "[grant:%s] prospect discovery failed: %s",
            request_id,
            error,
        )
        return []

    raw = content.get("prospects")
    if not isinstance(raw, list):
        return []

    discovered: list[ProvidedProspect] = []
    seen: set[str] = set()
    for item in raw[: payload.constraints.max_recipients]:
        if not isinstance(item, Mapping):
            continue
        try:
            candidate = ProvidedProspect.model_validate(item)
        except ValidationError:
            continue
        full_name = clean_optional(candidate.full_name)
        title = clean_optional(candidate.title)
        if not full_name or not title:
            continue
        key = normalize_key(full_name)
        if not key or key in seen:
            continue
        seen.add(key)
        discovered.append(
            ProvidedProspect(
                full_name=full_name,
                title=title,
                organization=clean_optional(candidate.organization)
                or payload.organization.name,
                linkedin_url=clean_optional(candidate.linkedin_url),
                note=clean_optional(candidate.note)
                or "Discovered automatically from public leadership sources.",
            )
        )
    return discovered


def infer_persona_from_title(title: str | None) -> str:
    t = (title or "").lower()
    if any(k in t for k in ["security", "ciso", "cyber"]):
        return "Security"
    if any(k in t for k in ["cio", "cto", "it", "technology", "data"]):
        return "IT Leadership"
    if any(k in t for k in ["procurement", "acquisition", "contract"]):
        return "Procurement"
    if any(k in t for k in ["elections", "program", "operations"]):
        return "Program Operations"
    if any(k in t for k in ["deputy", "chief", "director", "secretary"]):
        return "Executive Leadership"
    return "Stakeholder"


def dedupe_recipients(
    recipients: list[GrantRecipient], max_recipients: int
) -> list[GrantRecipient]:
    seen: set[str] = set()
    cleaned: list[GrantRecipient] = []
    for recipient in recipients:
        key = normalize_key(recipient.label)
        if not key or key in seen:
            continue
        seen.add(key)
        cleaned.append(
            GrantRecipient(
                label=compact_spaces(recipient.label),
                persona=compact_spaces(recipient.persona),
                rationale=compact_spaces(recipient.rationale),
            )
        )
        if len(cleaned) >= max_recipients:
            break
    return cleaned


def sanitize_campaign(
    campaign: GrantCampaign,
    payload: GrantCampaignGenerateRequest,
    recipients: list[GrantRecipient],
) -> GrantCampaign:
    summary = compact_spaces(campaign.strategy_summary)
    title = compact_spaces(campaign.campaign_title)
    if summary.lower().startswith(title.lower()):
        summary = summary[len(title) :].strip(" :.-")

    cleaned_sequences = sanitize_prospect_sequences(
        campaign.prospect_campaigns, recipients
    )

    final_recipients = recipients if payload.prospects else dedupe_recipients(campaign.recipients, payload.constraints.max_recipients)

    sanitized = GrantCampaign(
        campaign_title=title,
        strategy_summary=summary,
        recipients=final_recipients,
        prospect_campaigns=cleaned_sequences,
    )
    if payload.prospects:
        sanitized = ensure_named_prospect_copy(sanitized, payload)
    return sanitized


def trim_project_research(brief: ProjectResearchBrief) -> ProjectResearchBrief:
    return ProjectResearchBrief(
        project_summary=compact_spaces(brief.project_summary),
        urgency_drivers=[compact_spaces(x) for x in brief.urgency_drivers[:2]],
        implementation_risks=[compact_spaces(x) for x in brief.implementation_risks[:2]],
        decision_triggers=[compact_spaces(x) for x in brief.decision_triggers[:2]],
        evidence=brief.evidence[:3],
    )


def should_skip_project_web_search(payload: GrantCampaignGenerateRequest) -> bool:
    has_description = bool(payload.award.description and len(payload.award.description) > 80)
    good_evidence = sum(1 for e in payload.evidence if (e.excerpt and len(e.excerpt) > 40) or (e.url and e.url != "N/A"))
    return has_description and good_evidence >= 2




def sparse_project_signal_text(value: str | None) -> bool:
    text = compact_spaces(value or "")
    if not text:
        return False
    tokens = [t for t in re.split(r"[^a-z0-9]+", normalize_text(text)) if t]
    if len(tokens) <= 3:
        return True

    generic_nouns = {
        "implementation",
        "rollout",
        "deployment",
        "migration",
        "upgrade",
        "integration",
        "initiative",
        "program",
        "project",
        "transformation",
    }
    platform_tokens = {
        "servicenow",
        "salesforce",
        "workday",
        "oracle",
        "sap",
        "dynamics",
        "microsoft",
        "aws",
        "azure",
    }
    if len(tokens) <= 7 and any(t in platform_tokens for t in tokens) and any(
        t in generic_nouns for t in tokens
    ):
        # Example: "ServiceNow implementation" -> too sparse for ownership claims.
        return True

    if len(set(tokens)) <= 4:
        return True
    return False


def has_sparse_project_signal_inputs(payload: GrantCampaignGenerateRequest) -> bool:
    candidates: list[str] = []
    if payload.award.description:
        candidates.append(payload.award.description)
    for item in payload.evidence:
        if item.excerpt:
            candidates.append(item.excerpt)
    non_empty = [compact_spaces(value) for value in candidates if clean_optional(value)]
    if not non_empty:
        return False
    meaningful = [value for value in non_empty if not sparse_project_signal_text(value)]
    return len(meaningful) == 0


def soften_sparse_signal_ownership_claims(body: str) -> str:
    out = body
    ownership_pattern = re.compile(
        r"(?<!if\s)\byour\s+((?:[A-Za-z0-9&/,'?\-]+\s+){0,4}(?:implementation|rollout|deployment|migration|upgrade|integration|initiative|program|project))\b",
        re.IGNORECASE,
    )
    blocked_context_tokens = {"mandate", "leadership", "oversight", "responsibility", "responsible"}

    def ownership_repl(match: re.Match[str]) -> str:
        phrase = compact_spaces(match.group(1))
        phrase_tokens = set(re.split(r"[^a-z0-9]+", normalize_text(phrase)))
        if phrase_tokens.intersection(blocked_context_tokens):
            return match.group(0)
        if not phrase:
            return match.group(0)
        return f"if {phrase} is a current priority"

    out = ownership_pattern.sub(ownership_repl, out)
    return normalize_copy_artifacts(out)


def apply_sparse_signal_guardrails_to_campaign(
    campaign: GrantCampaign, payload: GrantCampaignGenerateRequest
) -> GrantCampaign:
    if not has_sparse_project_signal_inputs(payload):
        return campaign

    patched_sequences: list[ProspectCampaign] = []
    for seq in campaign.prospect_campaigns:
        patched_emails: list[GrantEmail] = []
        for email in seq.emails:
            patched_emails.append(
                GrantEmail(
                    email_number=email.email_number,
                    subject=email.subject,
                    body=soften_sparse_signal_ownership_claims(email.body),
                )
            )
        patched_sequences.append(
            ProspectCampaign(
                recipient_label=seq.recipient_label,
                recipient_persona=seq.recipient_persona,
                recipient_rationale=seq.recipient_rationale,
                emails=patched_emails,
            )
        )

    return GrantCampaign(
        campaign_title=campaign.campaign_title,
        strategy_summary=campaign.strategy_summary,
        recipients=campaign.recipients,
        prospect_campaigns=patched_sequences,
    )
def should_skip_prospect_web_search(payload: GrantCampaignGenerateRequest) -> bool:
    if not payload.prospects:
        return True
    text_sources = [payload.award.description or ""] + [e.excerpt or "" for e in payload.evidence]
    haystack = " ".join(text_sources).lower()
    if not haystack.strip():
        return False
    for p in payload.prospects:
        if not p.full_name or not p.title:
            return False
        # Require at least a signal mention in provided context to skip search.
        last = p.full_name.split()[-1].lower()
        if last not in haystack:
            return False
    return True


def enforce_source_bound_campaign(
    campaign: GrantCampaign,
    project_research: ProjectResearchBrief,
    prospect_briefs: list[ProspectBrief],
) -> GrantCampaign:
    project_facts = [e.fact for e in project_research.evidence]
    brief_map = {normalize_key(b.full_name): b for b in prospect_briefs}
    allowed_generic = {
        "best",
        "{{accountsignature}}",
        "open to a working session",
        "can we schedule a meeting",
        "let's schedule a meeting",
        "let's set a meeting",
        "working session",
        "review scope",
    }
    patched_sequences: list[ProspectCampaign] = []
    for seq in campaign.prospect_campaigns:
        name_key = normalize_key(extract_name_from_label(seq.recipient_label))
        brief = brief_map.get(name_key)
        person_facts = [s.fact for s in (brief.signals if brief else [])]
        allowed_facts = project_facts + person_facts + get_cri_credibility_ammo()["approved_statements"]

        patched_emails: list[GrantEmail] = []
        for email in seq.emails:
            patched_body = source_bound_body(email.body, allowed_facts, allowed_generic)
            patched_emails.append(
                GrantEmail(
                    email_number=email.email_number,
                    subject=email.subject,
                    body=patched_body,
                )
            )
        patched_sequences.append(
            ProspectCampaign(
                recipient_label=seq.recipient_label,
                recipient_persona=seq.recipient_persona,
                recipient_rationale=seq.recipient_rationale,
                emails=patched_emails,
            )
        )

    return GrantCampaign(
        campaign_title=campaign.campaign_title,
        strategy_summary=campaign.strategy_summary,
        recipients=campaign.recipients,
        prospect_campaigns=patched_sequences,
    )


def source_bound_body(body: str, allowed_facts: list[str], allowed_generic: set[str]) -> str:
    lines = body.splitlines()
    out_lines: list[str] = []
    stripped_content_count = 0
    unsupported_count = 0
    for line in lines:
        stripped = line.strip()
        if not stripped:
            out_lines.append(line)
            continue
        lower = stripped.lower()
        if any(token in lower for token in allowed_generic):
            out_lines.append(line)
            continue
        if stripped.startswith("Hi ") or stripped.startswith("Hello "):
            out_lines.append(line)
            continue
        if is_sentence_supported_by_facts(stripped, allowed_facts):
            out_lines.append(line)
            stripped_content_count += 1
            continue
        unsupported_count += 1
    cleaned = dedupe_lines("\n".join(out_lines))
    if stripped_content_count == 0 and unsupported_count > 0:
        cleaned = f"{cleaned}\n\nCould we schedule a working session to review implementation scope and rollout sequencing?".strip()
    return normalize_copy_artifacts(cleaned)


def is_sentence_supported_by_facts(sentence: str, facts: list[str]) -> bool:
    sentence_tokens = content_tokens(sentence)
    if len(sentence_tokens) < 3:
        return True
    for fact in facts:
        fact_tokens = content_tokens(fact)
        # Overlap threshold keeps claims tethered without requiring exact match.
        if len(sentence_tokens.intersection(fact_tokens)) >= 3:
            return True
    return False


def content_tokens(text: str) -> set[str]:
    stop = {
        "the",
        "and",
        "for",
        "with",
        "that",
        "this",
        "from",
        "your",
        "you",
        "our",
        "are",
        "can",
        "will",
        "have",
        "has",
        "into",
        "across",
        "their",
        "been",
        "being",
        "how",
        "what",
        "where",
        "when",
        "who",
        "would",
        "could",
        "should",
    }
    tokens = re.findall(r"[A-Za-z0-9']+", text.lower())
    return {t for t in tokens if len(t) > 2 and t not in stop}


def ensure_named_prospect_copy(
    campaign: GrantCampaign, payload: GrantCampaignGenerateRequest
) -> GrantCampaign:
    named = [p for p in payload.prospects if p.full_name]
    if not named:
        return campaign

    summary = campaign.strategy_summary
    name_tokens = [n.full_name.lower() for n in named if n.full_name]
    if not any(token in summary.lower() for token in name_tokens):
        first = named[0]
        title = first.title or "lead stakeholder"
        summary = compact_spaces(
            f"{summary} Priority contact: {first.full_name} ({title}) for implementation ownership."
        )

    return GrantCampaign(
        campaign_title=campaign.campaign_title,
        strategy_summary=summary,
        recipients=campaign.recipients,
        prospect_campaigns=campaign.prospect_campaigns,
    )


def dedupe_lines(text: str) -> str:
    out: list[str] = []
    seen_nonempty: set[str] = set()
    prev_blank = False
    for raw_line in text.splitlines():
        line = compact_spaces(raw_line)
        if not line:
            if prev_blank:
                continue
            out.append("")
            prev_blank = True
            continue
        prev_blank = False
        key = normalize_text(line)
        if key in seen_nonempty:
            continue
        seen_nonempty.add(key)
        out.append(line)
    return "\n".join(out).strip()


def normalize_key(value: str) -> str:
    return "".join(ch.lower() for ch in value if ch.isalnum())


def compact_spaces(value: str) -> str:
    return " ".join(value.split())


def render_campaign_text(campaign: GrantCampaign) -> str:
    lines = [campaign.campaign_title, "", campaign.strategy_summary, ""]
    for recipient in campaign.recipients:
        lines.append(f"- {recipient.label} ({recipient.persona}): {recipient.rationale}")
    lines.append("")
    for sequence in campaign.prospect_campaigns:
        lines.append(
            f"{sequence.recipient_label} ({sequence.recipient_persona})"
        )
        lines.append(sequence.recipient_rationale)
        lines.append("")
        for email in sequence.emails:
            lines.append(f"Email {email.email_number} Subject: {email.subject}")
            lines.append(email.body.strip())
            lines.append("")
        lines.append("---")
        lines.append("")
    return "\n".join(lines).strip()


def sanitize_prospect_sequences(
    sequences: list[ProspectCampaign], recipients: list[GrantRecipient]
) -> list[ProspectCampaign]:
    cleaned: list[ProspectCampaign] = []
    recipient_map = {normalize_key(r.label): r for r in recipients}
    used: set[str] = set()

    for sequence in sequences:
        key = normalize_key(sequence.recipient_label)
        if not key or key in used:
            continue
        used.add(key)
        ref = recipient_map.get(key)
        emails = sanitize_email_block(sequence.emails, sequence.recipient_label)
        cleaned.append(
            ProspectCampaign(
                recipient_label=compact_spaces(ref.label if ref else sequence.recipient_label),
                recipient_persona=compact_spaces(ref.persona if ref else sequence.recipient_persona),
                recipient_rationale=compact_spaces(ref.rationale if ref else sequence.recipient_rationale),
                emails=emails,
            )
        )

    if cleaned:
        return cleaned[: len(recipients)]

    fallback: list[ProspectCampaign] = []
    for recipient in recipients:
        fallback.append(
            ProspectCampaign(
                recipient_label=recipient.label,
                recipient_persona=recipient.persona,
                recipient_rationale=recipient.rationale,
                emails=sanitize_email_block([], recipient.label),
            )
        )
    return fallback


def sanitize_email_block(emails: list[GrantEmail], recipient_label: str) -> list[GrantEmail]:
    cleaned: list[GrantEmail] = []
    seen_numbers: set[int] = set()
    for email in sorted(emails, key=lambda e: e.email_number):
        if email.email_number in seen_numbers:
            continue
        seen_numbers.add(email.email_number)
        cleaned.append(
            GrantEmail(
                email_number=email.email_number,
                subject=compact_spaces(email.subject),
                body=format_email_body_for_delivery(dedupe_lines(email.body), recipient_label),
            )
        )
    return sorted(cleaned, key=lambda e: e.email_number)[:4]


def format_email_body_for_delivery(body: str, recipient_label: str) -> str:
    text = body.strip()
    first_name = extract_first_name_from_label(recipient_label)
    name_only_salute = re.compile(rf"^{re.escape(first_name)}\s*,\s*$", flags=re.IGNORECASE)

    raw_lines = [line.rstrip() for line in text.splitlines()]
    while raw_lines and not raw_lines[0].strip():
        raw_lines.pop(0)
    if raw_lines and name_only_salute.match(raw_lines[0].strip()):
        raw_lines.pop(0)
    while raw_lines and not raw_lines[0].strip():
        raw_lines.pop(0)
    text = "\n".join(raw_lines).strip()

    if not re.match(r"^(Hi|Hello)\b", text, flags=re.IGNORECASE):
        text = f"Hi {first_name},\n\n{text}" if text else f"Hi {first_name},"

    # Remove duplicated name-only salutation line directly after greeting.
    lines = text.splitlines()
    if len(lines) >= 3 and re.match(r"^(Hi|Hello)\b", lines[0], flags=re.IGNORECASE):
        second = lines[1].strip()
        third = lines[2].strip()
        if not second and name_only_salute.match(third):
            del lines[2]
            text = "\n".join(lines).strip()

    text = remove_redundant_name_prefix_after_greeting(text, first_name)
    text = replace_weak_opening_phrases(text)
    text = enforce_direct_opening_line(text)
    text = normalize_copy_artifacts(text)
    text = dedupe_lines(text)

    has_signature_token = "{{accountSignature}}" in text
    has_signoff = any(token in text.lower() for token in ["\nbest,", "\nregards,", "\nthank you,"])
    if not has_signoff:
        text = f"{text}\n\nBest,"
    if not has_signature_token:
        text = f"{text}\n\n{{{{accountSignature}}}}"
    return text.strip()


def remove_redundant_name_prefix_after_greeting(text: str, first_name: str) -> str:
    lines = text.splitlines()
    if not lines:
        return text
    if not re.match(r"^(Hi|Hello)\b", lines[0], flags=re.IGNORECASE):
        return text
    idx = 1
    while idx < len(lines) and not lines[idx].strip():
        idx += 1
    if idx >= len(lines):
        return text
    target = lines[idx].lstrip()
    prefix = re.compile(rf"^{re.escape(first_name)}\s*,\s*", flags=re.IGNORECASE)
    lines[idx] = prefix.sub("", target)
    return "\n".join(lines).strip()


def replace_weak_opening_phrases(text: str) -> str:
    replacements: list[tuple[re.Pattern[str], str]] = [
        (
            re.compile(
                r"\byour leadership on [^.]+ is pivotal for [^.]+\.",
                re.IGNORECASE,
            ),
            "You are tasked with owning implementation execution for this initiative across teams and jurisdictions.",
        ),
        (
            re.compile(
                r"\byour oversight is essential for [^.]+\.",
                re.IGNORECASE,
            ),
            "You are responsible for implementation controls, governance, and execution quality for this rollout.",
        ),
        (
            re.compile(
                r"\byour role in governing [^.]+ is central to [^.]+\.",
                re.IGNORECASE,
            ),
            "You are accountable for turning governance requirements into an executable implementation plan.",
        ),
        (
            re.compile(
                r"\byour role in [^.]+ is central to [^.]+\.",
                re.IGNORECASE,
            ),
            "You are accountable for execution ownership and delivery quality for this initiative.",
        ),
        (
            re.compile(
                r"\bwith (the )?[^.]+ deadline[^.]*, [^.]+ (are|is) [^.]+\.",
                re.IGNORECASE,
            ),
            "You are tasked with delivering this initiative on schedule with auditable controls and clear cross-team ownership.",
        ),
    ]
    output = text
    for pattern, replacement in replacements:
        output = pattern.sub(replacement, output)
    return output


def enforce_direct_opening_line(text: str) -> str:
    lines = text.splitlines()
    if not lines:
        return text
    if not re.match(r"^(Hi|Hello)\b", lines[0], flags=re.IGNORECASE):
        return text

    idx = 1
    while idx < len(lines) and not lines[idx].strip():
        idx += 1
    if idx >= len(lines):
        return text

    line = lines[idx].strip()
    # Normalize accidental lowercase starts after greeting.
    if line and line[0].islower():
        line = line[0].upper() + line[1:]

    # Only force mandate framing for weak opener patterns.
    if re.match(r"^Your (leadership|oversight|role|mandate)\b", line, flags=re.IGNORECASE):
        converted = re.sub(
            r"^Your\b",
            "You are tasked with",
            line,
            count=1,
            flags=re.IGNORECASE,
        )
        converted = converted.rstrip(".")
        line = f"{converted}."

    lines[idx] = line
    return "\n".join(lines).strip()


def normalize_copy_artifacts(text: str) -> str:
    out = text
    out = re.sub(r"\?\.", "?", out)
    out = re.sub(r"\.\?", "?", out)
    out = re.sub(r"\bwith with\b", "with", out, flags=re.IGNORECASE)
    out = re.sub(r"\bthe the\b", "the", out, flags=re.IGNORECASE)
    out = re.sub(
        r"\bresource allocation,\s*personnel onboarding,\s*and partnership development\b",
        "rollout sequencing, control ownership, and cross-office execution",
        out,
        flags=re.IGNORECASE,
    )
    out = re.sub(
        r"\bpartnership development\b",
        "cross-office execution planning",
        out,
        flags=re.IGNORECASE,
    )
    out = re.sub(r"[ \t]+\n", "\n", out)
    return out.strip()


def extract_first_name_from_label(recipient_label: str) -> str:
    base = recipient_label.split("(")[0].strip()
    if not base:
        return "{{firstName}}"
    token = base.split()[0].strip(",.")
    if re.fullmatch(r"[A-Za-z][A-Za-z'-]*", token):
        return token
    return "{{firstName}}"


def extract_name_from_label(recipient_label: str) -> str:
    base = recipient_label.split("(")[0].strip()
    return compact_spaces(base) if base else recipient_label


def email_mentions_recipient(body: str, recipient_label: str) -> bool:
    lower = body.lower()
    full_name = extract_name_from_label(recipient_label).lower()
    first_name = extract_first_name_from_label(recipient_label).lower()
    return full_name in lower or (first_name and first_name in lower)


def split_email_sentences(text: str) -> list[str]:
    chunks = re.split(r"(?<=[.!?])\s+|\n+", text)
    out: list[str] = []
    for chunk in chunks:
        line = compact_spaces(chunk)
        if not line:
            continue
        out.append(line)
    return out


def email_quality_issues(body: str, sibling_bodies: list[str]) -> list[str]:
    issues: list[str] = []
    cleaned_lines = [
        normalize_text(line)
        for line in body.splitlines()
        if line.strip() and "{{accountsignature}}" not in normalize_text(line)
    ]
    if cleaned_lines:
        duplicate_ratio = 1 - (len(set(cleaned_lines)) / len(cleaned_lines))
        if duplicate_ratio > 0.2:
            issues.append("duplicate_lines")

    body_sentences = [normalize_text(s) for s in split_email_sentences(body)]
    if len(set(body_sentences)) < 3:
        issues.append("low_unique_sentence_count")

    body_tokens = content_tokens(body)
    for sibling in sibling_bodies:
        sibling_tokens = content_tokens(sibling)
        overlap = jaccard_similarity(body_tokens, sibling_tokens)
        if overlap >= 0.72:
            issues.append("too_similar_to_other_email")
            break
    return issues


async def regenerate_single_email(
    *,
    payload: GrantCampaignGenerateRequest,
    recipient: GrantRecipient,
    existing_sequence: list[GrantEmail],
    target_email_number: int,
    project_research: ProjectResearchBrief,
    api_key: str,
    model: str,
    request_id: str,
    cost_tracker: dict[str, Any],
) -> GrantEmail:
    sequence_context = [
        {
            "email_number": email.email_number,
            "subject": email.subject,
            "body": email.body,
            "target": email.email_number == target_email_number,
        }
        for email in sorted(existing_sequence, key=lambda row: row.email_number)
    ]
    sibling_bodies = [
        email.body
        for email in existing_sequence
        if email.email_number != target_email_number
    ]
    system_prompt = (
        "You are an enterprise AE writing one campaign email in a 4-email sequence. "
        "Return JSON only. Preserve sequence flow and avoid repeating ideas already used in sibling emails."
    )
    base_payload = {
        "task": "Regenerate exactly one email in an existing 4-email sequence.",
        "target_email_number": target_email_number,
        "recipient": recipient.model_dump(),
        "organization": payload.organization.model_dump(),
        "award": payload.award.model_dump(mode="json"),
        "project_research": project_research.model_dump(mode="json"),
        "evidence": [item.model_dump(mode="json") for item in payload.evidence],
        "existing_sequence_context": sequence_context,
        "rules": [
            "Do not rewrite sibling emails.",
            "Generate only target_email_number subject and body.",
            "Avoid repeated sentence structure from sibling emails.",
            "Anchor at least one sentence in concrete provided evidence or project_research.",
            "Max 90 words in body.",
            "Use CTA language as working session or scope review meeting.",
            "No em dash.",
            "No motivational filler.",
            "If sparse_project_signal_inputs is true, do not assert ownership phrases like 'your implementation'. Use conditional phrasing.",
        ],
        "required_output_schema": {
            "subject": "string",
            "body": "string",
        },
    }

    attempt_payloads = [
        base_payload,
        {
            **base_payload,
            "rules": base_payload["rules"]
            + [
                "Use sentence structures that are materially different from sibling emails.",
                "Do not repeat any full sentence from sibling emails.",
            ],
        },
    ]
    last_issues: list[str] = []
    for idx, prompt_payload in enumerate(attempt_payloads, start=1):
        content = await openai_json_completion(
            api_key=api_key,
            model=model,
            system_prompt=system_prompt,
            user_payload=prompt_payload,
            temperature=0.25 if idx == 1 else 0.2,
            stage=f"regenerate_email_{target_email_number}_attempt_{idx}",
            request_id=request_id,
            cost_tracker=cost_tracker,
        )
        subject = compact_spaces(clean_optional(str(content.get("subject") or "")) or "")
        body = clean_optional(str(content.get("body") or ""))
        if not subject or not body:
            last_issues = ["missing_subject_or_body"]
            continue
        formatted_body = format_email_body_for_delivery(dedupe_lines(body), recipient.label)
        issues = email_quality_issues(formatted_body, sibling_bodies)
        if issues:
            last_issues = issues
            continue
        return GrantEmail(
            email_number=target_email_number,
            subject=subject,
            body=formatted_body,
        )
    raise RuntimeError(
        "Regenerated email failed quality checks."
        + (f" issues={','.join(last_issues)}" if last_issues else "")
    )


async def openai_json_completion(
    *,
    api_key: str,
    model: str,
    system_prompt: str,
    user_payload: Mapping[str, Any],
    temperature: float,
    stage: str,
    request_id: str,
    cost_tracker: dict[str, Any],
) -> Mapping[str, Any]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    request_body = {
        "model": model,
        "response_format": {"type": "json_object"},
        "temperature": temperature,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_payload)},
        ],
    }
    logger.info(
        "[grant:%s] openai stage=%s request model=%s temperature=%s",
        request_id,
        stage,
        model,
        temperature,
    )
    started = time.perf_counter()
    try:
        async with httpx.AsyncClient(base_url=openai_api_base(), timeout=45.0) as client:
            upstream = await client.post("/v1/chat/completions", headers=headers, json=request_body)
    except httpx.RequestError as error:
        logger.exception(
            "[grant:%s] openai stage=%s request error: %s",
            request_id,
            stage,
            error,
        )
        raise RuntimeError(f"Failed to reach OpenAI API: {error}") from error

    elapsed_ms = int((time.perf_counter() - started) * 1000)
    payload = parse_json(upstream)
    register_chat_completion_usage(
        cost_tracker=cost_tracker,
        model=model,
        stage=stage,
        payload=payload,
    )
    logger.info(
        "[grant:%s] openai stage=%s status=%s elapsed_ms=%s",
        request_id,
        stage,
        upstream.status_code,
        elapsed_ms,
    )
    if not upstream.is_success:
        message = payload.get("error") if isinstance(payload, Mapping) else None
        if isinstance(message, Mapping):
            detail = message.get("message")
        else:
            detail = None
        logger.error(
            "[grant:%s] openai stage=%s non-success detail=%s",
            request_id,
            stage,
            detail,
        )
        raise RuntimeError(detail or "OpenAI API returned an error")

    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("OpenAI API response missing choices.")
    first = choices[0]
    if not isinstance(first, Mapping):
        raise RuntimeError("OpenAI API response choice malformed.")
    message = first.get("message")
    if not isinstance(message, Mapping):
        raise RuntimeError("OpenAI API response message missing.")
    content = message.get("content")
    if not isinstance(content, str):
        raise RuntimeError("OpenAI API response content missing.")
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"OpenAI returned non-JSON content: {error}") from error
    if not isinstance(parsed, Mapping):
        raise RuntimeError("OpenAI returned invalid JSON object.")
    logger.info("[grant:%s] openai stage=%s parsed response", request_id, stage)
    return parsed


async def openai_responses_json_completion(
    *,
    api_key: str,
    model: str,
    system_prompt: str,
    user_payload: Mapping[str, Any],
    temperature: float,
    stage: str,
    request_id: str,
    cost_tracker: dict[str, Any],
    tools: list[Mapping[str, Any]] | None = None,
) -> Mapping[str, Any]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    request_body: dict[str, Any] = {
        "model": model,
        "temperature": temperature,
        "input": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_payload)},
        ],
    }
    if tools:
        request_body["tools"] = tools
    else:
        request_body["text"] = {"format": {"type": "json_object"}}

    logger.info(
        "[grant:%s] openai responses stage=%s request model=%s temperature=%s tools=%s",
        request_id,
        stage,
        model,
        temperature,
        bool(tools),
    )
    started = time.perf_counter()
    try:
        async with httpx.AsyncClient(base_url=openai_api_base(), timeout=60.0) as client:
            upstream = await client.post("/v1/responses", headers=headers, json=request_body)
    except httpx.RequestError as error:
        logger.exception(
            "[grant:%s] openai responses stage=%s request error: %s",
            request_id,
            stage,
            error,
        )
        raise RuntimeError(f"Failed to reach OpenAI Responses API: {error}") from error

    elapsed_ms = int((time.perf_counter() - started) * 1000)
    payload = parse_json(upstream)
    register_responses_usage(
        cost_tracker=cost_tracker,
        model=model,
        stage=stage,
        payload=payload,
    )
    logger.info(
        "[grant:%s] openai responses stage=%s status=%s elapsed_ms=%s",
        request_id,
        stage,
        upstream.status_code,
        elapsed_ms,
    )
    if not upstream.is_success:
        message = payload.get("error") if isinstance(payload, Mapping) else None
        if isinstance(message, Mapping):
            detail = message.get("message")
        else:
            detail = None
        logger.error(
            "[grant:%s] openai responses stage=%s non-success detail=%s",
            request_id,
            stage,
            detail,
        )
        raise RuntimeError(detail or "OpenAI Responses API returned an error")

    content_text = extract_responses_output_text(payload)
    if not content_text:
        raise RuntimeError("OpenAI Responses API returned empty content.")
    try:
        parsed = json.loads(content_text)
    except json.JSONDecodeError as error:
        candidate = extract_json_object_fragment(content_text)
        if not candidate:
            raise RuntimeError(f"OpenAI Responses API returned non-JSON content: {error}") from error
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError as nested_error:
            raise RuntimeError(
                f"OpenAI Responses API returned non-JSON content: {nested_error}"
            ) from nested_error
    if not isinstance(parsed, Mapping):
        raise RuntimeError("OpenAI Responses API returned invalid JSON object.")
    logger.info("[grant:%s] openai responses stage=%s parsed response", request_id, stage)
    return parsed


def extract_json_object_fragment(text: str) -> str:
    start = text.find("{")
    if start < 0:
        return ""
    depth = 0
    in_string = False
    escaped = False
    for idx in range(start, len(text)):
        ch = text[idx]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            depth += 1
            continue
        if ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : idx + 1]
    return ""


def merge_context_signals_into_prospect_briefs(
    payload: GrantCampaignGenerateRequest, briefs: list[ProspectBrief]
) -> list[ProspectBrief]:
    if not payload.prospects:
        return briefs

    by_key = {normalize_key(b.full_name): b for b in briefs}
    merged: list[ProspectBrief] = []
    for prospect in payload.prospects[: payload.constraints.max_recipients]:
        full_name = prospect.full_name or "Named Prospect"
        key = normalize_key(full_name)
        existing = by_key.get(key)

        if existing:
            signals = list(existing.signals)
            if not signals:
                injected = context_signal_for_prospect(payload, prospect)
                if injected:
                    signals = [injected]
            confidence = existing.research_confidence if signals else "LOW"
            merged.append(
                ProspectBrief(
                    full_name=existing.full_name,
                    title=existing.title,
                    organization=existing.organization or payload.organization.name,
                    linkedin_url=existing.linkedin_url,
                    research_confidence=confidence,
                    signals=signals,
                    personalization_angle=existing.personalization_angle
                    if existing.personalization_angle
                    else "Project mandate alignment and implementation ownership.",
                )
            )
            continue

        injected = context_signal_for_prospect(payload, prospect)
        merged.append(
            ProspectBrief(
                full_name=full_name,
                title=prospect.title or "Stakeholder",
                organization=prospect.organization or payload.organization.name,
                linkedin_url=prospect.linkedin_url or "NOT_FOUND",
                research_confidence="MEDIUM" if injected else "LOW",
                signals=[injected] if injected else [],
                personalization_angle="Project mandate alignment and implementation ownership.",
            )
        )
    return merged


def context_signal_for_prospect(
    payload: GrantCampaignGenerateRequest, prospect: ProvidedProspect
) -> ProspectSignal | None:
    if not prospect.full_name:
        return None
    text_candidates: list[tuple[str, str]] = []
    if payload.award.description:
        text_candidates.append((payload.award.description, "provided://award_description"))
    for item in payload.evidence:
        if item.excerpt:
            text_candidates.append((item.excerpt, item.url or "provided://evidence"))

    name_re = re.compile(rf"\b{re.escape(prospect.full_name)}\b", re.IGNORECASE)
    for raw_text, src in text_candidates:
        for sentence in split_sentences(raw_text):
            s = compact_spaces(sentence)
            if not s:
                continue
            if is_low_value_context_sentence(s):
                continue
            if name_re.search(s):
                return ProspectSignal(fact=s, source_url=src)

    # Fallback to a grounded statement from provided input when no sentence contains the name.
    if prospect.title:
        org = payload.organization.name
        return ProspectSignal(
            fact=f"Provided initiative context names {prospect.full_name} as {prospect.title} for {org}.",
            source_url="provided://initiative_context",
        )
    return None


def split_sentences(text: str) -> list[str]:
    if not text:
        return []
    return re.split(r"(?<=[.!?])\s+", text.strip())


def is_low_value_context_sentence(sentence: str) -> bool:
    s = sentence.strip().lower()
    if not s:
        return True
    disallowed_markers = [
        "top prospect:",
        "inferred from provided campaign context",
        "provided in the initiative context",
    ]
    if any(marker in s for marker in disallowed_markers):
        return True
    # Low-value if it names multiple alternates ("X ... or Y ...") instead of one concrete fact.
    if " or " in s and "(" in s and ")" in s:
        return True
    return False


def extract_responses_output_text(payload: Mapping[str, Any]) -> str:
    direct = payload.get("output_text")
    if isinstance(direct, str) and direct.strip():
        return direct

    output = payload.get("output")
    if not isinstance(output, list):
        return ""
    parts: list[str] = []
    for item in output:
        if not isinstance(item, Mapping):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, Mapping):
                continue
            if block.get("type") == "output_text":
                text = block.get("text")
                if isinstance(text, str):
                    parts.append(text)
    return "\n".join(parts).strip()


def init_cost_tracker(request_id: str) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "input_tokens": 0,
        "output_tokens": 0,
        "web_search_calls": 0,
        "estimated_model_cost_usd": 0.0,
        "estimated_tool_cost_usd": 0.0,
        "stages": [],
    }


def register_chat_completion_usage(
    *,
    cost_tracker: dict[str, Any],
    model: str,
    stage: str,
    payload: Mapping[str, Any],
) -> None:
    usage = payload.get("usage")
    if not isinstance(usage, Mapping):
        return
    prompt_tokens = int(usage.get("prompt_tokens") or 0)
    completion_tokens = int(usage.get("completion_tokens") or 0)
    add_cost_usage(
        cost_tracker=cost_tracker,
        model=model,
        stage=stage,
        input_tokens=prompt_tokens,
        output_tokens=completion_tokens,
        web_search_calls=0,
    )


def register_responses_usage(
    *,
    cost_tracker: dict[str, Any],
    model: str,
    stage: str,
    payload: Mapping[str, Any],
) -> None:
    usage = payload.get("usage")
    input_tokens = 0
    output_tokens = 0
    if isinstance(usage, Mapping):
        input_tokens = int(usage.get("input_tokens") or 0)
        output_tokens = int(usage.get("output_tokens") or 0)
    web_search_calls = count_web_search_calls(payload)
    add_cost_usage(
        cost_tracker=cost_tracker,
        model=model,
        stage=stage,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        web_search_calls=web_search_calls,
    )


def count_web_search_calls(payload: Mapping[str, Any]) -> int:
    output = payload.get("output")
    if not isinstance(output, list):
        return 0
    count = 0
    for item in output:
        if not isinstance(item, Mapping):
            continue
        item_type = str(item.get("type") or "")
        if "web_search" in item_type:
            count += 1
    return count


def add_cost_usage(
    *,
    cost_tracker: dict[str, Any],
    model: str,
    stage: str,
    input_tokens: int,
    output_tokens: int,
    web_search_calls: int,
) -> None:
    pricing = MODEL_PRICING_PER_MILLION.get(model, MODEL_PRICING_PER_MILLION["gpt-4.1"])
    input_cost = (input_tokens / 1_000_000.0) * pricing["input"]
    output_cost = (output_tokens / 1_000_000.0) * pricing["output"]
    tool_cost = web_search_calls * WEB_SEARCH_PREVIEW_COST_PER_CALL_USD
    model_cost = input_cost + output_cost

    cost_tracker["input_tokens"] += input_tokens
    cost_tracker["output_tokens"] += output_tokens
    cost_tracker["web_search_calls"] += web_search_calls
    cost_tracker["estimated_model_cost_usd"] += model_cost
    cost_tracker["estimated_tool_cost_usd"] += tool_cost
    cost_tracker["stages"].append(
        {
            "stage": stage,
            "model": model,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "web_search_calls": web_search_calls,
            "estimated_model_cost_usd": round(model_cost, 6),
            "estimated_tool_cost_usd": round(tool_cost, 6),
            "estimated_total_cost_usd": round(model_cost + tool_cost, 6),
        }
    )


def summarize_cost_tracker(cost_tracker: dict[str, Any]) -> dict[str, Any]:
    model_cost = float(cost_tracker.get("estimated_model_cost_usd") or 0.0)
    tool_cost = float(cost_tracker.get("estimated_tool_cost_usd") or 0.0)
    return {
        "request_id": cost_tracker.get("request_id"),
        "input_tokens": int(cost_tracker.get("input_tokens") or 0),
        "output_tokens": int(cost_tracker.get("output_tokens") or 0),
        "web_search_calls": int(cost_tracker.get("web_search_calls") or 0),
        "estimated_model_cost_usd": round(model_cost, 6),
        "estimated_tool_cost_usd": round(tool_cost, 6),
        "estimated_total_usd": round(model_cost + tool_cost, 6),
        "pricing_assumptions": {
            "models_per_1m_tokens_usd": MODEL_PRICING_PER_MILLION,
            "web_search_preview_per_call_usd": WEB_SEARCH_PREVIEW_COST_PER_CALL_USD,
        },
        "stages": cost_tracker.get("stages", []),
    }










