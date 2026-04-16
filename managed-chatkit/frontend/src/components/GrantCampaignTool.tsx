import { FormEvent, MutableRefObject, useEffect, useMemo, useRef, useState } from "react";
import { PDFDocument, StandardFonts, rgb } from "pdf-lib";
import { apiUrl } from "../lib/apiBase";

const FORM_STORAGE_KEY = "grant_campaign_tool_form_v1";
const RESULT_STORAGE_KEY = "grant_campaign_tool_result_v1";
const MODAL_STORAGE_KEY = "grant_campaign_tool_modal_open_v1";
type GrantCampaignResponse = {
  mode: string;
  lead_id: string;
  generated_at: string;
  minimum_fields_used?: {
    organization_name?: string | null;
    organization_website?: string | null;
    award_id?: string | null;
  };
  campaign: {
    campaign_title: string;
    strategy_summary: string;
    recipients: Array<{ label: string; persona: string; rationale: string }>;
    prospect_campaigns: Array<{
      recipient_label: string;
      recipient_persona: string;
      recipient_rationale: string;
      emails: Array<{ email_number: number; subject: string; body: string }>;
    }>;
  };
  campaign_text: string;
};

type ApolloRecipientResult = {
  full_name: string;
  title?: string | null;
  found: boolean;
  email?: string | null;
  phone?: string | null;
  email_status?: string | null;
  linkedin_url?: string | null;
  apollo_person_id?: string | null;
  source?: string;
  detail?: string | null;
};

type ApolloEnrichResponse = {
  results: ApolloRecipientResult[];
  matched_count: number;
  requested_count: number;
  organization_domain?: string | null;
};

type ApolloHealthResponse = {
  ok: boolean;
  route?: string;
  has_api_key?: boolean;
  message?: string | null;
};

type HubspotContextResponse = {
  organization_name?: string | null;
  organization_domain?: string | null;
  retrieval_source?: string | null;
  summary?: Record<string, unknown> | null;
  search?: Record<string, unknown> | null;
  companies?: Record<string, unknown> | null;
  contacts?: Record<string, unknown> | null;
  deals?: Record<string, unknown> | null;
  exact_matches?: Record<string, unknown> | null;
  account_match?: Record<string, unknown> | null;
  recommended_action?: Record<string, unknown> | null;
  relationship_history?: Record<string, unknown> | null;
  similar_closed_won?: Record<string, unknown> | null;
  errors?: string[];
};

type ApolloAccountSnapshotResponse = {
  matched?: boolean;
  message?: string;
  organization?: Record<string, unknown> | null;
  lookup?: Record<string, unknown> | null;
};

type CampaignFormContext = {
  organizationName: string;
  organizationWebsite: string;
  awardId: string;
  agency: string;
  awardDescription: string;
};

type RegenerateEmailResponse = {
  recipient_label?: string;
  email?: { email_number: number; subject: string; body: string };
  sequence?: Array<{ email_number: number; subject: string; body: string }>;
  error?: string;
};

type RecommendedAssetItem = {
  id?: string;
  title: string;
  url: string;
  thumbnail_url?: string | null;
  thumbnail_base64?: string | null;
  industry?: string | null;
  score?: number | null;
  reason?: string | null;
  path?: string | null;
  matched_terms?: string[];
  source?: string;
};

type CaseStudyRecommendResponse = {
  items?: RecommendedAssetItem[];
  message?: string;
  source?: string;
};

type HubspotSummaryTile = {
  key: "contacts" | "companies" | "deals" | "tickets";
  label: string;
  accessible: boolean | null;
  total: number | null;
};

type HubspotCompanyItem = {
  id: string;
  name: string;
  industry: string;
  domain: string;
  location: string;
  url: string;
};

type HubspotContactItem = {
  id: string;
  name: string;
  email: string;
  phone: string;
  company: string;
  url: string;
};

type HubspotDealItem = {
  id: string;
  name: string;
  amount: string;
  stage: string;
  closeDate: string;
  url: string;
};

type HubspotSearchItem = {
  id: string;
  name: string;
  email: string;
  company: string;
  url: string;
};

type StoredFormState = {
  organizationName: string;
  organizationWebsite: string;
  organizationCity: string;
  organizationState: string;
  awardId: string;
  agency: string;
  awardDescription: string;
  maxRecipients: string;
};

const THINKING_STEPS = [
  "Analyzing organization profile...",
  "Identifying target stakeholders...",
  "Drafting personalized email sequences...",
  "Structuring campaign framework...",
  "Campaign ready",
] as const;

const STEP_PREFIXES = [
  "\u{1F50D} ",
  "\u{1F3AF} ",
  "\u{2709}\u{FE0F} ",
  "\u{1F4CB} ",
  "\u{2705} ",
] as const;

const WORKSPACE_LOADING_STEPS = [
  "Checking Apollo service health...",
  "Resolving organization domain from name...",
  "Searching Apollo company candidates...",
  "Verifying official organization website...",
  "Building account snapshot...",
  "Loading CRM context and relationship history...",
  "Scoring similar wins and preparing insights...",
  "Finalizing workspace view...",
] as const;

const WORKSPACE_STEP_PREFIXES = [
  "\u{1F9EA} ",
  "\u{1F50E}\u{FE0F} ",
  "\u{1F3E2} ",
  "\u{1F310} ",
  "\u{1F4CA} ",
  "\u{1F517} ",
  "\u{1F4C8} ",
  "\u{2705} ",
] as const;

export function GrantCampaignTool() {
  const generatedLeadId = useMemo(() => crypto.randomUUID(), []);
  const [organizationName, setOrganizationName] = useState("");
  const [organizationWebsite, setOrganizationWebsite] = useState("");
  const [organizationCity, setOrganizationCity] = useState("");
  const [organizationState, setOrganizationState] = useState("");
  const [awardId, setAwardId] = useState("");
  const [agency, setAgency] = useState("");
  const [awardDescription, setAwardDescription] = useState("");
  const [maxRecipients, setMaxRecipients] = useState("2");

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<GrantCampaignResponse | null>(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [visibleSteps, setVisibleSteps] = useState(0);

  useEffect(() => {
    try {
      const rawForm = window.localStorage.getItem(FORM_STORAGE_KEY);
      if (rawForm) {
        const parsed = JSON.parse(rawForm) as Partial<StoredFormState>;
        setOrganizationName(parsed.organizationName ?? "");
        setOrganizationWebsite(parsed.organizationWebsite ?? "");
        setOrganizationCity(parsed.organizationCity ?? "");
        setOrganizationState(parsed.organizationState ?? "");
        setAwardId(parsed.awardId ?? "");
        setAgency(parsed.agency ?? "");
        setAwardDescription(parsed.awardDescription ?? "");
        setMaxRecipients(parsed.maxRecipients ?? "2");
      }
      const rawResult = window.localStorage.getItem(RESULT_STORAGE_KEY);
      if (rawResult) {
        const parsedResult = JSON.parse(rawResult) as GrantCampaignResponse;
        setResult(parsedResult);
      }
      const rawModal = window.localStorage.getItem(MODAL_STORAGE_KEY);
      if (rawModal === "1" && rawResult) {
        setIsModalOpen(true);
      }
    } catch (storageError) {
      console.warn("[sled-tool] failed to restore persisted session", storageError);
    }
  }, []);

  useEffect(() => {
    const formSnapshot: StoredFormState = {
      organizationName,
      organizationWebsite,
      organizationCity,
      organizationState,
      awardId,
      agency,
      awardDescription,
      maxRecipients,
    };
    try {
      window.localStorage.setItem(FORM_STORAGE_KEY, JSON.stringify(formSnapshot));
    } catch (storageError) {
      console.warn("[sled-tool] failed to persist form session", storageError);
    }
  }, [
    organizationName,
    organizationWebsite,
    organizationCity,
    organizationState,
    awardId,
    agency,
    awardDescription,
    maxRecipients,
  ]);

  useEffect(() => {
    try {
      if (result) {
        window.localStorage.setItem(RESULT_STORAGE_KEY, JSON.stringify(result));
      } else {
        window.localStorage.removeItem(RESULT_STORAGE_KEY);
      }
    } catch (storageError) {
      console.warn("[sled-tool] failed to persist campaign result", storageError);
    }
  }, [result]);

  useEffect(() => {
    try {
      window.localStorage.setItem(MODAL_STORAGE_KEY, isModalOpen ? "1" : "0");
    } catch {
      // no-op
    }
  }, [isModalOpen]);

  useEffect(() => {
    if (!loading) return;
    setVisibleSteps(0);
    const stepsWithoutFinal = THINKING_STEPS.length - 1;
    const timer = window.setInterval(() => {
      setVisibleSteps((prev) => {
        if (prev >= stepsWithoutFinal) {
          window.clearInterval(timer);
          return prev;
        }
        return prev + 1;
      });
    }, 150);
    return () => window.clearInterval(timer);
  }, [loading]);

  const resetFormState = () => {
    setOrganizationName("");
    setOrganizationWebsite("");
    setOrganizationCity("");
    setOrganizationState("");
    setAwardId("");
    setAgency("");
    setAwardDescription("");
    setMaxRecipients("2");
    setError(null);
    setResult(null);
    setVisibleSteps(0);
    try {
      window.localStorage.removeItem(FORM_STORAGE_KEY);
      window.localStorage.removeItem(RESULT_STORAGE_KEY);
      window.localStorage.removeItem(MODAL_STORAGE_KEY);
    } catch {
      // no-op
    }
  };

  const closeModal = () => {
    setIsModalOpen(false);
    resetFormState();
  };
  const expectedLocalPort = "3001";

  const submitPayload = async (payload: unknown) => {
    setLoading(true);
    setError(null);
    setResult(null);
    setIsModalOpen(true);
    try {
      const endpoint = apiUrl("/api/grant-campaign/generate");
      const response = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const rawBody = await response.text();
      let json = {} as {
        error?: string;
      } & GrantCampaignResponse;
      try {
        json = rawBody
          ? ((JSON.parse(rawBody) as typeof json) ?? ({} as typeof json))
          : ({} as typeof json);
      } catch {
        json = {} as typeof json;
      }
      if (!response.ok) {
        throw new Error(
          json.error ??
            `Grant campaign request failed (${response.status} ${response.statusText}).`
        );
      }
      setResult(json);
      setVisibleSteps(THINKING_STEPS.length);
    } catch (submitError) {
      console.error("[sled-tool] submit failed", submitError);
      const onWrongLocalPort =
        window.location.hostname === "localhost" &&
        window.location.port &&
        window.location.port !== expectedLocalPort;
      const portHint = onWrongLocalPort
        ? ` You are currently on http://localhost:${window.location.port}, but this app is configured for http://localhost:${expectedLocalPort}.`
        : "";
      setError(
        submitError instanceof TypeError
          ? `Network error: cannot reach backend at /api/grant-campaign/generate. Confirm backend is running on http://127.0.0.1:8000 and Vite proxy is active.${portHint}`
          : submitError instanceof Error
          ? submitError.message
          : "Grant campaign request failed."
      );
    } finally {
      setLoading(false);
    }
  };

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!organizationName.trim() || !awardDescription.trim()) {
      setError("Please complete all required fields marked with *.");
      return;
    }

    const evidence = buildEvidence(
      cleanNullable(awardId),
      cleanNullable(awardDescription)
    );
    const payload = {
      mode: "grant_awardee_outreach",
      lead_id: generatedLeadId,
      organization: {
        name: cleanNullable(organizationName),
        website: cleanNullable(organizationWebsite),
        city: cleanNullable(organizationCity),
        state: cleanNullable(organizationState),
      },
      award: {
        source: "usaspending",
        award_id: cleanNullable(awardId),
        generated_internal_id: null,
        agency: cleanNullable(agency),
        amount: null,
        award_date: null,
        period_start: null,
        period_end: null,
        cfda_number: null,
        cfda_title: null,
        description: cleanNullable(awardDescription),
        place_of_performance: null,
      },
      evidence,
      constraints: {
        max_recipients: Math.max(1, Math.min(5, Number(maxRecipients || 2))),
        version: 1,
      },
    };
    await submitPayload(payload);
  };

  return (
    <div className="tool-shell panel stack-sm">
      <h2 className="section-title">SLED Campaign Tool</h2>
      <p className="muted">
        Generate consultant implementation outreach from grant data. Required
        fields are marked with *.
      </p>

      <form className="stack-sm" onSubmit={handleSubmit}>        <div className="toolbar">
          <Field
            label="Organization Name *"
            value={organizationName}
            onChange={setOrganizationName}
            required
          />
          <Field
            label="Organization Website"
            value={organizationWebsite}
            onChange={setOrganizationWebsite}
          />
          <Field label="City" value={organizationCity} onChange={setOrganizationCity} />
          <Field label="State" value={organizationState} onChange={setOrganizationState} />
          <Field label="Award ID" value={awardId} onChange={setAwardId} />
          <Field label="Agency" value={agency} onChange={setAgency} />
          <Field
            label="Max Recipients"
            value={maxRecipients}
            onChange={setMaxRecipients}
            type="number"
            min={1}
            max={5}
          />
        </div>

        <TextAreaField
          label="Award Description / Account Notes *"
          value={awardDescription}
          onChange={setAwardDescription}
          rows={4}
          required
        />

        <div className="actions-row"> 
          <button className="action-btn" type="submit" disabled={loading}>
            {loading ? "Generating..." : "Generate Campaign"}
          </button>
        </div>
      </form>

      {error && !isModalOpen ? <p className="status-error">{error}</p> : null}

      <CampaignResultModal
        isOpen={isModalOpen}
        loading={loading}
        result={result}
        error={error}
        visibleSteps={visibleSteps}
        formContext={{
          organizationName,
          organizationWebsite,
          awardId,
          agency,
          awardDescription,
        }}
        onResultUpdate={setResult}
        onClose={closeModal}
      />
    </div>
  );
}

type ModalProps = {
  isOpen: boolean;
  loading: boolean;
  result: GrantCampaignResponse | null;
  error: string | null;
  visibleSteps: number;
  formContext: CampaignFormContext;
  onResultUpdate: (next: GrantCampaignResponse | null) => void;
  onClose: () => void;
};

function CampaignResultModal({
  isOpen,
  loading,
  result,
  error,
  visibleSteps,
  formContext,
  onResultUpdate,
  onClose,
}: ModalProps) {
  type WorkspaceTab = "overview" | "hubspot" | "email" | "phone";
  const callerName =
    (import.meta as ImportMeta & { env?: Record<string, string> }).env?.VITE_CRI_CALLER_NAME?.trim() ||
    "";
  const callerPhrase = callerName ? `${callerName} with CRI` : "a member of the CRI team";
  const [copiedEmailKey, setCopiedEmailKey] = useState<string | null>(null);
  const [apolloEmailLoadingLabel, setApolloEmailLoadingLabel] = useState<string | null>(null);
  const [apolloPhoneLoadingLabel, setApolloPhoneLoadingLabel] = useState<string | null>(null);
  const [apolloError, setApolloError] = useState<string | null>(null);
  const [emailRegenerateError, setEmailRegenerateError] = useState<string | null>(null);
  const [regeneratingEmailKey, setRegeneratingEmailKey] = useState<string | null>(null);
  const [apolloByLabel, setApolloByLabel] = useState<Record<string, ApolloRecipientResult>>({});
  const [apolloAvailable, setApolloAvailable] = useState<boolean>(false);
  const [apolloHealthMessage, setApolloHealthMessage] = useState<string | null>(null);
  const [hubspotLoading, setHubspotLoading] = useState(false);
  const [hubspotError, setHubspotError] = useState<string | null>(null);
  const [hubspotContext, setHubspotContext] = useState<HubspotContextResponse | null>(null);
  const [apolloSnapshot, setApolloSnapshot] = useState<ApolloAccountSnapshotResponse | null>(null);
  const [apolloDomainLoading, setApolloDomainLoading] = useState(false);
  const [caseStudies, setCaseStudies] = useState<CaseStudyRecommendResponse | null>(null);
  const [caseStudyError, setCaseStudyError] = useState<string | null>(null);
  const [workspaceStepIndex, setWorkspaceStepIndex] = useState(0);
  const [activeTab, setActiveTab] = useState<WorkspaceTab>("overview");
  const [splitMode, setSplitMode] = useState(false);
  const [embeddedWorkspaceUrl, setEmbeddedWorkspaceUrl] = useState<string | null>(null);
  const [embeddedWorkspaceTitle, setEmbeddedWorkspaceTitle] = useState<string>("Workspace");
  const apolloPopupRef = useRef<Window | null>(null);
  const hubspotPopupRef = useRef<Window | null>(null);
  const modalBodyRef = useRef<HTMLDivElement | null>(null);
  const activeWorkspaceRunKeyRef = useRef<string | null>(null);
  const workspaceRequestSeqRef = useRef(0);

  const closePopupRef = (popupRef: MutableRefObject<Window | null>) => {
    const popup = popupRef.current;
    if (!popup || popup.closed) {
      popupRef.current = null;
      return;
    }
    try {
      popup.close();
    } catch {
      // no-op
    }
    popupRef.current = null;
  };

  const closeWorkspacePopups = () => {
    closePopupRef(apolloPopupRef);
    closePopupRef(hubspotPopupRef);
  };

  const closeModalAndApollo = () => {
    setSplitMode(false);
    setEmbeddedWorkspaceUrl(null);
    setEmbeddedWorkspaceTitle("Workspace");
    closeWorkspacePopups();
    onClose();
  };

  useEffect(() => {
    if (!isOpen || !result) return;
    let cancelled = false;
    const checkHealth = async () => {
      try {
        const response = await fetch(apiUrl("/api/apollo/health"));
        const data = (await response.json()) as ApolloHealthResponse;
        if (cancelled) return;
        if (!response.ok || !data.ok) {
          setApolloAvailable(false);
          setApolloHealthMessage(
            "Apollo integration unavailable. Restart backend to load latest routes."
          );
          return;
        }
        if (!data.has_api_key) {
          setApolloAvailable(false);
          setApolloHealthMessage(data.message ?? "APOLLO_API_KEY is not configured.");
          return;
        }
        setApolloAvailable(true);
        setApolloHealthMessage(null);
      } catch {
        if (cancelled) return;
        setApolloAvailable(false);
        setApolloHealthMessage(
          "Apollo integration unavailable. Restart backend and verify API routing."
        );
      }
    };
    void checkHealth();
    return () => {
      cancelled = true;
    };
  }, [isOpen, result]);

  useEffect(() => {
    if (!isOpen || !result) {
      activeWorkspaceRunKeyRef.current = null;
      return;
    }
    const runKey = result.lead_id || result.generated_at || result.campaign.campaign_title;
    if (activeWorkspaceRunKeyRef.current === runKey && hubspotContext) {
      return;
    }
    activeWorkspaceRunKeyRef.current = runKey;
    workspaceRequestSeqRef.current += 1;
    setHubspotContext(null);
    setApolloSnapshot(null);
    setCaseStudies(null);
    setHubspotError(null);
    setCaseStudyError(null);
    setApolloError(null);
    void loadWorkspaceData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isOpen, result?.lead_id, result?.generated_at]);

  useEffect(() => {
    if (!isOpen) {
      setSplitMode(false);
      setEmbeddedWorkspaceUrl(null);
      setEmbeddedWorkspaceTitle("Workspace");
      setActiveTab("overview");
      setWorkspaceStepIndex(0);
    }
  }, [isOpen]);

  useEffect(() => {
    if (!hubspotLoading) {
      setWorkspaceStepIndex(0);
      return;
    }
    const timer = window.setInterval(() => {
      setWorkspaceStepIndex((prev) => (prev + 1) % WORKSPACE_LOADING_STEPS.length);
    }, 1300);
    return () => window.clearInterval(timer);
  }, [hubspotLoading]);

  useEffect(() => {
    if (!isOpen) return;
    if (!modalBodyRef.current) return;
    modalBodyRef.current.scrollTop = 0;
  }, [activeTab, isOpen]);

  if (!isOpen) return null;

  const copyEmail = async (
    recipientLabel: string,
    emailNumber: number,
    subject: string,
    body: string
  ) => {
    const key = `${recipientLabel}-${emailNumber}`;
    const text = `Subject: ${subject}\n\n${body}`;
    await navigator.clipboard.writeText(text);
    setCopiedEmailKey(key);
    window.setTimeout(() => {
      setCopiedEmailKey((prev) => (prev === key ? null : prev));
    }, 1400);
  };

  const regenerateEmail = async (
    sequence: {
      recipient_label: string;
      recipient_persona: string;
      recipient_rationale: string;
      emails: Array<{ email_number: number; subject: string; body: string }>;
    },
    emailNumber: number
  ) => {
    if (!result) return;
    const key = `${sequence.recipient_label}-${emailNumber}`;
    setRegeneratingEmailKey(key);
    setEmailRegenerateError(null);
    try {
      const response = await fetch(apiUrl("/api/grant-campaign/regenerate-email"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          lead_id: result.lead_id,
          organization: {
            name: formContext.organizationName || result.minimum_fields_used?.organization_name || null,
            website:
              formContext.organizationWebsite || result.minimum_fields_used?.organization_website || null,
            city: null,
            state: null,
          },
          award: {
            source: "usaspending",
            award_id: formContext.awardId || result.minimum_fields_used?.award_id || null,
            generated_internal_id: null,
            agency: formContext.agency || null,
            amount: null,
            award_date: null,
            period_start: null,
            period_end: null,
            cfda_number: null,
            cfda_title: null,
            description: formContext.awardDescription || null,
            place_of_performance: null,
          },
          evidence: buildEvidence(
            cleanNullable(formContext.awardId || result.minimum_fields_used?.award_id || null),
            cleanNullable(formContext.awardDescription || null)
          ),
          recipient_label: sequence.recipient_label,
          recipient_persona: sequence.recipient_persona,
          recipient_rationale: sequence.recipient_rationale,
          target_email_number: emailNumber,
          existing_sequence: sequence.emails,
        }),
      });
      const body = (await response.json()) as RegenerateEmailResponse;
      if (!response.ok) {
        throw new Error(body.error ?? `Regenerate failed (${response.status})`);
      }
      const nextSequence = Array.isArray(body.sequence)
        ? body.sequence
        : sequence.emails.map((email) =>
            email.email_number === emailNumber && body.email ? body.email : email
          );
      const updatedCampaign = result.campaign.prospect_campaigns.map((row) => {
        if (row.recipient_label !== sequence.recipient_label) return row;
        return {
          ...row,
          emails: nextSequence,
        };
      });
      onResultUpdate({
        ...result,
        campaign: {
          ...result.campaign,
          prospect_campaigns: updatedCampaign,
        },
      });
    } catch (regenerateError) {
      setEmailRegenerateError(
        regenerateError instanceof Error ? regenerateError.message : "Email regeneration failed."
      );
    } finally {
      setRegeneratingEmailKey((prev) => (prev === key ? null : prev));
    }
  };

  const exportPdf = async () => {
    if (!result) return;
    try {
      await exportCampaignPdf(result, apolloByLabel, hubspotContext, apolloSnapshot);
    } catch (pdfError) {
      console.error("PDF export failed, falling back to print view.", pdfError);
      const reportHtml = buildLeadershipReportHtml(result, apolloByLabel, hubspotContext, apolloSnapshot);
      const reportWindow = window.open("", "_blank", "noopener,noreferrer");
      if (!reportWindow) return;
      reportWindow.document.open();
      reportWindow.document.write(reportHtml);
      reportWindow.document.close();
      reportWindow.focus();
      window.setTimeout(() => {
        reportWindow.print();
      }, 250);
    }
  };

  const openSideBySideWorkspace = (
    url: string,
    workspaceName: string,
    popupRef: MutableRefObject<Window | null>,
    titleOverride?: string
  ) => {
    if (!url) return;
    // Primary path: embedded side-by-side pane inside the modal.
    setEmbeddedWorkspaceUrl(url);
    const title =
      titleOverride ??
      (workspaceName === "apollo_workspace"
        ? "Apollo Workspace"
        : workspaceName === "hubspot_workspace"
          ? "HubSpot Workspace"
          : "Workspace");
    setEmbeddedWorkspaceTitle(title);
    setSplitMode(true);

    // Keep popup refs available only for explicit fallback/new-window actions.
    if (popupRef.current && popupRef.current.closed) {
      popupRef.current = null;
    }
  };

  const openEmbeddedWorkspaceInNewWindow = () => {
    if (!embeddedWorkspaceUrl) return;
    const newWindow = window.open(embeddedWorkspaceUrl, "_blank", "noopener,noreferrer");
    if (newWindow) {
      try {
        newWindow.focus();
      } catch {
        // no-op
      }
    }
  };

  const closeEmbeddedWorkspace = () => {
    setSplitMode(false);
    setEmbeddedWorkspaceUrl(null);
    setEmbeddedWorkspaceTitle("Workspace");
  };

  const openApolloAccount = () => {
    const apolloUrl =
      (import.meta as ImportMeta & { env?: Record<string, string> }).env
        ?.VITE_APOLLO_APP_URL ?? "https://app.apollo.io/";
    openSideBySideWorkspace(apolloUrl, "apollo_workspace", apolloPopupRef);
  };

  const openHubspotWorkspaceLink = (url: string) => {
    openSideBySideWorkspace(normalizeWorkspaceUrl(url), "hubspot_workspace", hubspotPopupRef);
  };

  const openWebsiteWorkspaceLink = (url: string) => {
    const normalized = normalizeWorkspaceUrl(url);
    const proxied = apiUrl(`/api/web/embed?url=${encodeURIComponent(normalized)}`);
    openSideBySideWorkspace(proxied, "website_workspace", hubspotPopupRef, "Organization Website");
  };

  const enrichRecipientEmailWithApollo = async (recipient: {
    label: string;
    persona: string;
    rationale: string;
  }) => {
    if (!result) return;
    setApolloEmailLoadingLabel(recipient.label);
    setApolloError(null);
    try {
      const parsed = parseRecipientLabel(recipient.label);
      const response = await fetch(apiUrl("/api/apollo/enrich-recipients"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          recipients: [
            {
              full_name: parsed.fullName,
              title: parsed.title,
            },
          ],
          organization_name: result.minimum_fields_used?.organization_name ?? null,
          organization_website: result.minimum_fields_used?.organization_website ?? null,
          reveal_personal_emails: false,
        }),
      });
      const body = (await response.json()) as ApolloEnrichResponse | { error?: string };
      if (!response.ok) {
        throw new Error(
          (body as { error?: string }).error ??
            `Apollo enrich failed (${response.status} ${response.statusText})`
        );
      }
      const payload = body as ApolloEnrichResponse;
      const key = normalizePersonKey(parsed.fullName);
      const match = payload.results.find((item) => normalizePersonKey(item.full_name) === key);
      setApolloByLabel((prev) => ({
        ...prev,
        [recipient.label]:
          match ??
          ({
            full_name: parsed.fullName,
            title: parsed.title,
            found: false,
            detail: "No Apollo match found",
          } satisfies ApolloRecipientResult),
      }));
    } catch (enrichError) {
      setApolloError(
        enrichError instanceof Error ? enrichError.message : "Apollo enrichment failed."
      );
    } finally {
      setApolloEmailLoadingLabel((prev) => (prev === recipient.label ? null : prev));
    }
  };

  const enrichRecipientPhoneWithApollo = async (recipient: {
    label: string;
    persona: string;
    rationale: string;
  }) => {
    if (!result) return;
    setApolloPhoneLoadingLabel(recipient.label);
    setApolloError(null);
    try {
      const parsed = parseRecipientLabel(recipient.label);
      const response = await fetch(apiUrl("/api/apollo/enrich-recipients"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          recipients: [
            {
              full_name: parsed.fullName,
              title: parsed.title,
            },
          ],
          organization_name: result.minimum_fields_used?.organization_name ?? null,
          organization_website: result.minimum_fields_used?.organization_website ?? null,
          reveal_personal_emails: false,
        }),
      });
      const body = (await response.json()) as ApolloEnrichResponse | { error?: string };
      if (!response.ok) {
        throw new Error(
          (body as { error?: string }).error ??
            `Apollo enrich failed (${response.status} ${response.statusText})`
        );
      }
      const payload = body as ApolloEnrichResponse;
      const key = normalizePersonKey(parsed.fullName);
      const match = payload.results.find((item) => normalizePersonKey(item.full_name) === key);
      setApolloByLabel((prev) => ({
        ...prev,
        [recipient.label]:
          match ??
          ({
            full_name: parsed.fullName,
            title: parsed.title,
            found: false,
            detail: "No Apollo match found",
          } satisfies ApolloRecipientResult),
      }));
    } catch (enrichError) {
      setApolloError(
        enrichError instanceof Error ? enrichError.message : "Apollo enrichment failed."
      );
    } finally {
      setApolloPhoneLoadingLabel((prev) => (prev === recipient.label ? null : prev));
    }
  };

  const loadWorkspaceData = async () => {
    if (!result) return;
    const requestSeq = ++workspaceRequestSeqRef.current;
    const isStale = () => requestSeq !== workspaceRequestSeqRef.current;
    setHubspotLoading(true);
    setHubspotError(null);
    setCaseStudyError(null);
    setHubspotContext(null);
    setCaseStudies(null);
    setApolloSnapshot(null);
    try {
      const orgName = result.minimum_fields_used?.organization_name ?? null;
      const orgWebsite = result.minimum_fields_used?.organization_website ?? null;
      const apolloSnapshotResp = await fetch(apiUrl("/api/apollo/account-snapshot"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          organization_name: orgName,
          organization_website: orgWebsite,
        }),
      });
      const apolloBody = (await apolloSnapshotResp.json()) as ApolloAccountSnapshotResponse & {
        error?: string;
      };
      if (isStale()) return;
      if (!apolloSnapshotResp.ok) {
        setApolloSnapshot({
          matched: false,
          message:
            apolloBody.error ??
            `Apollo account snapshot failed (${apolloSnapshotResp.status})`,
        });
      } else {
        setApolloSnapshot(apolloBody);
      }

      const apolloIndustry = readStringFromRecord(apolloBody.organization, "industry") || null;
      const apolloCity = readStringFromRecord(apolloBody.organization, "city") || null;
      const apolloState = readStringFromRecord(apolloBody.organization, "state") || null;
      const hubspotResp = await fetch(apiUrl("/api/hubspot/context"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          organization_name: orgName,
          organization_website: orgWebsite,
          organization_industry: apolloIndustry,
          organization_city: apolloCity,
          organization_state: apolloState,
          project_signal_text: [
            cleanNullable(formContext.awardId || result.minimum_fields_used?.award_id || null),
            cleanNullable(formContext.awardDescription || null),
          ]
            .filter(Boolean)
            .join(" | ") || null,
          max_items: 25,
          years_back: 5,
          closed_won_only: true,
        }),
      });
      const hubspotBody = (await hubspotResp.json()) as HubspotContextResponse & {
        error?: string;
        details?: unknown;
      };
      if (isStale()) return;
      if (!hubspotResp.ok) {
        const detailsText =
          Array.isArray(hubspotBody.details) && hubspotBody.details.length
            ? ` | ${hubspotBody.details.map((d) => String(d)).join(" ; ")}`
            : "";
        throw new Error(
          `${hubspotBody.error ?? `HubSpot context failed (${hubspotResp.status})`}${detailsText}`
        );
      }

      setHubspotContext(hubspotBody);
      const industryHint =
        apolloIndustry ||
        getPrimaryIndustryFromHubspot(hubspotBody);
      const caseResp = await fetch(apiUrl("/api/case-studies/recommend"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          organization_name: orgName,
          industry_vertical: industryHint,
          project_description:
            result.campaign.strategy_summary || result.campaign.campaign_title || null,
          max_items: 3,
        }),
      });
      const caseBody = (await caseResp.json()) as CaseStudyRecommendResponse & { error?: string };
      if (isStale()) return;
      if (!caseResp.ok) {
        setCaseStudyError(caseBody.error ?? `Case study lookup failed (${caseResp.status})`);
      } else {
        const normalizedItems = normalizeRecommendedAssets(caseBody.items);
        setCaseStudies({ ...caseBody, items: normalizedItems });
      }
      setActiveTab("overview");
    } catch (hubspotLoadError) {
      if (isStale()) return;
      setHubspotContext(null);
      setCaseStudies(null);
      setHubspotError(
        hubspotLoadError instanceof Error
          ? hubspotLoadError.message
          : "HubSpot context lookup failed."
      );
      setActiveTab("overview");
    } finally {
      if (isStale()) return;
      setHubspotLoading(false);
    }
  };

  const findDomainOnly = async () => {
    if (!result) return;
    setApolloDomainLoading(true);
    setApolloError(null);
    try {
      const orgName = result.minimum_fields_used?.organization_name ?? null;
      const orgWebsite = result.minimum_fields_used?.organization_website ?? null;
      const response = await fetch(apiUrl("/api/apollo/account-snapshot"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          organization_name: orgName,
          organization_website: orgWebsite,
        }),
      });
      const body = (await response.json()) as ApolloAccountSnapshotResponse & { error?: string };
      if (!response.ok) {
        throw new Error(body.error ?? `Apollo account snapshot failed (${response.status})`);
      }
      setApolloSnapshot(body);
    } catch (domainError) {
      setApolloError(domainError instanceof Error ? domainError.message : "Domain lookup failed.");
    } finally {
      setApolloDomainLoading(false);
    }
  };

  return (
    <div
      className={`campaign-modal-overlay${splitMode ? " split-mode" : ""}`}
    >
      <div
        className={`campaign-modal${splitMode ? " split-mode" : ""}`}
        onClick={(event) => event.stopPropagation()}
      >
        <button
          className="campaign-modal-close"
          type="button"
          onClick={() => {
            closeModalAndApollo();
          }}
        >
          X
        </button>

        {loading ? (
          <>
            <div className="campaign-modal-header">
              <h3>Generating Campaign...</h3>
              <span className="thinking-spinner" />
            </div>
            <div className="campaign-modal-body campaign-thinking">
              <ul className="thinking-log">
                {THINKING_STEPS.slice(0, visibleSteps).map((step, index) => (
                  <li
                    key={step}
                    className={`thinking-step ${
                      index === THINKING_STEPS.length - 1 ? "is-complete" : ""
                    }`}
                    style={{ animationDelay: `${index * 150}ms` }}
                  >
                    {STEP_PREFIXES[index] ?? ""}
                    {step}
                  </li>
                ))}
              </ul>
            </div>
          </>
        ) : error ? (
          <>
            <div className="campaign-modal-header">
              <h3>Generation Error</h3>
            </div>
            <div className="campaign-modal-body">
              <p className="status-error">{error}</p>
            </div>
          </>
        ) : result ? (
          <>
            <div className="campaign-modal-header">
              <h3>{result.campaign.campaign_title}</h3>
            </div>
            <div className="campaign-modal-body campaign-output" ref={modalBodyRef}>
              {hubspotLoading ? (
                <section className="workspace-loading-screen">
                  <p className="status-loading">Preparing account workspace. Please wait...</p>
                  <ul className="thinking-log">
                    {WORKSPACE_LOADING_STEPS.map((step, index) => {
                      const relative = (index - workspaceStepIndex + WORKSPACE_LOADING_STEPS.length) %
                        WORKSPACE_LOADING_STEPS.length;
                      const isCurrent = relative === 0;
                      const isUpcoming = relative > 0 && relative <= 2;
                      return (
                        <li
                          key={`workspace-step-${step}`}
                          className={`thinking-step workspace-step${
                            isCurrent ? " is-current" : isUpcoming ? " is-upcoming" : ""
                          }`}
                        >
                          {isCurrent ? "\u{1F504} " : WORKSPACE_STEP_PREFIXES[index] ?? "\u{2022} "}
                          {step}
                        </li>
                      );
                    })}
                  </ul>
                </section>
              ) : (
                <>
                  <div className="workspace-tabs top-tabs">
                    {[
                      ["overview", "Overview"],
                      ["hubspot", "HubSpot"],
                      ["email", "Email"],
                      ["phone", "Phone Call"],
                    ].map(([key, label]) => (
                      <button
                        key={`top-${key}`}
                        className={`workspace-tab${activeTab === key ? " is-active" : ""}`}
                        type="button"
                        onClick={() => setActiveTab(key as WorkspaceTab)}
                      >
                        {label}
                      </button>
                    ))}
                  </div>

                  {hubspotError ? <p className="status-error">{hubspotError}</p> : null}
                  {apolloHealthMessage ? <p className="muted">{apolloHealthMessage}</p> : null}

                  {activeTab === "overview" ? (
                    <>
      <section className="campaign-section">
                        <h4>Executive Pitch Summary</h4>
                        <ExecutivePitchSummary summary={result.campaign.strategy_summary} />
                      </section>
                      <section className="campaign-section account-snapshot-section">
                        <h4>Account Snapshot</h4>
                        <ApolloSnapshotCard
                          snapshot={apolloSnapshot}
                          onFindDomain={findDomainOnly}
                          findingDomain={apolloDomainLoading}
                          onOpenWorkspaceLink={openWebsiteWorkspaceLink}
                        />
                      </section>
                    </>
                  ) : null}

                  {activeTab === "hubspot" ? (
                    <>
                      <section className="campaign-section">
                        <h4>HubSpot Context</h4>
                        {hubspotContext ? (
                          <HubspotDashboard
                            context={hubspotContext}
                            snapshot={apolloSnapshot}
                            onOpenWorkspaceLink={openHubspotWorkspaceLink}
                            onOpenWebsiteLink={openWebsiteWorkspaceLink}
                          />
                        ) : (
                          <p className="muted">Load account context to view CRM data.</p>
                        )}
                      </section>
                      <section className="campaign-section">
                        <h4>HubSpot Recommended Assets</h4>
                        {caseStudyError ? <p className="status-error">{caseStudyError}</p> : null}
                        <p className="muted">
                          Open any asset, download it from the resource, then attach manually in Apollo.
                        </p>
                        <CaseStudiesPanel
                          data={caseStudies}
                          onOpenWorkspaceLink={openHubspotWorkspaceLink}
                        />
                      </section>
                    </>
                  ) : null}

                  {activeTab === "email" ? (
                    <>
                      <section className="campaign-section">
                        <h4>Target Stakeholders</h4>
                        {apolloError ? <p className="status-error">{apolloError}</p> : null}
                        <div className="stakeholder-list">
                          {result.campaign.recipients.map((recipient) => (
                            <article key={recipient.label} className="stakeholder-card">
                              <div className="stakeholder-card-header">
                                <p>
                                  <strong>{recipient.label}</strong>
                                </p>
                                <button
                                  className="action-btn-secondary"
                                  type="button"
                                  onClick={() => {
                                    void enrichRecipientEmailWithApollo(recipient);
                                  }}
                                  disabled={!apolloAvailable || apolloEmailLoadingLabel === recipient.label}
                                >
                                  {apolloEmailLoadingLabel === recipient.label
                                    ? "Fetching Email..."
                                    : "Get Email from Apollo"}
                                </button>
                              </div>
                              <p className="muted">{recipient.persona}</p>
                              <p>{recipient.rationale}</p>
                              {apolloByLabel[recipient.label]?.email ? (
                                <p>
                                  <strong>Email:</strong> {apolloByLabel[recipient.label].email}
                                </p>
                              ) : apolloByLabel[recipient.label] ? (
                                <p className="muted">
                                  Apollo: no email found
                                  {apolloByLabel[recipient.label].detail
                                    ? ` (${apolloByLabel[recipient.label].detail})`
                                    : ""}
                                </p>
                              ) : null}
                            </article>
                          ))}
                        </div>
                      </section>
                      <section className="campaign-section">
                        <h4>Personalized Email Sequences</h4>
                        {emailRegenerateError ? <p className="status-error">{emailRegenerateError}</p> : null}
                        <div className="email-list">
                          {result.campaign.prospect_campaigns.map((sequence) => (
                            <article
                              key={`${sequence.recipient_label}-${sequence.recipient_persona}`}
                              className="email-card"
                            >
                              <h5>{sequence.recipient_label}</h5>
                              <p className="muted">
                                {sequence.recipient_persona} - {sequence.recipient_rationale}
                              </p>
                              {sequence.emails.map((email) => (
                                <div
                                  key={`${sequence.recipient_label}-${email.email_number}`}
                                  className="email-block"
                                >
                                  <p className="email-subject-line">
                                    <strong>
                                      Email {email.email_number}: {email.subject}
                                    </strong>
                                  </p>
                                  <p>{email.body}</p>
                                  <div className="email-copy-row">
                                    <button
                                      className="action-btn-secondary"
                                      type="button"
                                      onClick={() =>
                                        copyEmail(
                                          sequence.recipient_label,
                                          email.email_number,
                                          email.subject,
                                          email.body
                                        )
                                      }
                                    >
                                      {copiedEmailKey ===
                                      `${sequence.recipient_label}-${email.email_number}`
                                        ? "Copied"
                                        : "Copy Email"}
                                    </button>
                                    <button
                                      className="action-btn-secondary"
                                      type="button"
                                      onClick={() => {
                                        void regenerateEmail(sequence, email.email_number);
                                      }}
                                      disabled={
                                        regeneratingEmailKey ===
                                        `${sequence.recipient_label}-${email.email_number}`
                                      }
                                    >
                                      {regeneratingEmailKey ===
                                      `${sequence.recipient_label}-${email.email_number}`
                                        ? "Regenerating..."
                                        : "Regenerate"}
                                    </button>
                                  </div>
                                </div>
                              ))}
                            </article>
                          ))}
                        </div>
                      </section>
                    </>
                  ) : null}

                  {activeTab === "phone" ? (
                    <>
                      <section className="campaign-section">
                        <h4>Target Stakeholders</h4>
                        {apolloError ? <p className="status-error">{apolloError}</p> : null}
                        <div className="stakeholder-list">
                          {result.campaign.recipients.map((recipient) => (
                            <article key={`phone-${recipient.label}`} className="stakeholder-card">
                              <div className="stakeholder-card-header">
                                <p>
                                  <strong>{recipient.label}</strong>
                                </p>
                                <button
                                  className="action-btn-secondary"
                                  type="button"
                                  onClick={() => {
                                    void enrichRecipientPhoneWithApollo(recipient);
                                  }}
                                  disabled={!apolloAvailable || apolloPhoneLoadingLabel === recipient.label}
                                >
                                  {apolloPhoneLoadingLabel === recipient.label
                                    ? "Fetching Phone..."
                                    : "Get Phone from Apollo"}
                                </button>
                              </div>
                              <p className="muted">{recipient.persona}</p>
                              <p>{recipient.rationale}</p>
                              {apolloByLabel[recipient.label]?.phone ? (
                                <p>
                                  <strong>Phone:</strong> {apolloByLabel[recipient.label].phone}
                                </p>
                              ) : apolloByLabel[recipient.label] ? (
                                <p className="muted">
                                  Apollo: no phone found
                                  {apolloByLabel[recipient.label].detail
                                    ? ` (${apolloByLabel[recipient.label].detail})`
                                    : ""}
                                </p>
                              ) : null}
                            </article>
                          ))}
                        </div>
                      </section>

                      <section className="campaign-section">
                        <h4>Phone Call Prep</h4>
                        <PhoneCallPanel
                          result={result}
                          context={hubspotContext}
                          snapshot={apolloSnapshot}
                          callerPhrase={callerPhrase}
                        />
                      </section>
                    </>
                  ) : null}
                </>
              )}
            </div>
            {!hubspotLoading ? (
              <div className="campaign-modal-actions">
                <button
                  className="workspace-tab campaign-action-pill"
                  type="button"
                  onClick={() => {
                    void loadWorkspaceData();
                  }}
                >
                  Refresh CRM Context
                </button>
                <button className="workspace-tab campaign-action-pill" type="button" onClick={openApolloAccount}>
                  Open Apollo
                </button>
                <button className="workspace-tab campaign-action-pill" type="button" onClick={exportPdf}>
                  Export as PDF
                </button>
                <button
                  className="workspace-tab campaign-action-pill"
                  type="button"
                  onClick={() => {
                    closeModalAndApollo();
                  }}
                >
                  Close
                </button>
              </div>
            ) : null}
          </>
        ) : null}
      </div>
      {splitMode && embeddedWorkspaceUrl ? (
        <aside className="workspace-sidecar" onClick={(event) => event.stopPropagation()}>
          <div className="workspace-sidecar-header">
            <h4>{embeddedWorkspaceTitle}</h4>
            <div className="workspace-sidecar-actions">
              <button className="workspace-tab" type="button" onClick={openEmbeddedWorkspaceInNewWindow}>
                Open in New Window
              </button>
              <button className="workspace-tab" type="button" onClick={closeEmbeddedWorkspace}>
                Close Pane
              </button>
            </div>
          </div>
          <iframe
            className="workspace-sidecar-frame"
            src={embeddedWorkspaceUrl}
            title={embeddedWorkspaceTitle}
            referrerPolicy="strict-origin-when-cross-origin"
            sandbox="allow-same-origin allow-scripts allow-forms allow-popups allow-downloads"
          />
        </aside>
      ) : null}
    </div>
  );
}

function HubspotDashboard({
  context,
  snapshot,
  onOpenWorkspaceLink,
  onOpenWebsiteLink,
}: {
  context: HubspotContextResponse;
  snapshot: ApolloAccountSnapshotResponse | null;
  onOpenWorkspaceLink: (url: string) => void;
  onOpenWebsiteLink: (url: string) => void;
}) {
  const errors = Array.isArray(context.errors) ? context.errors : [];
  const accountInsight = buildAccountInsight(context);
  const recommendedAction = readRecommendedAction(context);
  const hasConfidentMatch = accountInsight.confidentMatch && Boolean(accountInsight.suggestedCompanyName);
  const hasAnyMatch = accountInsight.matched;
  const canConfirmAccount = accountInsight.confirmable && Boolean(accountInsight.suggestedCompanyName);
  const companyDomainHref = accountInsight.suggestedCompanyDomain
    ? "https://" + accountInsight.suggestedCompanyDomain
    : "";
  const apolloIndustry = readStringFromRecord(snapshot?.organization ?? null, "industry");
  const trustedHubspotIndustry = getPrimaryIndustryFromHubspot(context);
  const industryTarget =
    apolloIndustry ||
    trustedHubspotIndustry ||
    "";
  const dealBriefs = buildRelevantDealBriefs(context, industryTarget);
  const outreachBrief = buildOutreachBrief(context, dealBriefs);

  return (
    <div className="hubspot-dashboard">
      {!hasAnyMatch ? (
        <section className="hubspot-card">
          <div className="hubspot-empty-match">
            <span className="hubspot-empty-match-icon" aria-hidden="true">
              âœ…
            </span>
            <p className="hubspot-empty-match-text">No Company Match Found</p>
          </div>
        </section>
      ) : null}

      {hasAnyMatch ? (
        <section className="hubspot-card">
          <h5>Account Match</h5>
          <p className={`hubspot-pill ${hasConfidentMatch ? "is-ok" : "is-warn"}`}>
            {hasConfidentMatch
              ? "Matched"
              : canConfirmAccount
                ? "Needs Review"
                : "No Confirmable Record"}
          </p>

          <div className="account-match-layout">
            <div className="account-match-primary">
              <p className="account-match-label">Company Name</p>
              <p className="account-match-name">{accountInsight.suggestedCompanyName || "Not available"}</p>
            </div>
            <div className="account-match-meta">
              <div className="account-match-item">
                <p className="account-match-item-label">Website</p>
                {companyDomainHref ? (
                  <a
                    href={companyDomainHref}
                    className="account-match-inline-link"
                    onClick={(event) => {
                      event.preventDefault();
                      onOpenWebsiteLink(companyDomainHref);
                    }}
                  >
                    {accountInsight.suggestedCompanyDomain}
                  </a>
                ) : (
                  <p className="muted">Not available</p>
                )}
              </div>
              <div className="account-match-item">
                <p className="account-match-item-label">Record</p>
                {canConfirmAccount && accountInsight.suggestedCompanyUrl ? (
                  <a
                    href={accountInsight.suggestedCompanyUrl}
                    className="account-match-inline-link"
                    onClick={(event) => {
                      event.preventDefault();
                      onOpenWorkspaceLink(accountInsight.suggestedCompanyUrl);
                    }}
                  >
                    Open Matched Company Record
                  </a>
                ) : (
                  <p className="muted">Not available</p>
                )}
              </div>
            </div>
          </div>
          {!hasConfidentMatch && !canConfirmAccount ? (
            <p className="muted">
              HubSpot returned search signal but no concrete company record to open. Refresh CRM context after verifying name/domain.
            </p>
          ) : null}
        </section>
      ) : null}

      {recommendedAction ? (
        <section className="hubspot-card">
          <h5>Recommended Action</h5>
          <p className="hubspot-pill is-warn">{recommendedAction.label}</p>
          <div className="hubspot-kv-grid">
            <p>
              <strong>Why:</strong> {recommendedAction.rationale}
            </p>
            <p>
              <strong>Next Step:</strong> {recommendedAction.nextStep}
            </p>
            <p>
              <strong>Do Not Do:</strong> {recommendedAction.doNotDo}
            </p>
          </div>
        </section>
      ) : null}

      <section className="hubspot-card">
        <h5>Industry Signal</h5>
        <div className="hubspot-kv-grid">
          <p>
            <strong>Industry:</strong> {formatIndustryLabel(industryTarget) || "Not available"}
          </p>
          <p>
            <strong>Source:</strong> {apolloIndustry ? "Apollo" : trustedHubspotIndustry ? "HubSpot" : "N/A"}
          </p>
        </div>
      </section>

      {hasConfidentMatch ? (
        <>
          <section className="hubspot-card">
            <h5>Account Deals ({dealBriefs.length})</h5>
            <p className="muted">
              Showing up to 5 most recent matched-account deals (open, closed-won, closed-lost):
              {industryTarget ? ` ${industryTarget}` : " account-related context"}.
            </p>
            {dealBriefs.length ? (
              <ul className="hubspot-list">
                {dealBriefs.map((deal) => (
                  <li key={deal.id || `${deal.name}-${deal.stage}`} className="hubspot-list-item">
                    <div>
                      <strong>{deal.name}</strong>
                      <p className="muted">
                        {deal.stage} | Amount: {deal.amount} | Next: {deal.nextStepDate} | Last activity: {deal.lastActivityDate}
                      </p>
                    </div>
                    {deal.url ? (
                      <a
                        href={deal.url}
                        className="hubspot-link"
                        onClick={(event) => {
                          event.preventDefault();
                          onOpenWorkspaceLink(deal.url);
                        }}
                      >
                        Open
                      </a>
                    ) : null}
                  </li>
                ))}
              </ul>
            ) : (
              <p className="muted">No deal signal found for this account yet.</p>
            )}
          </section>

          <section className="hubspot-card">
            <h5>Outreach-First Brief</h5>
            <div className="hubspot-kv-grid">
              <p>
                <strong>Why Now:</strong> {outreachBrief.whyNow}
              </p>
              <p>
                <strong>Who To Message:</strong> {outreachBrief.whoToMessage}
              </p>
              <p>
                <strong>What To Reference:</strong> {outreachBrief.whatToReference}
                {outreachBrief.referenceUrl ? (
                  <>
                    {" "}
                    <a
                      href={outreachBrief.referenceUrl}
                      className="hubspot-link"
                      onClick={(event) => {
                        event.preventDefault();
                        onOpenWorkspaceLink(outreachBrief.referenceUrl as string);
                      }}
                    >
                      Open ROM
                    </a>
                  </>
                ) : null}
              </p>
            </div>
          </section>
        </>
      ) : null}

      {errors.length ? (
        <section className="hubspot-card">
          <h5>HubSpot Warnings</h5>
          <ul className="hubspot-errors">
            {errors.map((error, index) => (
              <li key={`${error}-${index}`}>{error}</li>
            ))}
          </ul>
        </section>
      ) : null}
    </div>
  );
}

type HubspotCompanyRaw = {
  id: string;
  name: string;
  domain: string;
  url: string;
  properties: Record<string, unknown>;
};

type HubspotContactRaw = {
  id: string;
  name: string;
  title: string;
  company: string;
  url: string;
  properties: Record<string, unknown>;
};

type HubspotDealRaw = {
  id: string;
  name: string;
  stage: string;
  pipeline: string;
  amount: string;
  closeDate: string;
  createdAt: string;
  updatedAt: string;
  url: string;
  properties: Record<string, unknown>;
};

function getHubspotCompanyRawList(context: HubspotContextResponse | null): HubspotCompanyRaw[] {
  const root = asRecord(context?.companies);
  const list = asArray(root?.companies);
  return list
    .map((item) => {
      const row = asRecord(item);
      const props = asRecord(row?.properties) ?? {};
      return {
        id: asString(row?.id),
        name: asString(props.name),
        domain: asString(props.domain),
        url: asString(row?.url),
        properties: props,
      };
    })
    .filter((row) => row.id || row.name);
}

function getHubspotContactRawList(context: HubspotContextResponse | null): HubspotContactRaw[] {
  const root = asRecord(context?.contacts);
  const list = asArray(root?.contacts);
  return list
    .map((item) => {
      const row = asRecord(item);
      const props = asRecord(row?.properties) ?? {};
      const first = asString(props.firstname);
      const last = asString(props.lastname);
      return {
        id: asString(row?.id),
        name: asString(props.name) || [first, last].filter(Boolean).join(" ").trim(),
        title: asString(props.jobtitle) || asString(props.title),
        company: asString(props.company),
        url: asString(row?.url),
        properties: props,
      };
    })
    .filter((row) => row.id || row.name);
}

function getHubspotDealRawList(context: HubspotContextResponse | null): HubspotDealRaw[] {
  const root = asRecord(context?.deals);
  const list = asArray(root?.deals);
  return list
    .map((item) => {
      const row = asRecord(item);
      const props = asRecord(row?.properties) ?? {};
      return {
        id: asString(row?.id),
        name: asString(props.dealname),
        stage: asString(props.dealstage_label) || asString(props.dealstage),
        pipeline: asString(props.pipeline_label) || asString(props.pipeline),
        amount: asString(props.amount),
        closeDate: asString(props.closedate),
        createdAt: asString(row?.createdAt) || asString(props.createdate) || asString(props.hs_createdate),
        updatedAt:
          asString(row?.updatedAt) || asString(props.hs_lastmodifieddate) || asString(props.updatedate),
        url: asString(row?.url),
        properties: props,
      };
    })
    .filter((row) => row.id || row.name);
}

function normalizeDomain(value: string): string {
  const lower = value.trim().toLowerCase();
  if (!lower) return "";
  return lower.startsWith("www.") ? lower.slice(4) : lower;
}

function pickFirstValue(record: Record<string, unknown>, keys: string[]): string {
  for (const key of keys) {
    const value = asString(record[key]);
    if (value) return value;
  }
  return "";
}

function isClosedLostStage(stage: string): boolean {
  const normalized = stage.toLowerCase();
  return normalized.includes("closedlost") || normalized.includes("closed_lost") || normalized.includes("lost");
}

function isClosedWonStage(stage: string): boolean {
  const normalized = stage.toLowerCase();
  return normalized.includes("closedwon") || normalized.includes("closed_won") || normalized.includes("won");
}

function formatShortDate(value: string): string {
  if (!value) return "N/A";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleDateString();
}

function isHttpUrl(value: string): boolean {
  if (!value) return false;
  try {
    const parsed = new URL(value);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

function extractFirstHttpUrl(value: string): string {
  if (!value) return "";
  const match = value.match(/https?:\/\/[^\s|)\]]+/i);
  const candidate = match ? match[0] : "";
  return isHttpUrl(candidate) ? candidate : "";
}

function daysSince(value: string): number | null {
  if (!value) return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  const now = new Date();
  const diff = now.getTime() - parsed.getTime();
  return diff >= 0 ? Math.floor(diff / (1000 * 60 * 60 * 24)) : 0;
}

function buildAccountInsight(context: HubspotContextResponse | null): {
  matched: boolean;
  confidentMatch: boolean;
  confirmable: boolean;
  confidence: string;
  matchBasis: string;
  owner: string;
  ownerId: string;
  suggestedCompanyName: string;
  suggestedCompanyDomain: string;
  suggestedCompanyUrl: string;
  candidateCount: number;
} {
  const accountMatch = readRecord(context?.account_match);
  const matched = Boolean(accountMatch?.matched);
  const method = asString(accountMatch?.method) || "none";
  const reason = asString(accountMatch?.reason) || "No usable account-level match";
  const confidenceValue = Number(accountMatch?.confidence);

  const selected = readRecord(accountMatch?.selected_company);
  const suggestedCompanyName =
    asString(selected?.name) || asString(accountMatch?.selected_name) || asString(accountMatch?.company_name);
  const suggestedCompanyDomain = normalizeDomain(
    asString(selected?.domain) || asString(accountMatch?.selected_domain) || asString(accountMatch?.company_domain)
  );
  const suggestedCompanyUrl = asString(selected?.url) || asString(accountMatch?.selected_company_url);

  const ownerId = asString(selected?.owner_id) || asString(selected?.hubspot_owner_id);
  const ownerName = asString(selected?.owner_name) || asString(selected?.owner);
  const owner = ownerName || (ownerId ? `Owner name unavailable (ID ${ownerId})` : "Unassigned");

  const hasRealCompanyId = Boolean(asString(selected?.id));
  const hasRealCompanyName = Boolean(suggestedCompanyName);
  const companyType = normalizeText(asString(selected?.company_type));
  const isSynthetic =
    Boolean(selected?.synthetic) ||
    companyType.includes("synthetic") ||
    companyType.includes("domain_candidate") ||
    companyType.includes("ghost");

  const hasConfirmableFlag = Object.prototype.hasOwnProperty.call(accountMatch ?? {}, "confirmable");
  const backendConfirmable = accountMatch?.confirmable === true;
  const fallbackConfirmable = hasRealCompanyId && hasRealCompanyName && !isSynthetic && Boolean(suggestedCompanyUrl);
  const confirmable = hasConfirmableFlag ? backendConfirmable : fallbackConfirmable;
  const confidentMatch = accountMatch?.confident_match === true && confirmable;

  const confidence = Number.isFinite(confidenceValue)
    ? `${Math.round(Math.max(0, Math.min(1, confidenceValue)) * 100)}%`
    : matched
      ? "Matched"
      : "None";

  const exact = readRecord(context?.exact_matches);
  const companies = exact && Array.isArray(exact.companies) ? (exact.companies as unknown[]) : [];
  const candidateCount = companies.length;

  return {
    matched,
    confidentMatch,
    confirmable,
    confidence,
    matchBasis: `${method} | ${reason}`,
    owner,
    ownerId,
    suggestedCompanyName,
    suggestedCompanyDomain,
    suggestedCompanyUrl,
    candidateCount,
  };
}

function readRecommendedAction(
  context: HubspotContextResponse | null
): { label: string; rationale: string; nextStep: string; doNotDo: string } | null {
  const action = readRecord(context?.recommended_action);
  if (!action) return null;

  const label = asString(action.label) || "Recommended Action";
  const rationale = asString(action.rationale) || asString(action.why_now) || "No rationale provided";
  const nextStep = asString(action.next_step) || asString(action.nextStep) || "No next step provided";
  const doNotDo = asString(action.do_not_do) || asString(action.doNotDo) || "No guardrail provided";

  if (!label && !rationale && !nextStep && !doNotDo) return null;
  return { label, rationale, nextStep, doNotDo };
}
function buildActivePipelineInsight(context: HubspotContextResponse | null): {
  openCount: number;
  nextStepDate: string;
  lastActivity: string;
  lastActivityOwner: string;
  sequenceStatus: string;
  openDeals: Array<{
    id: string;
    name: string;
    stage: string;
    ageInStageDays: number | string;
    nextStepDate: string;
    url: string;
  }>;
} {
  const deals = getHubspotDealRawList(context);
  const contacts = getHubspotContactRawList(context);
  const openDeals = deals
    .filter((deal) => {
      const stage = deal.stage.toLowerCase();
      if (!stage) return true;
      return !isClosedWonStage(stage) && !isClosedLostStage(stage);
    })
    .map((deal) => {
      const props = deal.properties;
      const ageDays =
        daysSince(asString(props.hs_lastmodifieddate) || deal.updatedAt || deal.createdAt) ?? "N/A";
      const nextStepDate =
        formatShortDate(
          pickFirstValue(props, ["hs_next_activity_date", "hs_next_step_date", "next_activity_date"])
        ) || "N/A";
      return {
        id: deal.id,
        name: deal.name || "Open Deal",
        stage: deal.stage || "Stage unknown",
        ageInStageDays: ageDays,
        nextStepDate,
        url: deal.url,
      };
    });

  const latestDeal = [...deals]
    .sort((a, b) => {
      const aTime = new Date(a.updatedAt || a.createdAt || 0).getTime();
      const bTime = new Date(b.updatedAt || b.createdAt || 0).getTime();
      return bTime - aTime;
    })
    .find((deal) => deal.updatedAt || deal.createdAt);
  const latestDealProps = latestDeal?.properties ?? {};
  const lastActivityDate = pickFirstValue(latestDealProps, ["hs_last_activity_date", "lastactivitydate"]);
  const lastActivityOwner =
    pickFirstValue(latestDealProps, ["hubspot_owner_id", "hs_owner_id", "owner_id"]) || "Unknown";
  const nextStepAcrossDeals = openDeals.find((deal) => deal.nextStepDate !== "N/A")?.nextStepDate ?? "N/A";

  const sequenceCount = contacts.reduce((sum, contact) => {
    const raw = pickFirstValue(contact.properties, [
      "hs_sequences_active_enrollment_count",
      "sequences_active_enrollment_count",
      "sequence_enrollment_count",
    ]);
    const numeric = Number(raw);
    return Number.isFinite(numeric) ? sum + numeric : sum;
  }, 0);

  return {
    openCount: openDeals.length,
    nextStepDate: nextStepAcrossDeals,
    lastActivity: lastActivityDate ? formatShortDate(lastActivityDate) : "Not available",
    lastActivityOwner,
    sequenceStatus:
      sequenceCount > 0
        ? `${sequenceCount} active enrollment${sequenceCount === 1 ? "" : "s"}`
        : "No active sequence data",
    openDeals,
  };
}

function buildHistoricalWinInsight(context: HubspotContextResponse | null): {
  closedWonCount: number;
  salesCycle: string;
  motionTypes: string;
  stakeholderTitles: string;
  objections: string;
  proofPoints: string;
  closedWonDeals: HubspotDealItem[];
} {
  const dealsRaw = getHubspotDealRawList(context);
  const contactsRaw = getHubspotContactRawList(context);
  const deals = getHubspotDeals(context?.deals ?? null);
  const similarWins = getSimilarWins(context);

  const closedWonRaw = dealsRaw.filter((deal) => isClosedWonStage(deal.stage));
  const closedWonDeals = deals.filter((deal) => isClosedWonStage(deal.stage)).slice(0, 8);
  const fallbackClosedWon = similarWins.map((deal) => ({
    id: deal.id,
    name: deal.deal_name || "Closed Won Deal",
    amount: deal.amount ? `$${deal.amount}` : "No amount",
    stage: deal.dealstage || "Closed Won",
    closeDate: formatShortDate(deal.close_date),
    url: deal.url,
  }));
  const displayDeals = closedWonDeals.length ? closedWonDeals : fallbackClosedWon;

  const cycleDays = closedWonRaw
    .map((deal) => {
      const created = new Date(deal.createdAt || 0).getTime();
      const closed = new Date(deal.closeDate || 0).getTime();
      if (!created || !closed || closed < created) return null;
      return Math.floor((closed - created) / (1000 * 60 * 60 * 24));
    })
    .filter((days): days is number => Number.isFinite(days));
  const avgCycle = cycleDays.length
    ? `${Math.round(cycleDays.reduce((sum, days) => sum + days, 0) / cycleDays.length)} days avg`
    : "Not enough close/create dates";

  const motionCounts = new Map<string, number>();
  for (const deal of closedWonRaw) {
    const motion =
      pickFirstValue(deal.properties, ["dealtype", "motion_type", "pipeline"]) || deal.pipeline || "Unknown";
    motionCounts.set(motion, (motionCounts.get(motion) ?? 0) + 1);
  }
  const topMotions = [...motionCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3)
    .map(([motion, count]) => `${motion} (${count})`);

  const titleCounts = new Map<string, number>();
  for (const contact of contactsRaw) {
    if (!contact.title) continue;
    titleCounts.set(contact.title, (titleCounts.get(contact.title) ?? 0) + 1);
  }
  const topTitles = [...titleCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 4)
    .map(([title, count]) => `${title} (${count})`);

  const objectionText = collectDealTextSignals(closedWonRaw, [
    "common_objections",
    "objection",
    "objections",
    "objection_notes",
  ]);
  const proofText = collectDealTextSignals(closedWonRaw, [
    "proof_points",
    "value_proposition",
    "business_case",
    "decision_criteria",
  ]);

  return {
    closedWonCount: displayDeals.length,
    salesCycle: avgCycle,
    motionTypes: topMotions.length ? topMotions.join(", ") : "Not captured",
    stakeholderTitles: topTitles.length ? topTitles.join(", ") : "Not captured",
    objections: objectionText.length ? objectionText.join(" | ") : "Not captured",
    proofPoints: proofText.length ? proofText.join(" | ") : "Not captured",
    closedWonDeals: displayDeals,
  };
}

function collectDealTextSignals(deals: HubspotDealRaw[], keys: string[]): string[] {
  const values: string[] = [];
  for (const deal of deals) {
    for (const key of keys) {
      const value = asString(deal.properties[key]);
      if (value) values.push(value);
    }
  }
  return [...new Set(values)].slice(0, 3);
}

type DealBrief = {
  id: string;
  name: string;
  stage: string;
  nextStepDate: string;
  lastActivityDate: string;
  url: string;
  stageAgeDays: number | null;
  amount: string;
  motion: string;
};

function getMatchedCompanyIdentity(context: HubspotContextResponse | null): {
  id: string;
  name: string;
  domain: string;
} | null {
  const accountMatch = readRecord(context?.account_match);
  const selected = readRecord(accountMatch?.selected_company);
  const id = asString(selected?.id) || asString(accountMatch?.selected_company_id);
  const name = normalizeText(
    asString(selected?.name) || asString(accountMatch?.selected_name) || asString(accountMatch?.company_name)
  );
  const domain = normalizeDomain(
    asString(selected?.domain) || asString(accountMatch?.selected_domain) || asString(accountMatch?.company_domain)
  );
  if (!id && !name && !domain) return null;
  return { id, name, domain };
}

function dealMatchesMatchedCompany(
  deal: HubspotDealRaw,
  matchedCompany: { id: string; name: string; domain: string } | null
): boolean {
  if (!matchedCompany) return true;

  const props = deal.properties;
  const dealCompanyId = pickFirstValue(props, [
    "hubspot_company_id",
    "hs_company_id",
    "associatedcompanyid",
    "associated_company_id",
    "company_id",
  ]);
  if (matchedCompany.id && dealCompanyId && normalizeText(dealCompanyId) === normalizeText(matchedCompany.id)) {
    return true;
  }

  const dealCompanyDomain = normalizeDomain(
    pickFirstValue(props, ["company_domain", "domain", "associated_company_domain"])
  );
  if (matchedCompany.domain && dealCompanyDomain && dealCompanyDomain === matchedCompany.domain) {
    return true;
  }

  const dealCompanyName = normalizeText(
    pickFirstValue(props, ["company", "associated_company", "associatedcompany", "account_name"])
  );
  if (
    matchedCompany.name &&
    dealCompanyName &&
    (dealCompanyName.includes(matchedCompany.name) || matchedCompany.name.includes(dealCompanyName))
  ) {
    return true;
  }

  const dealName = normalizeText(deal.name);
  if (matchedCompany.name && dealName && dealName.includes(matchedCompany.name)) {
    return true;
  }

  return false;
}


function contactMatchesMatchedCompany(
  contact: HubspotContactRaw,
  matchedCompany: { id: string; name: string; domain: string } | null
): boolean {
  if (!matchedCompany) return true;

  const props = contact.properties;
  const contactCompanyId = pickFirstValue(props, [
    "hubspot_company_id",
    "hs_company_id",
    "associatedcompanyid",
    "associated_company_id",
    "company_id",
  ]);
  if (matchedCompany.id && contactCompanyId && normalizeText(contactCompanyId) === normalizeText(matchedCompany.id)) {
    return true;
  }

  const contactDomain = normalizeDomain(
    pickFirstValue(props, ["company_domain", "domain", "email_domain", "associated_company_domain"])
  );
  if (matchedCompany.domain && contactDomain && contactDomain === matchedCompany.domain) {
    return true;
  }

  const companyName = normalizeText(contact.company || pickFirstValue(props, ["company", "associated_company"]));
  if (
    matchedCompany.name &&
    companyName &&
    (companyName.includes(matchedCompany.name) || matchedCompany.name.includes(companyName))
  ) {
    return true;
  }

  return false;
}

function scoreContactForIcp(contact: HubspotContactRaw, dealBriefs: DealBrief[]): number {
  const title = normalizeText(contact.title);
  const topDeal = dealBriefs[0];
  const dealSignalText = normalizeText(
    `${topDeal?.name || ""} ${topDeal?.motion || ""} ${topDeal?.stage || ""}`
  );
  let score = 0;

  if (/(chief|cfo|ceo|coo|cio|cto|president|owner)/.test(title)) score += 8;
  if (/(svp|vice president|vp|director|head)/.test(title)) score += 6;
  if (/(manager|lead|program)/.test(title)) score += 4;
  if (/(specialist|analyst|coordinator)/.test(title)) score += 2;

  if (/(finance|procurement|sourcing|contracts|acquisition)/.test(title)) score += 4;
  if (/(it|technology|information|systems|digital|security|data)/.test(title)) score += 4;
  if (/(operations|implementation|delivery|transformation)/.test(title)) score += 5;

  if (dealSignalText.includes("closed lost") || dealSignalText.includes("lost")) {
    if (/(operations|implementation|delivery|program|transformation)/.test(title)) score += 3;
    if (/(finance|procurement|contracts|acquisition)/.test(title)) score += 2;
  }

  if (dealSignalText.includes("closed won") || dealSignalText.includes("won")) {
    if (/(executive|chief|vp|director|head)/.test(title)) score += 2;
  }

  if (!contact.title) score -= 2;
  return score;
}

function buildIcpRoleFallback(dealBriefs: DealBrief[]): string {
  const topDeal = dealBriefs[0];
  const signal = normalizeText(`${topDeal?.motion || ""} ${topDeal?.stage || ""} ${topDeal?.name || ""}`);
  const roles = [
    "Program Director (Implementation/Delivery)",
    "VP/Director of Operations",
    "Procurement or Contracts Lead",
  ];

  if (/(it|technology|systems|platform|service now|servicenow|digital)/.test(signal)) {
    roles.unshift("CIO/CTO or IT Director");
  }
  if (/(finance|cost|budget)/.test(signal)) {
    roles.unshift("CFO/Finance Director");
  }

  return roles.slice(0, 3).join(", ");
}
function buildRelevantDealBriefs(context: HubspotContextResponse | null, industryTarget: string): DealBrief[] {
  const deals = getHubspotDealRawList(context);
  const matchedCompany = getMatchedCompanyIdentity(context);
  const scopedDeals = matchedCompany ? deals.filter((deal) => dealMatchesMatchedCompany(deal, matchedCompany)) : deals;

  if (matchedCompany && !scopedDeals.length) {
    return [];
  }

  const industryTokens = normalizeText(industryTarget)
    .split(/[^a-z0-9]+/)
    .filter((token: string) => token.length >= 4);

  return scopedDeals
    .map((deal) => {
      const stageAgeDays = daysSince(
        asString(deal.properties.hs_lastmodifieddate) || deal.updatedAt || deal.createdAt
      );
      const nextStepRaw = pickFirstValue(deal.properties, [
        "hs_next_activity_date",
        "hs_next_step_date",
        "next_activity_date",
      ]);
      const lastActivityRaw = pickFirstValue(deal.properties, ["hs_last_activity_date", "lastactivitydate"]);
      const dealText = normalizeText(`${deal.name} ${deal.pipeline} ${deal.stage}`);
      const industryScore = industryTokens.length
        ? industryTokens.filter((token: string) => dealText.includes(token)).length
        : 0;
      const recencyTs = new Date(deal.updatedAt || deal.createdAt || deal.closeDate || 0).getTime();
      const closedLostBoost = isClosedLostStage(deal.stage) ? 2 : 0;
      const closedWonBoost = isClosedWonStage(deal.stage) ? 1 : 0;

      return {
        id: deal.id,
        name: deal.name || "Deal",
        stage: deal.stage || "Stage unknown",
        nextStepDate: nextStepRaw ? formatShortDate(nextStepRaw) : "Not set",
        lastActivityDate: lastActivityRaw
          ? formatShortDate(lastActivityRaw)
          : formatShortDate(deal.updatedAt || deal.closeDate || "") || "Not logged",
        url: deal.url,
        stageAgeDays,
        amount: deal.amount ? `$${deal.amount}` : "No amount",
        motion: deal.pipeline || pickFirstValue(deal.properties, ["dealtype", "motion_type"]) || "Unknown motion",
        industryScore,
        recencyTs: Number.isFinite(recencyTs) ? recencyTs : 0,
        closedLostBoost,
        closedWonBoost,
      };
    })
    .sort((a, b) => {
      if (b.industryScore !== a.industryScore) return b.industryScore - a.industryScore;
      if (b.closedLostBoost !== a.closedLostBoost) return b.closedLostBoost - a.closedLostBoost;
      if (b.closedWonBoost !== a.closedWonBoost) return b.closedWonBoost - a.closedWonBoost;
      return b.recencyTs - a.recencyTs;
    })
    .slice(0, 5)
    .map(({ industryScore: _industryScore, recencyTs: _recencyTs, closedLostBoost: _a, closedWonBoost: _b, ...deal }) => deal);
}
function buildOutreachBrief(
  context: HubspotContextResponse | null,
  dealBriefs: DealBrief[]
): {
  whyNow: string;
  whoToMessage: string;
  whatToReference: string;
  referenceUrl: string | null;
} {
  const topDeal = dealBriefs[0];
  const reasons: string[] = [];
  if (topDeal) {
    if (typeof topDeal.stageAgeDays === "number" && topDeal.stageAgeDays >= 21) {
      reasons.push(`${topDeal.name} has been in ${topDeal.stage} for ${topDeal.stageAgeDays} days`);
    }
    if (topDeal.nextStepDate && topDeal.nextStepDate !== "Not set" && topDeal.nextStepDate !== "N/A") {
      reasons.push(`next step date is ${topDeal.nextStepDate}`);
    }
    if (topDeal.lastActivityDate && topDeal.lastActivityDate !== "Not logged") {
      reasons.push(`last recorded activity was ${topDeal.lastActivityDate}`);
    }
  }
  if (!reasons.length) {
    reasons.push("active account exists but urgency signals are limited in current CRM fields");
  }

  const contacts = getHubspotContactRawList(context);
  const rankedContacts = contacts
    .map((contact) => {
      const title = normalizeText(contact.title);
      let score = 0;
      if (/(chief|cfo|ceo|coo|cio|cto|svp|vp|director|head|owner|president)/.test(title)) score += 3;
      if (/(manager|lead|program|operations|procurement|finance)/.test(title)) score += 2;
      if (/(specialist|analyst|coordinator)/.test(title)) score += 1;
      return { ...contact, score };
    })
    .sort((a, b) => b.score - a.score)
    .slice(0, 3);

  const whoToMessage = rankedContacts.length
    ? rankedContacts
        .map((contact) => `${contact.name || "Unknown"}${contact.title ? ` (${contact.title})` : ""}`)
        .join(", ")
    : "No clear decision-maker/influencer/champion contacts found";

  const topDealRaw = getHubspotDealRawList(context)[0];
  const referenceSignals = topDealRaw
    ? collectDealTextSignals([topDealRaw], [
        "dealname",
        "dealtype",
        "motion_type",
        "value_proposition",
        "business_case",
        "decision_criteria",
        "pain_points",
      ])
    : [];
  const whatToReference = referenceSignals.length
    ? referenceSignals.join(" | ")
    : topDeal
      ? `${topDeal.motion} motion in ${topDeal.stage} for ${topDeal.name}`
      : "No initiative/pain/motion fields populated in current CRM payload";
  const explicitReference = topDealRaw
    ? pickFirstValue(topDealRaw.properties, [
        "rom_url",
        "rom_link",
        "reference_url",
        "recommendation_memo_url",
        "rom",
      ]) || topDealRaw.url
    : topDeal?.url || "";
  const explicitReferenceUrl = isHttpUrl(explicitReference) ? explicitReference : "";
  const derivedReferenceUrl = extractFirstHttpUrl(whatToReference);
  const referenceUrl = explicitReferenceUrl || derivedReferenceUrl || null;

  return {
    whyNow: reasons.join("; "),
    whoToMessage,
    whatToReference,
    referenceUrl,
  };
}

function buildIndustryClosedDealBriefs(
  context: HubspotContextResponse | null,
  industryTarget: string
): DealBrief[] {
  const industryTokens = normalizeText(industryTarget)
    .split(/[^a-z0-9]+/)
    .filter((token: string) => token.length >= 4);

  const similarWins = getSimilarWins(context);
  const similarRanked = similarWins
    .map((deal) => {
      const dealText = normalizeText(`${deal.deal_name} ${deal.dealstage}`);
      const score = industryTokens.length
        ? industryTokens.filter((token: string) => dealText.includes(token)).length
        : 0;
      const closedTs = new Date(deal.close_date || 0).getTime();
      return {
        id: deal.id,
        name: deal.deal_name || "Closed Won Deal",
        stage: deal.dealstage || "Closed Won",
        nextStepDate: "N/A",
        lastActivityDate: deal.close_date ? formatShortDate(deal.close_date) : "Not logged",
        url: deal.url,
        stageAgeDays: null,
        amount: deal.amount ? `$${deal.amount}` : "No amount",
        motion: deal.dealstage || "Closed Won",
        score,
        closedTs: Number.isFinite(closedTs) ? closedTs : 0,
      };
    })
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return b.closedTs - a.closedTs;
    })
    .slice(0, 3)
    .map(({ score: _score, closedTs: _closedTs, ...deal }) => deal);

  if (similarRanked.length) return similarRanked;

  const closedRaw = getHubspotDealRawList(context).filter((deal) => isClosedWonStage(deal.stage));
  return closedRaw
    .map((deal) => {
      const dealText = normalizeText(`${deal.name} ${deal.pipeline} ${deal.stage}`);
      const score = industryTokens.length
        ? industryTokens.filter((token: string) => dealText.includes(token)).length
        : 0;
      const closedTs = new Date(deal.closeDate || deal.updatedAt || 0).getTime();
      return {
        id: deal.id,
        name: deal.name || "Closed Won Deal",
        stage: deal.stage || "Closed Won",
        nextStepDate: "N/A",
        lastActivityDate: formatShortDate(deal.closeDate || deal.updatedAt),
        url: deal.url,
        stageAgeDays: null,
        amount: deal.amount ? `$${deal.amount}` : "No amount",
        motion: deal.pipeline || "Closed Won",
        score,
        closedTs: Number.isFinite(closedTs) ? closedTs : 0,
      };
    })
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return b.closedTs - a.closedTs;
    })
    .slice(0, 3)
    .map(({ score: _score, closedTs: _closedTs, ...deal }) => deal);
}

function buildHubspotExportSummary(
  context: HubspotContextResponse | null,
  apolloSnapshot: ApolloAccountSnapshotResponse | null = null
): {
  status: string;
  matchConfidence: string;
  matchBasis: string;
  accountOwner: string;
  deals: DealBrief[];
  industryDeals: DealBrief[];
  outreach: { whyNow: string; whoToMessage: string; whatToReference: string } | null;
  industrySignal: string;
  industrySignalSource: string;
} {
  const insight = buildAccountInsight(context);
  // Match HubspotDashboard: Apollo industry first, then HubSpot (PDF was missing Apollo-only signal).
  const apolloIndustry = readStringFromRecord(apolloSnapshot?.organization ?? null, "industry");
  const trustedHubspotIndustry = getPrimaryIndustryFromHubspot(context);
  const industryTarget = apolloIndustry || trustedHubspotIndustry || "";
  const industryDeals = buildIndustryClosedDealBriefs(context, industryTarget);
  const industrySignal = formatIndustryLabel(industryTarget);
  const industrySignalSource = apolloIndustry
    ? "Apollo"
    : trustedHubspotIndustry
      ? "HubSpot"
      : "N/A";
  if (!insight.matched) {
    return {
      status: "No exact account match found for this organization.",
      matchConfidence: insight.confidence,
      matchBasis: insight.matchBasis,
      accountOwner: "N/A",
      deals: [],
      industryDeals,
      outreach: null,
      industrySignal,
      industrySignalSource,
    };
  }
  if (!insight.confidentMatch) {
    return {
      status: "Possible account match found but below trusted confidence threshold.",
      matchConfidence: insight.confidence,
      matchBasis: insight.matchBasis,
      accountOwner: insight.owner || "Unassigned",
      deals: [],
      industryDeals,
      outreach: null,
      industrySignal,
      industrySignalSource,
    };
  }

  const deals = buildRelevantDealBriefs(context, industryTarget);
  const outreach = buildOutreachBrief(context, deals);
  return {
    status: "Confident account match found.",
    matchConfidence: insight.confidence,
    matchBasis: insight.matchBasis,
    accountOwner: insight.owner || "Unassigned",
    deals,
    industryDeals,
    outreach,
    industrySignal,
    industrySignalSource,
  };
}

function ApolloSnapshotCard({
  snapshot,
  onFindDomain,
  findingDomain,
  onOpenWorkspaceLink,
}: {
  snapshot: ApolloAccountSnapshotResponse | null;
  onFindDomain: () => void;
  findingDomain: boolean;
  onOpenWorkspaceLink: (url: string) => void;
}) {
  if (!snapshot) {
    return <p className="muted">No Apollo account snapshot loaded yet.</p>;
  }
  if (!snapshot.matched) {
    return <p className="muted">{snapshot.message ?? "No Apollo organization match found."}</p>;
  }
  const org = snapshot.organization ?? {};
  const tech = readArrayFromRecord(org, "tech_stack");
  const orgName = readStringFromRecord(org, "name") || "Unknown Organization";
  const domain = readStringFromRecord(org, "domain") || "N/A";
  const domainPresent = domain !== "N/A";
  const industryRaw = readStringFromRecord(org, "industry") || "N/A";
  const size = readStringFromRecord(org, "estimated_num_employees") || "N/A";
  const hq = readStringFromRecord(org, "hq_location") || "N/A";
  const industry = industryRaw
    .split(/\s+/)
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(" ");
  const prettyTech = tech.map((item) =>
    item
      .split(/\s+/)
      .filter(Boolean)
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
      .join(" ")
  );
  return (
    <div className="apollo-snapshot">
      <header className="apollo-snapshot-header">
        <p className="apollo-snapshot-name">{orgName}</p>
      </header>

      <div className="apollo-snapshot-metrics">
        <article className="apollo-snapshot-metric">
          <p className="apollo-snapshot-metric-label">Domain</p>
          <div className="apollo-snapshot-metric-value">
            {domainPresent ? (
              <button
                type="button"
                className="apollo-domain-link"
                onClick={() => {
                  onOpenWorkspaceLink(`https://${domain}`);
                }}
              >
                {domain}
              </button>
            ) : (
              <button
                type="button"
                className="action-btn-secondary apollo-domain-action"
                onClick={onFindDomain}
                disabled={findingDomain}
              >
                {findingDomain ? "Finding..." : "Find Domain"}
              </button>
            )}
          </div>
        </article>
        <article className="apollo-snapshot-metric">
          <p className="apollo-snapshot-metric-label">Industry</p>
          <p className="apollo-snapshot-metric-value">{industry}</p>
        </article>
        <article className="apollo-snapshot-metric">
          <p className="apollo-snapshot-metric-label">Employees</p>
          <p className="apollo-snapshot-metric-value">{size}</p>
        </article>
        <article className="apollo-snapshot-metric">
          <p className="apollo-snapshot-metric-label">HQ</p>
          <p className="apollo-snapshot-metric-value">{hq}</p>
        </article>
      </div>

      <section className="apollo-snapshot-stack">
        <p className="apollo-snapshot-stack-label">Tech Stack</p>
        {prettyTech.length ? (
          <ul className="apollo-snapshot-tech-list">
            {prettyTech.slice(0, 20).map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        ) : (
          <p className="muted">Not available</p>
        )}
      </section>
    </div>
  );
}

function RelationshipHistoryPanel({ context }: { context: HubspotContextResponse | null }) {
  const events = getRelationshipEvents(context);
  if (!events.length) {
    return <p className="muted">No exact-match relationship history found yet.</p>;
  }
  return (
    <ul className="hubspot-list">
      {events.map((event) => (
        <li key={`${event.entity_id}-${event.type}-${event.timestamp}`} className="hubspot-list-item">
          <div>
            <strong>{event.title || "Activity"}</strong>
            <p className="muted">
              {event.type} | {formatDate(event.timestamp)} | {event.detail || "No detail"}
            </p>
          </div>
          {event.url ? (
            <a href={event.url} target="_blank" rel="noreferrer" className="hubspot-link">
              Open
            </a>
          ) : null}
        </li>
      ))}
    </ul>
  );
}

function SimilarWinsPanel({ context }: { context: HubspotContextResponse | null }) {
  const deals = getSimilarWins(context);
  if (!deals.length) {
    return (
      <p className="muted">
        No closed won deals returned for the last 5 years with current filters.
      </p>
    );
  }
  return (
    <ul className="hubspot-list">
      {deals.map((deal) => (
        <li key={`${deal.id}-${deal.close_date}`} className="hubspot-list-item">
          <div>
            <strong>{deal.deal_name || "Closed Won Deal"}</strong>
            <p className="muted">
              {deal.amount ? `$${deal.amount}` : "No amount"} | {deal.dealstage || "Stage N/A"} |{" "}
              {formatDate(deal.close_date)}
            </p>
          </div>
          {deal.url ? (
            <a href={deal.url} target="_blank" rel="noreferrer" className="hubspot-link">
              Open
            </a>
          ) : null}
        </li>
      ))}
    </ul>
  );
}

function PhoneCallPanel({
  result,
  context,
  snapshot,
  callerPhrase,
}: {
  result: GrantCampaignResponse;
  context: HubspotContextResponse | null;
  snapshot: ApolloAccountSnapshotResponse | null;
  callerPhrase: string;
}) {
  const orgName = result.minimum_fields_used?.organization_name || "the organization";
  const industry =
    readStringFromRecord(snapshot?.organization ?? null, "industry") ||
    getPrimaryIndustryFromHubspot(context) ||
    "their sector";
  const win = getSimilarWins(context)[0];
  return (
    <div className="email-list">
      {result.campaign.recipients.map((recipient) => {
        const name = parseRecipientLabel(recipient.label).fullName || "there";
        return (
          <article key={`phone-${recipient.label}`} className="email-card">
            <h5>{recipient.label}</h5>
            <p className="muted">{recipient.persona} - {recipient.rationale}</p>

            <div className="email-block">
              <p className="email-subject-line">
                <strong>Opener (30 seconds)</strong>
              </p>
              <p>
                Hi {name}, this is {callerPhrase}. I’m reaching out because teams in {industry}
                are under pressure to execute complex initiatives without adding delivery risk.
                We help organizations like {orgName} turn strategy into implementable operating plans
                across governance, controls, and rollout sequencing.
              </p>
            </div>

            <div className="email-block">
              <p className="email-subject-line">
                <strong>Credibility + Relevance</strong>
              </p>
              <p>
                {win
                  ? `A recent comparable closed won engagement was "${win.deal_name || "program delivery win"}" where the focus was execution quality and measurable outcomes.`
                  : "We have public-sector and enterprise implementation experience focused on delivery quality, governance, and operational continuity."}
              </p>
            </div>

            <div className="email-block">
              <p className="email-subject-line">
                <strong>Discovery Questions</strong>
              </p>
              <p>1. Where is execution slowing down right now: governance, cross-team coordination, or delivery capacity?</p>
              <p>2. Which milestone is most at risk in the next 60-90 days?</p>
              <p>3. If we solved one operational bottleneck this quarter, what would create the biggest impact?</p>
            </div>

            <div className="email-block">
              <p className="email-subject-line">
                <strong>Close</strong>
              </p>
              <p>
                If useful, we can run a short working session to map risks, ownership, and the first
                execution sprint so your team can move faster with less rework.
              </p>
            </div>
          </article>
        );
      })}
    </div>
  );
}

function normalizeRecommendedAssets(items: unknown): RecommendedAssetItem[] {
  const list = Array.isArray(items) ? items : [];
  const out: RecommendedAssetItem[] = [];
  for (let index = 0; index < list.length; index += 1) {
    const row = asRecord(list[index]);
    const title = readStringFromRecord(row, "title") || `Asset ${index + 1}`;
    const url = readStringFromRecord(row, "url");
    if (!url) continue;
    const scoreRaw = row?.score;
    out.push({
      id: readStringFromRecord(row, "id") || undefined,
      title,
      url,
      thumbnail_url:
        readStringFromRecord(row, "thumbnail_url") ||
        readStringFromRecord(row, "preview_url") ||
        null,
      thumbnail_base64:
        readStringFromRecord(row, "thumbnail_base64") ||
        readStringFromRecord(row, "preview_base64") ||
        null,
      industry: readStringFromRecord(row, "industry") || null,
      score: typeof scoreRaw === "number" ? scoreRaw : null,
      reason: readStringFromRecord(row, "reason") || null,
      path: readStringFromRecord(row, "path") || null,
      matched_terms: Array.isArray(row?.matched_terms)
        ? row.matched_terms.map((term) => String(term))
        : [],
      source: readStringFromRecord(row, "source") || undefined,
    });
  }
  return out;
}

function CaseStudiesPanel({
  data,
  onOpenWorkspaceLink,
}: {
  data: CaseStudyRecommendResponse | null;
  onOpenWorkspaceLink: (url: string) => void;
}) {
  const items = normalizeRecommendedAssets(data?.items);
  if (!items?.length) {
    return <p className="muted">{data?.message ?? "No case study recommendations available."}</p>;
  }
  return (
    <ul className="hubspot-list">
      {items.map((item, index) => {
        const title = item.title || `Case Study ${index + 1}`;
        const url = item.url;
        const previewUrl =
          normalizeThumbnailSrc(item.thumbnail_base64) ||
          item.thumbnail_url ||
          buildSharepointPreviewImageUrl(url);
        return (
          <li key={`${title}-${index}`} className="hubspot-list-item">
            <div className="asset-item-main">
              <button
                type="button"
                className="asset-title-button"
                onClick={() => onOpenWorkspaceLink(url)}
              >
                {title}
              </button>
              {previewUrl ? (
                <img
                  src={previewUrl}
                  alt={`${title} preview`}
                  loading="lazy"
                  className="asset-preview-image"
                  onClick={() => onOpenWorkspaceLink(url)}
                  onError={(event) => {
                    event.currentTarget.style.display = "none";
                  }}
                />
              ) : null}
            </div>
            <div className="asset-item-actions">
              {url ? (
                <a
                  href={url}
                  className="asset-action-button"
                  onClick={(event) => {
                    event.preventDefault();
                    onOpenWorkspaceLink(url);
                  }}
                >
                  Open
                </a>
              ) : null}
            </div>
          </li>
        );
      })}
    </ul>
  );
}

function buildSharepointPreviewImageUrl(url: string): string {
  if (!url || !url.includes("sharepoint.com")) return "";
  if (url.toLowerCase().includes("/_layouts/15/doc.aspx")) return "";
  return apiUrl(`/api/assets/thumbnail?url=${encodeURIComponent(url)}`);
}

function ExecutivePitchSummary({ summary }: { summary: string }) {
  const parsed = parsePitchSummary(summary);
  return (
    <ul className="campaign-summary-list">
      <li className="campaign-summary-item">
        <strong>Executive Summary:</strong> {parsed.executiveSummary}
      </li>
      {parsed.objective ? (
        <li className="campaign-summary-item">
          <strong>Objective:</strong> {parsed.objective}
        </li>
      ) : null}
    </ul>
  );
}

function parsePitchSummary(summary: string): { executiveSummary: string; objective: string | null } {
  const text = String(summary || "").replace(/\s+/g, " ").trim();
  if (!text) {
    return { executiveSummary: "No executive summary available.", objective: null };
  }
  const labelRegex = /\b(outcome|objective)\s*:\s*/i;
  const labelMatch = labelRegex.exec(text);
  if (!labelMatch || labelMatch.index < 0) {
    return { executiveSummary: text, objective: null };
  }
  const executiveSummary = text.slice(0, labelMatch.index).trim().replace(/[:;\-]\s*$/, "");
  const objective = text.slice(labelMatch.index + labelMatch[0].length).trim();
  return {
    executiveSummary: executiveSummary || text,
    objective: objective || null,
  };
}

function normalizeWorkspaceUrl(url: string): string {
  try {
    const parsed = new URL(url);
    if (!parsed.hostname.includes("sharepoint.com")) return url;
    parsed.searchParams.delete("mobileredirect");
    if (!parsed.searchParams.get("action")) {
      parsed.searchParams.set("action", "edit");
    }
    parsed.searchParams.set("web", "1");
    return parsed.toString();
  } catch {
    return url;
  }
}

function normalizeThumbnailSrc(base64: string | null | undefined): string {
  const raw = (base64 || "").trim();
  if (!raw) return "";
  if (raw.startsWith("data:image/")) return raw;
  return `data:image/jpeg;base64,${raw}`;
}

function buildLeadershipReportHtml(
  result: GrantCampaignResponse,
  apolloByLabel: Record<string, ApolloRecipientResult> = {},
  hubspotContext: HubspotContextResponse | null = null,
  apolloSnapshot: ApolloAccountSnapshotResponse | null = null
): string {
  const generated = new Date(result.generated_at).toLocaleString();
  const stakeholderHtml = result.campaign.recipients
    .map(
      (r) => `
      <div class="stakeholder">
        <h3>${escapeHtml(r.label)}</h3>
        <p class="meta">${escapeHtml(r.persona)}</p>
        <p>${escapeHtml(r.rationale)}</p>
        ${
          apolloByLabel[r.label]?.email
            ? `<p><strong>Email:</strong> ${escapeHtml(apolloByLabel[r.label]?.email ?? "")}</p>`
            : ""
        }
      </div>
    `
    )
    .join("");

  const sequenceHtml = result.campaign.prospect_campaigns
    .map((seq) => {
      const emails = seq.emails
        .map(
          (email) => `
          <div class="email">
            <h4>Email ${email.email_number}: ${escapeHtml(email.subject)}</h4>
            <pre>${escapeHtml(email.body)}</pre>
          </div>
        `
        )
        .join("");
      return `
        <section class="sequence">
          <h3>${escapeHtml(seq.recipient_label)}</h3>
          <p class="meta">${escapeHtml(seq.recipient_persona)} - ${escapeHtml(
        seq.recipient_rationale
      )}</p>
          ${emails}
        </section>
      `;
    })
    .join("");

  const hubspotExport = hubspotContext
    ? buildHubspotExportSummary(hubspotContext, apolloSnapshot)
    : null;
  const hubspotHtml = hubspotExport
    ? `
    <h2>HubSpot Activity</h2>
    <div class="card">
      <p><strong>Match State:</strong> ${escapeHtml(hubspotExport.matchConfidence)}</p>
      <p><strong>Match Notes:</strong> ${escapeHtml(hubspotExport.matchBasis)}</p>
      <p><strong>Industry signal:</strong> ${escapeHtml(hubspotExport.industrySignal || "Not available")} <span class="muted">(${escapeHtml(hubspotExport.industrySignalSource)})</span></p>
      ${
        hubspotExport.deals.length
          ? `<p><strong>Relevant Deals:</strong></p>
        <ul>
          ${hubspotExport.deals
            .map(
              (deal) =>
                `<li>${escapeHtml(deal.name)} (${escapeHtml(deal.stage)}) - Next: ${escapeHtml(
                  deal.nextStepDate
                )} - Last Activity: ${escapeHtml(deal.lastActivityDate)}</li>`
            )
            .join("")}
        </ul>`
          : "<p><strong>Relevant Deals:</strong> Not available (no confident account match)</p>"
      }
      ${
        hubspotExport.industryDeals.length
          ? `<p><strong>Industry-Matched Closed Deals:</strong></p>
        <ul>
          ${hubspotExport.industryDeals
            .map(
              (deal) =>
                `<li>${escapeHtml(deal.name)} (${escapeHtml(deal.stage)}) - Closed: ${escapeHtml(
                  deal.lastActivityDate
                )} - Motion: ${escapeHtml(deal.motion)}</li>`
            )
            .join("")}
        </ul>`
          : "<p><strong>Industry-Matched Closed Deals:</strong> None available</p>"
      }
      ${
        hubspotExport.outreach
          ? `
        <p><strong>Why Now:</strong> ${escapeHtml(hubspotExport.outreach.whyNow)}</p>
        <p><strong>Who To Message:</strong> ${escapeHtml(hubspotExport.outreach.whoToMessage)}</p>
        <p><strong>What To Reference:</strong> ${escapeHtml(
          hubspotExport.outreach.whatToReference
        )}</p>
      `
          : ""
      }
    </div>
  `
    : "";

  return `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>${escapeHtml(result.campaign.campaign_title)}</title>
    <style>
      body { font-family: 'Segoe UI', Arial, sans-serif; color: #0f172a; margin: 36px; line-height: 1.45; }
      h1 { margin: 0 0 6px; color: #1b2a4a; }
      h2 { margin: 22px 0 10px; color: #1b2a4a; border-bottom: 2px solid #dbe5f4; padding-bottom: 6px; }
      h3 { margin: 0 0 4px; color: #172554; }
      h4 { margin: 8px 0 6px; color: #1e293b; }
      p { margin: 0 0 8px; }
      .meta { color: #475569; font-size: 12px; margin-bottom: 8px; }
      .card { border: 1px solid #dbe5f4; border-radius: 8px; padding: 12px; margin-bottom: 10px; background: #fff; }
      .stakeholder { border: 1px solid #dbe5f4; border-radius: 8px; padding: 12px; margin-bottom: 10px; }
      .sequence { border: 1px solid #dbe5f4; border-radius: 10px; padding: 12px; margin-bottom: 16px; }
      .email { border-top: 1px solid #e2e8f0; margin-top: 10px; padding-top: 10px; }
      pre { white-space: pre-wrap; font-family: inherit; margin: 0; }
      .timestamp { color: #64748b; font-size: 12px; margin-bottom: 18px; }
      @media print { body { margin: 20px; } }
    </style>
  </head>
  <body>
    <h1>${escapeHtml(result.campaign.campaign_title)}</h1>
    <div class="timestamp">Generated: ${escapeHtml(generated)}</div>
    <div class="card">
      <h2>Executive Pitch Summary</h2>
      <p>${escapeHtml(result.campaign.strategy_summary)}</p>
    </div>
    <h2>Target Stakeholders</h2>
    ${stakeholderHtml}
    ${hubspotHtml}
    <h2>Personalized Email Sequences</h2>
    ${sequenceHtml}
  </body>
</html>`;
}

async function exportCampaignPdf(
  result: GrantCampaignResponse,
  apolloByLabel: Record<string, ApolloRecipientResult> = {},
  hubspotContext: HubspotContextResponse | null = null,
  apolloSnapshot: ApolloAccountSnapshotResponse | null = null
): Promise<void> {
  const pdf = await PDFDocument.create();
  const pageWidth = 612;
  const pageHeight = 792;
  const margin = 40;
  const maxWidth = pageWidth - margin * 2;

  const fontRegular = await pdf.embedFont(StandardFonts.Helvetica);
  const fontBold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const colorBody = rgb(15 / 255, 23 / 255, 42 / 255);
  const colorHeading = rgb(27 / 255, 42 / 255, 74 / 255);
  const colorMuted = rgb(100 / 255, 116 / 255, 139 / 255);

  let page = pdf.addPage([pageWidth, pageHeight]);
  let y = pageHeight - margin;

  const wrapText = (text: string, size: number, font: typeof fontRegular): string[] => {
    const words = (text || "").split(/\s+/).filter(Boolean);
    if (!words.length) return [""];
    const lines: string[] = [];
    let current = words[0];
    for (let i = 1; i < words.length; i += 1) {
      const next = `${current} ${words[i]}`;
      if (font.widthOfTextAtSize(next, size) <= maxWidth) {
        current = next;
      } else {
        lines.push(current);
        current = words[i];
      }
    }
    lines.push(current);
    return lines;
  };

  const ensure = (needed: number) => {
    if (y - needed >= margin) return;
    page = pdf.addPage([pageWidth, pageHeight]);
    y = pageHeight - margin;
  };

  const draw = (
    text: string,
    opts?: { size?: number; bold?: boolean; muted?: boolean; gap?: number }
  ) => {
    const size = opts?.size ?? 11;
    const font = opts?.bold ? fontBold : fontRegular;
    const color = opts?.muted ? colorMuted : opts?.bold ? colorHeading : colorBody;
    const lines = wrapText(text, size, font);
    const lineHeight = size + 4;
    ensure(lines.length * lineHeight + 6);
    for (const line of lines) {
      page.drawText(line, { x: margin, y, size, font, color });
      y -= lineHeight;
    }
    y -= opts?.gap ?? 4;
  };

  draw(result.campaign.campaign_title, { size: 18, bold: true, gap: 2 });
  draw(`Generated: ${new Date(result.generated_at).toLocaleString()}`, {
    size: 10,
    muted: true,
    gap: 10,
  });

  draw("Executive Pitch Summary", { size: 13, bold: true });
  draw(result.campaign.strategy_summary, { size: 11, gap: 10 });

  draw("Target Stakeholders", { size: 13, bold: true });
  for (const recipient of result.campaign.recipients) {
    draw(recipient.label, { size: 11, bold: true, gap: 2 });
    draw(`${recipient.persona} - ${recipient.rationale}`, { size: 10, gap: 8 });
    const enrichedEmail = apolloByLabel[recipient.label]?.email;
    if (enrichedEmail) {
      draw(`Email: ${enrichedEmail}`, { size: 10, bold: true, gap: 8 });
    }
  }

  if (hubspotContext) {
    const hubspotExport = buildHubspotExportSummary(hubspotContext, apolloSnapshot);
    draw("HubSpot Activity", { size: 13, bold: true });
    draw(`Match State: ${hubspotExport.matchConfidence}`, { size: 10, gap: 2 });
    draw(`Match Notes: ${hubspotExport.matchBasis}`, { size: 10, gap: 2 });
    draw(
      `Industry signal: ${hubspotExport.industrySignal || "Not available"} (${hubspotExport.industrySignalSource})`,
      { size: 10, gap: 6 }
    );
    if (hubspotExport.deals.length) {
      draw("Relevant Deals", { size: 10, bold: true, gap: 2 });
      for (const deal of hubspotExport.deals) {
        draw(
          `${deal.name} (${deal.stage}) | Next: ${deal.nextStepDate} | Last: ${deal.lastActivityDate}`,
          { size: 9, gap: 2 }
        );
      }
      y -= 4;
    } else {
      draw("Relevant Deals: Not available (no confident account match)", { size: 10, gap: 6 });
    }
    if (hubspotExport.industryDeals.length) {
      draw("Industry-Matched Closed Deals", { size: 10, bold: true, gap: 2 });
      for (const deal of hubspotExport.industryDeals) {
        draw(
          `${deal.name} (${deal.stage}) | Closed: ${deal.lastActivityDate} | Motion: ${deal.motion}`,
          { size: 9, gap: 2 }
        );
      }
      y -= 4;
    } else {
      draw("Industry-Matched Closed Deals: None available", { size: 10, gap: 6 });
    }
    if (hubspotExport.outreach) {
      draw(`Why Now: ${hubspotExport.outreach.whyNow}`, { size: 9, gap: 2 });
      draw(`Who To Message: ${hubspotExport.outreach.whoToMessage}`, { size: 9, gap: 2 });
      draw(`What To Reference: ${hubspotExport.outreach.whatToReference}`, { size: 9, gap: 8 });
    } else {
      y -= 4;
    }
  }

  draw("Personalized Email Sequences", { size: 13, bold: true });
  for (const sequence of result.campaign.prospect_campaigns) {
    draw(sequence.recipient_label, { size: 12, bold: true, gap: 2 });
    draw(`${sequence.recipient_persona} - ${sequence.recipient_rationale}`, {
      size: 10,
      muted: true,
      gap: 6,
    });
    for (const email of sequence.emails) {
      draw(`Email ${email.email_number}: ${email.subject}`, { size: 11, bold: true, gap: 2 });
      draw(email.body, { size: 10, gap: 6 });
    }
    y -= 4;
  }

  const bytes = await pdf.save();
  const blobBuffer = new ArrayBuffer(bytes.length);
  new Uint8Array(blobBuffer).set(bytes);
  const blob = new Blob([blobBuffer], { type: "application/pdf" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  const safeTitle = toSafeFilename(result.campaign.campaign_title);
  link.download = `${safeTitle || "SLED-Campaign"}-${new Date()
    .toISOString()
    .slice(0, 10)}.pdf`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function toSafeFilename(value: string): string {
  return (value || "")
    .trim()
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, "-")
    .replace(/\s+/g, " ")
    .replace(/\.+$/g, "")
    .slice(0, 120);
}

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function readRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function readStringFromRecord(record: unknown, key: string): string {
  const obj = readRecord(record);
  if (!obj) return "";
  const value = obj[key];
  if (value === null || value === undefined) return "";
  return String(value).trim();
}

function readArrayFromRecord(record: unknown, key: string): string[] {
  const obj = readRecord(record);
  if (!obj || !Array.isArray(obj[key])) return [];
  return (obj[key] as unknown[])
    .map((item) => String(item ?? "").trim())
    .filter(Boolean);
}

function getPrimaryIndustryFromHubspot(context: HubspotContextResponse | null): string {
  const accountMatch = readRecord(context?.account_match);
  const confidentMatch = Boolean(accountMatch?.confident_match);
  if (!confidentMatch) return "";
  const exact = readRecord(context?.exact_matches);
  const companies = exact && Array.isArray(exact.companies) ? (exact.companies as unknown[]) : [];
  for (const company of companies) {
    const industry = readStringFromRecord(company, "industry");
    if (industry) return industry;
  }
  const fallbackCompanies = getHubspotCompanies(context?.companies ?? null);
  return fallbackCompanies[0]?.industry || "";
}

function formatIndustryLabel(value: string): string {
  const raw = (value || "").trim();
  if (!raw) return "";
  return raw
    .replace(/_/g, " ")
    .split(/\s+/)
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(" ");
}

function getRelationshipEvents(
  context: HubspotContextResponse | null
): Array<{
  timestamp: string;
  type: string;
  entity_id: string;
  title: string;
  detail: string;
  url: string;
}> {
  const history = readRecord(context?.relationship_history);
  const events = history && Array.isArray(history.events) ? history.events : [];
  return events
    .map((item) => {
      const row = readRecord(item);
      return {
        timestamp: readStringFromRecord(row, "timestamp"),
        type: readStringFromRecord(row, "type"),
        entity_id: readStringFromRecord(row, "entity_id"),
        title: readStringFromRecord(row, "title"),
        detail: readStringFromRecord(row, "detail"),
        url: readStringFromRecord(row, "url"),
      };
    })
    .filter((item) => item.timestamp);
}

function getExactMatchCounts(context: HubspotContextResponse | null): {
  companies: number;
  contacts: number;
  deals: number;
  foundAny: boolean;
} {
  const exact = readRecord(context?.exact_matches);
  const companies = Array.isArray(exact?.companies) ? exact.companies.length : 0;
  const contacts = Array.isArray(exact?.contacts) ? exact.contacts.length : 0;
  const deals = Array.isArray(exact?.deals) ? exact.deals.length : 0;
  const foundAny = Boolean(exact?.found_any) || companies > 0 || contacts > 0 || deals > 0;
  return { companies, contacts, deals, foundAny };
}

function getSimilarWins(
  context: HubspotContextResponse | null
): Array<{
  id: string;
  deal_name: string;
  amount: string;
  dealstage: string;
  close_date: string;
  url: string;
}> {
  const similar = readRecord(context?.similar_closed_won);
  const deals = similar && Array.isArray(similar.deals) ? similar.deals : [];
  return deals
    .map((item) => {
      const row = readRecord(item);
      return {
        id: readStringFromRecord(row, "id"),
        deal_name: readStringFromRecord(row, "deal_name"),
        amount: readStringFromRecord(row, "amount"),
        dealstage: readStringFromRecord(row, "dealstage"),
        close_date: readStringFromRecord(row, "close_date"),
        url: readStringFromRecord(row, "url"),
      };
    })
    .filter((item) => item.id || item.deal_name);
}

function formatDate(value: string): string {
  if (!value) return "Unknown date";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleDateString();
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function asString(value: unknown): string {
  if (value === null || value === undefined) return "";
  return String(value).trim();
}

function normalizeText(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9\s]/g, " ").replace(/\s+/g, " ").trim();
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function getHubspotSummaryTiles(summary: Record<string, unknown> | null | undefined): HubspotSummaryTile[] {
  const summaryRoot = asRecord(summary);
  const summaryBlock = asRecord(summaryRoot?.summary);
  const sections: Array<{ key: HubspotSummaryTile["key"]; label: string }> = [
    { key: "companies", label: "Companies" },
    { key: "contacts", label: "Contacts" },
    { key: "deals", label: "Deals" },
    { key: "tickets", label: "Tickets" },
  ];
  return sections.map((section) => {
    const raw = asRecord(summaryBlock?.[section.key]);
    const accessibleRaw = raw?.accessible;
    const accessible =
      typeof accessibleRaw === "boolean" ? accessibleRaw : null;
    const total = asNumber(raw?.total);
    return {
      key: section.key,
      label: section.label,
      accessible,
      total,
    };
  });
}

function getHubspotCompanies(companies: Record<string, unknown> | null | undefined): HubspotCompanyItem[] {
  const root = asRecord(companies);
  const list = asArray(root?.companies);
  return list
    .map((item) => {
      const row = asRecord(item);
      const props = asRecord(row?.properties);
      const locationParts = [asString(props?.city), asString(props?.state)].filter(Boolean);
      return {
        id: asString(row?.id),
        name: asString(props?.name) || "Unknown Company",
        industry: asString(props?.industry) || "Unknown industry",
        domain: asString(props?.domain) || "No domain",
        location: locationParts.length ? locationParts.join(", ") : "Unknown location",
        url: asString(row?.url),
      };
    })
    .filter((item) => item.id || item.name);
}

function getHubspotContacts(contacts: Record<string, unknown> | null | undefined): HubspotContactItem[] {
  const root = asRecord(contacts);
  const list = asArray(root?.contacts);
  return list
    .map((item) => {
      const row = asRecord(item);
      const props = asRecord(row?.properties);
      const first = asString(props?.firstname);
      const last = asString(props?.lastname);
      const fullName = [first, last].filter(Boolean).join(" ").trim();
      return {
        id: asString(row?.id),
        name: fullName || "Unknown Contact",
        email: asString(props?.email) || "No email",
        phone: asString(props?.phone) || "No phone",
        company: asString(props?.company) || "No company",
        url: asString(row?.url),
      };
    })
    .filter((item) => item.id || item.name);
}

function getHubspotDeals(deals: Record<string, unknown> | null | undefined): HubspotDealItem[] {
  const root = asRecord(deals);
  const list = asArray(root?.deals);
  return list
    .map((item) => {
      const row = asRecord(item);
      const props = asRecord(row?.properties);
      const amount = asString(props?.amount);
      const closeDateRaw = asString(props?.closedate);
      const closeDate = closeDateRaw
        ? new Date(closeDateRaw).toLocaleDateString()
        : "No close date";
      return {
        id: asString(row?.id),
        name: asString(props?.dealname) || "Unknown Deal",
        amount: amount ? `$${amount}` : "No amount",
        stage: asString(props?.dealstage) || "No stage",
        closeDate,
        url: asString(row?.url),
      };
    })
    .filter((item) => item.id || item.name);
}

function getHubspotSearch(search: Record<string, unknown> | null | undefined): HubspotSearchItem[] {
  const root = asRecord(search);
  const list = asArray(root?.results);
  return list
    .map((item) => {
      const row = asRecord(item);
      const props = asRecord(row?.properties);
      const first = asString(props?.firstname);
      const last = asString(props?.lastname);
      const name = asString(row?.name) || [first, last].filter(Boolean).join(" ").trim();
      return {
        id: asString(row?.id),
        name: name || "Unknown Match",
        email: asString(props?.email),
        company: asString(props?.company),
        url: asString(row?.url),
      };
    })
    .filter((item) => item.id || item.name);
}

function parseRecipientLabel(label: string): { fullName: string; title: string | null } {
  const trimmed = (label || "").trim();
  const match = /^(.+?)\s*\(([^)]+)\)\s*$/.exec(trimmed);
  if (!match) return { fullName: trimmed, title: null };
  return { fullName: match[1].trim(), title: match[2].trim() };
}

function normalizePersonKey(name: string): string {
  return (name || "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

type FieldProps = {
  label: string;
  value: string;
  onChange: (value: string) => void;
  type?: "text" | "number";
  required?: boolean;
  min?: number;
  max?: number;
};

function Field({
  label,
  value,
  onChange,
  type = "text",
  required = false,
  min,
  max,
}: FieldProps) {
  return (
    <label className="field">
      <span>{label}</span>
      <input
        type={type}
        value={value}
        required={required}
        min={type === "number" ? min : undefined}
        max={type === "number" ? max : undefined}
        onChange={(event) => onChange(event.target.value)}
      />
    </label>
  );
}
type TextAreaProps = {
  label: string;
  value: string;
  onChange: (value: string) => void;
  rows: number;
  required?: boolean;
};

function TextAreaField({
  label,
  value,
  onChange,
  rows,
  required = false,
}: TextAreaProps) {
  return (
    <label className="field">
      <span>{label}</span>
      <textarea
        rows={rows}
        value={value}
        required={required}
        onChange={(event) => onChange(event.target.value)}
      />
    </label>
  );
}

function cleanNullable(value: string | null | undefined): string | null {
  const trimmed = (value ?? "").trim();
  return trimmed ? trimmed : null;
}

function buildEvidence(
  awardId: string | null,
  awardDescription: string | null
): Array<{
  label: string;
  url: string;
  source: "usaspending";
  excerpt: string | null;
}> {
  if (!awardId) return [];
  return [
    {
      label: "USAspending award record",
      url: `https://www.usaspending.gov/search/?hash=${encodeURIComponent(awardId)}`,
      source: "usaspending",
      excerpt: awardDescription,
    },
  ];
}





































