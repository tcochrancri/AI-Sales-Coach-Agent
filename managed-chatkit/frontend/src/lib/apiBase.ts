/**
 * In production, frontend and API are often on different Railway URLs.
 * VITE_API_URL must point at the FastAPI origin (same as ChatKit create-session).
 * When unset, use relative /api so Vite dev server proxy works.
 */
export function apiUrl(path: string): string {
  const raw = import.meta.env.VITE_API_URL;
  const base = typeof raw === "string" ? raw.trim().replace(/\/$/, "") : "";
  const p = path.startsWith("/") ? path : `/${path}`;
  if (!base) return p;
  return `${base}${p}`;
}
