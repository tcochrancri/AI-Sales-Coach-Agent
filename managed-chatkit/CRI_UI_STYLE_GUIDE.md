# CRI_UI_STYLE_GUIDE.md

## Purpose

This is the canonical UI/UX standard for CRI agent products.

Use this guide to keep all CRI interfaces consistent, operator-friendly, and free of unnecessary UI clutter.

## Core principle

Build only what is needed for the workflow.

- Do not add decorative panels with no clear operator action.
- Do not introduce new visual systems per feature.
- Do not add chat-first UI patterns unless the product explicitly requires chat.
- Prefer fewer, clearer sections over many competing sections.

## Design tokens (required)

Use these exact design tokens as the base system.

```css
:root {
  --bg: #0f1c3f;
  --panel: #ffffff;
  --ink: #172554;
  --subtle: #64748b;
  --accent: #1e3a8a;
  --accent-soft: #dbeafe;
  --border: #dbe5f4;
  --warn: #9a3412;
  --warn-soft: #ffedd5;
  --danger: #9f1239;
  --success: #166534;
  --success-soft: #dcfce7;
  --focus: #1d4ed8;
  --header-ink: #ffffff;
  --header-subtle: #93c5fd;
  --header-border: rgba(255, 255, 255, 0.2);
  --header-badge-bg: rgba(255, 255, 255, 0.1);
  --header-badge-ink: #bfdbfe;
}
```

## Typography and base layout

- Font stack: `'Segoe UI', 'Franklin Gothic Medium', sans-serif`
- Body line-height: `1.45`
- App shell: left sidebar + main region
- Main content width: up to `1360px`

## Required layout primitives

Reuse existing classes whenever possible.

- Container card: `.panel`
- Secondary text: `.muted`
- Section title: `.section-title`
- Small stack spacing: `.stack-sm`
- Horizontal action group: `.actions-row`
- Responsive form/control row: `.toolbar`
- Status chip: `.pill`, `.pill-warn`, `.pill-danger`
- Status alert: `.status-success`, `.status-error`, `.status-loading`
- Empty states: `.empty-state`

## Navigation pattern

Use the current CRI shell pattern.

- Sticky sidebar with grouped nav sections:
  - Research Workspaces
  - Operations
- Sticky topbar for context
- Keep nav labels short and task-oriented
- Keep active-state behavior consistent with `.sidebar-link.active`

## Hero/header pattern

For top-of-page workspace headers:

- Center headline/subheadline where current app already does this
- `h1` in white
- kicker in light blue (`#bfdbfe`)
- supporting subtitle in `#dbeafe`

Do not invent alternate hero styles per page.

## Data presentation rules

- Use tables for dense, comparable records
- Use cards (`.panel`) for summaries, detail groups, and actions
- Use tabbed sections for deep detail pages
- Keep source evidence visible and linked
- Keep scoring rationale visible where decisions are made

## Density and composition rules

- Summary first, evidence second, actions third
- Avoid stacking too many full-width panels before primary results
- Merge repetitive explanatory copy
- Keep policy context visible but compact

## Interaction rules

- Manual controls stay explicit (no hidden automation)
- Primary action button appears once per section
- Secondary actions use ghost-style treatment
- Every generated artifact must support operator review before external use

## Responsive rules

Maintain current breakpoints and behavior:

- `<900px`: sidebar collapses into compact top nav blocks
- `>=1060px`: two-column detail layouts allowed
- `>=1200px`: denser grids allowed

Do not create custom breakpoint systems unless approved.

## Accessibility minimums

- Keep visible focus (`:focus-visible` uses `--focus`)
- Ensure text contrast stays readable on dark backgrounds
- Avoid color-only status indicators; include text labels

## Agent guardrails (important)

When another CRI agent edits UI:

1. Reuse existing tokens and classes first.
2. Add new styles only if existing primitives cannot express the need.
3. Do not add new UI regions without clear user value.
4. Do not remove meaningful evidence or operational transparency.
5. Keep visual hierarchy consistent with this guide.
6. Never introduce Unicode replacement characters (`U+FFFD`) or mojibake into files.
7. If encoding corruption appears, fix the file content before completing the task.

## Implementation checklist for CRI agents

Before shipping UI changes, confirm:

- Uses CRI token palette
- Uses `.panel`/`.pill`/`.status` primitives
- Preserves sidebar + topbar shell
- Keeps primary workflow actions obvious
- Keeps evidence and rationale visible
- Adds no unnecessary containers or decorative sections
- Passes mobile layout sanity check
- Passes `npm run check:encoding`

## Source of truth

Primary style implementation lives in:

- `app/globals.css`
- `app/layout.tsx`
- `components/AppSidebar.tsx`

If these files change materially, update this guide.
