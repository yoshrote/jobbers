# Frontend – Claude Code Guide

This is a UX built on top of the Jobbers API to enable:
- Visibility into the state of all active tasks within a window of time (you can supply a "start after" ULID to slice the timeline) which can be filtered and sorted based on whatever the API supports
- Visibility into the contents of the task schedule (list and detail views)
- The ability to add a task into the schedule
- Visibility into the contents of the dead letter queue (list and detail views)
- The ability to resubmit tasks from the dead letter queue
- The ability to define and update Queues and their Config
- The ability to map queues to arbitrary roles while making it easy for a person to reuse existing roles

## Stack

- **React 18** with JSX (no TypeScript)
- **Vite 5** for dev server and builds
- **React Router 6** for client-side routing
- **Plain CSS** for styling (no CSS framework)
- **npm** as package manager

## Project Layout

```
frontend/
├── src/
│   ├── api/client.js      # All API calls — single source of fetch logic
│   ├── components/        # Shared UI components
│   └── pages/             # One file per route
├── index.html
└── vite.config.js         # Dev proxy: /api/* → http://localhost:8000
```

## API – Source of Truth

**`/openapi.json` (repo root) is the authoritative definition of every backend endpoint, parameter, and response shape.**

Before adding or modifying any API call:

1. Read `openapi.json` to find the correct path, HTTP method, query parameters, request body schema, and response schema.
2. Keep `frontend/src/api/client.js` in sync with the spec — every exported function must match the spec exactly (path, method, parameter names, required vs optional fields).
3. Do not invent endpoints, parameter names, or response fields that are not in the spec.
4. If the spec and existing client code disagree, trust the spec and fix the client.

JSDoc comments on client functions should reflect the spec's schema; update them when the spec changes.

## Adding a New API Call

1. Find the operation in `openapi.json` (search by `operationId` or path).
2. Note the path, method, required/optional query params, request body `$ref`, and success response `$ref`.
3. Add a named export to `src/api/client.js` using the existing `get`/`post`/`put`/`del` helpers.
4. Write a JSDoc comment that mirrors the spec's parameter and response types.

## Dev Server

```bash
npm run dev      # Vite at http://localhost:5173
                 # /api/* proxied to http://localhost:8000
```

The backend must be running for API calls to work. See the repo-root `docker-compose.yml` to start all services.

## Conventions

- One page component per route, kept in `src/pages/`.
- Shared, reusable UI goes in `src/components/`.
- State is local React state (`useState`/`useEffect`); no global state library.
- All network I/O goes through `src/api/client.js` — do not call `fetch` directly from components.
- Keep components as plain JSX; avoid adding build-time type checking unless the project migrates to TypeScript.
