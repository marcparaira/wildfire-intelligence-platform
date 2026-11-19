# Frontend Container Runtime — Technology Selection Study

**Project:** Wildfire Monitoring & Decision Support Platform  
**Status:** Proposed decision | **Date:** 2026-11-19  
**Scope:** Production container for the React + TypeScript + Vite single-page application (SPA). Architecture is out of scope.

## 1. Requirements

**Hard requirements**

- **H1:** Serve Vite's compiled static assets in production (no development server).
- **H2:** Support SPA route fallback, correct MIME types, cache-control headers, and compressed assets.
- **H3:** Run as a non-root process in a standalone Podman container.
- **H4:** Support a multi-stage build with no frontend build toolchain in the runtime image.
- **H5:** Be suitable for maintained, security-patched production use.

**Soft requirements**

- **S1:** Small runtime image and low idle resource consumption.
- **S2:** High static-file throughput and low latency.
- **S3:** Simple configuration, deployment, and troubleshooting.
- **S4:** Low ongoing maintenance burden and broad operational familiarity.

## 2. Options Considered and Hard-Requirement Screening

| Option | Hard requirements | Outcome |
|---|---|---|
| Nginx (unprivileged image) | H1–H5 | Retain |
| Caddy | H1–H5 | Retain |
| Lighttpd | H1–H5 | Retain |
| Apache HTTP Server | H1–H5 | Retain |
| Node.js + production static-server application | H1–H5, with an additional server application | Retain |
| ~~BusyBox HTTP Server~~ | Does not provide an adequately straightforward, production-ready configuration for all of H2 (SPA fallback, caching and compressed asset behavior) | **Exclude** |
| ~~Vite preview server~~ | Explicitly not intended as a production server (H1/H5) | **Exclude** |

Node.js is retained because a properly implemented and maintained static-serving application *can* meet the hard requirements; running `vite preview` in Node.js does not qualify.

## 3. Comparative Benchmark (Qualitative)

**Method:** Capability/operational assessment, **not a measured load test**. Relative rankings are estimates that must be validated using identical Vite build artifacts and equivalent Podman settings.

| Remaining option | Image / resource overhead | Static serving | Configuration & operations | Assessment |
|---|---|---|---|---|
| **Nginx** | Low | Excellent | Mature, familiar | **Best overall fit** |
| **Caddy** | Low–moderate | Excellent | Very simple | Strong alternative |
| **Lighttpd** | Low | Very good | Less common operationally | Viable |
| **Apache HTTP Server** | Moderate | Very good | More configuration surface | Viable, no clear advantage |
| **Node.js static server** | Higher | Good–very good | Extra runtime/dependencies | Unnecessary for static SPA |

**Validation benchmark before production:** On the same host, compare (1) image size, (2) idle RAM, (3) requests/sec and p95 latency for cached and uncached JS/CSS assets, (4) cold start, and (5) non-root operation, SPA deep-link handling, caching, and compression. No empirical performance figures are claimed here.

## 4. Final Selection

**Select: Nginx, using a maintained unprivileged runtime image.**

**Rationale:** Native static-file serving, configurable SPA fallback and cache headers, compression support, mature operations, and no need to include Node.js or Vite in the production image. Use Node.js only in the build stage, then copy the `dist/` artifacts into the Nginx runtime stage.

**Runner-up:** Caddy, particularly if configuration simplicity becomes the overriding preference. Revisit this choice if server-side rendering is introduced.

## References

- Vite — [Building for Production](https://vite.dev/guide/build) and [Deploying a Static Site](https://vite.dev/guide/static-deploy.html)
- Nginx — [Serving Static Content](https://docs.nginx.com/nginx/admin-guide/web-server/serving-static-content/)
- Nginx — [Unprivileged container image](https://github.com/nginx/docker-nginx-unprivileged)
- Caddy — [Static File Server](https://caddyserver.com/docs/caddyfile/directives/file_server)
- Lighttpd — [mod_staticfile](https://redmine.lighttpd.net/projects/lighttpd/wiki/Mod_staticfile)
