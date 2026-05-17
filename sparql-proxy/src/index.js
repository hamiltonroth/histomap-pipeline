/**
 * Histomap SPARQL proxy — forwards queries to Wikidata Query Service (WDQS).
 *
 * Purpose: GitHub Actions runners use Azure IP ranges that are blanket-blocked
 * by a stale WDQS outage rule. This Worker runs on Cloudflare's edge network
 * (non-Azure IPs) and is not subject to that block.
 *
 * Auth: the pipeline must send X-Proxy-Key matching the API_KEY Worker secret.
 * Set the secret once with:  npx wrangler secret put API_KEY
 *
 * Deploy:  npx wrangler deploy
 */

const WDQS_URL = "https://query.wikidata.org/sparql";

export default {
  async fetch(request, env) {
    // Simple pre-shared key auth — prevents public abuse of the proxy.
    const key = request.headers.get("X-Proxy-Key");
    if (!env.API_KEY || key !== env.API_KEY) {
      return new Response("Unauthorized", { status: 401 });
    }

    if (request.method !== "GET" && request.method !== "POST") {
      return new Response("Method not allowed", { status: 405 });
    }

    // Build the WDQS target URL, forwarding query params for GET requests.
    const incoming = new URL(request.url);
    const targetUrl =
      request.method === "GET"
        ? WDQS_URL + "?" + incoming.searchParams.toString()
        : WDQS_URL;

    const forwardHeaders = {
      Accept:
        request.headers.get("Accept") || "application/sparql-results+json",
      "User-Agent":
        "histomap-pipeline/0.1 (mailto:rothhamilton@gmail.com)",
    };
    if (request.method === "POST") {
      forwardHeaders["Content-Type"] =
        request.headers.get("Content-Type") ||
        "application/x-www-form-urlencoded";
    }

    const resp = await fetch(targetUrl, {
      method: request.method,
      headers: forwardHeaders,
      body: request.method === "POST" ? request.body : undefined,
    });

    return new Response(resp.body, {
      status: resp.status,
      headers: {
        "Content-Type":
          resp.headers.get("Content-Type") || "application/sparql-results+json",
      },
    });
  },
};
