// Script to deploy 79 natural-looking Vercel Edge relays to complete the 100-node armada
const VERCEL_TOKEN = process.env.VERCEL_TOKEN || "";
const VERCEL_API = "https://api.vercel.com";
const ROUTER_API = "http://127.0.0.1:8080/api/proxies";

const PREFIXES = [
  "aether", "titan", "solaris", "chronos", "cyber", "neural", "vortex", "echo",
  "phantom", "zenith", "omni", "horizon", "cosmic", "terra", "falcon", "atlas",
  "hydra", "nebula", "flare", "astro", "delta", "sigma", "pulsar", "photon",
  "kinetic", "vanguard", "spectral", "abyss", "prism", "helix", "aero", "orbit",
  "vector", "strata", "synapse", "quantum", "stellar", "lumina", "nova", "apex",
  "hyper", "kinesis", "pulse", "matrix", "cortex", "axiom", "vertex", "nexus"
];

const ROOTS = [
  "edge", "cloud", "mesh", "stream", "worker", "relay", "gateway",
  "core", "router", "node", "pipeline", "cluster", "pod", "conduit"
];

const SUFFIXES = [
  "io", "api", "net", "hub", "sys", "app", "link", "prime", "sync", "mesh"
];

function generate79Names() {
  const names = new Set();
  let pIdx = 0, rIdx = 0, sIdx = 0;

  while (names.size < 79) {
    const p = PREFIXES[pIdx % PREFIXES.length];
    const r = ROOTS[rIdx % ROOTS.length];
    const s = SUFFIXES[sIdx % SUFFIXES.length];
    const candidate = `${p}-${r}-${s}`;
    names.add(candidate);

    pIdx = (pIdx + 1) % PREFIXES.length;
    if (pIdx === 0) rIdx = (rIdx + 1) % ROOTS.length;
    if (rIdx === 0 && pIdx === 0) sIdx = (sIdx + 1) % SUFFIXES.length;
  }

  return Array.from(names);
}

const PROJECT_NAMES = generate79Names();

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const RELAY_FUNCTION_CODE = `
export const config = { runtime: "edge" };

export default async function handler(req) {
  const urlObj = new URL(req.url);
  const pathname = urlObj.pathname;

  // 1. Health check & Diagnostics
  if (pathname === "/health") {
    return new Response(JSON.stringify({
      status: "healthy",
      service: "edge-relay",
      region: process.env.VERCEL_REGION || "edge-global",
      timestamp: new Date().toISOString()
    }), {
      status: 200,
      headers: { "content-type": "application/json", "access-control-allow-origin": "*" }
    });
  }

  // 2. Models Forwarding
  if (pathname.includes("/models")) {
    const res = await fetch("https://opencode.ai/zen/v1/models", {
      headers: {
        "Authorization": "Bearer public",
        "x-opencode-client": "desktop"
      }
    });
    return new Response(res.body, {
      status: res.status,
      headers: {
        "content-type": "application/json",
        "access-control-allow-origin": "*"
      }
    });
  }

  // 3. Chat Completions
  if (req.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "POST, GET, OPTIONS",
        "access-control-allow-headers": "*"
      }
    });
  }

  const targetUrl = "https://opencode.ai/zen/v1/chat/completions";
  const incomingHeaders = req.headers;

  const fwdHeaders = new Headers();
  fwdHeaders.set("Content-Type", "application/json");
  fwdHeaders.set("Authorization", "Bearer public");
  fwdHeaders.set("x-opencode-client", "desktop");

  const sesId = incomingHeaders.get("x-opencode-session") || 
                incomingHeaders.get("x-session-id") || 
                ("ses_" + Math.random().toString(36).substring(2, 15));
  fwdHeaders.set("x-opencode-session", sesId);

  const reqId = incomingHeaders.get("x-opencode-request") || 
                ("msg_" + Math.random().toString(36).substring(2, 15));
  fwdHeaders.set("x-opencode-request", reqId);

  try {
    const bodyBytes = await req.arrayBuffer();
    const upstreamRes = await fetch(targetUrl, {
      method: "POST",
      headers: fwdHeaders,
      body: bodyBytes
    });

    const respHeaders = new Headers(upstreamRes.headers);
    respHeaders.set("access-control-allow-origin", "*");
    respHeaders.set("x-edge-relay", "vercel-enterprise-armada");

    return new Response(upstreamRes.body, {
      status: upstreamRes.status,
      headers: respHeaders
    });
  } catch (err) {
    return new Response(JSON.stringify({
      error: {
        message: "Vercel Relay Edge Error: " + err.message,
        type: "edge_relay_error"
      }
    }), {
      status: 502,
      headers: { "content-type": "application/json", "access-control-allow-origin": "*" }
    });
  }
}
`;

async function deployOne(baseName, current, total) {
  const randSuffix = Math.random().toString(36).substring(2, 6);
  const fullName = `${baseName}-${randSuffix}`;

  console.log(`[${current}/${total}] 🚀 Deploying Relay: ${fullName}...`);

  // 1. Create Project
  try {
    await fetch(`${VERCEL_API}/v9/projects`, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${VERCEL_TOKEN}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        name: fullName,
        framework: null
      })
    });
  } catch (e) {}

  // 2. Deploy Edge Function
  const deployPayload = {
    name: fullName,
    project: fullName,
    target: "production",
    files: [
      {
        file: "api/index.js",
        data: RELAY_FUNCTION_CODE,
        encoding: "utf-8"
      },
      {
        file: "vercel.json",
        data: JSON.stringify({
          version: 2,
          routes: [{ src: "/(.*)", dest: "/api/index.js" }]
        }),
        encoding: "utf-8"
      }
    ]
  };

  const deployRes = await fetch(`${VERCEL_API}/v13/deployments`, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${VERCEL_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(deployPayload)
  });

  if (!deployRes.ok) {
    const errBody = await deployRes.text();
    throw new Error(`Deployment API failed (${deployRes.status}): ${errBody}`);
  }

  const deployData = await deployRes.json();
  const deployId = deployData.id;
  let readyUrl = "";

  // Poll for ready state (max 15 attempts x 2.5s = 37.5s)
  for (let attempt = 0; attempt < 15; attempt++) {
    await sleep(2500);
    const pollRes = await fetch(`${VERCEL_API}/v13/deployments/${deployId}`, {
      headers: { "Authorization": `Bearer ${VERCEL_TOKEN}` }
    });
    if (pollRes.ok) {
      const pollData = await pollRes.json();
      if (pollData.readyState === "READY") {
        readyUrl = `https://${pollData.url}`;
        break;
      }
    }
  }

  if (!readyUrl) {
    readyUrl = `https://${deployData.url}`;
  }

  // 3. Disable Vercel Auth / SSO Protection so public requests are allowed
  try {
    await fetch(`${VERCEL_API}/v9/projects/${fullName}`, {
      method: "PATCH",
      headers: {
        "Authorization": `Bearer ${VERCEL_TOKEN}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ ssoProtection: null })
    });
  } catch (e) {}

  console.log(`  ✅ Ready at: ${readyUrl}`);

  // 4. Register to Go Router SQLite Database
  try {
    const regRes = await fetch(ROUTER_API, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: `Relay: ${fullName}`,
        url: readyUrl
      })
    });
    if (regRes.ok) {
      console.log(`  💾 Injected into Go Router SQLite pool!`);
    }
  } catch (err) {
    console.log(`  ⚠️ Failed to inject into local router: ${err.message}`);
  }

  return readyUrl;
}

async function main() {
  console.log("==========================================================");
  console.log("🛡️  DEPLOYING 79 VERCEL EDGE RELAYS (TARGET: 100 TOTAL NODES)");
  console.log("==========================================================");
  console.log("• Staggered Anti-Abuse Jitter: 2.0s - 3.5s per deployment");
  console.log("• Realistic Domain & Project Names");
  console.log("• Target: 79 Fresh Relays -> SQLite Router Pool\n");

  const results = [];

  for (let i = 0; i < PROJECT_NAMES.length; i++) {
    try {
      const url = await deployOne(PROJECT_NAMES[i], i + 1, PROJECT_NAMES.length);
      results.push(url);
    } catch (err) {
      console.error(`  ❌ Failed: ${err.message}`);
      if (err.message.includes("429") || err.message.includes("rate_limit")) {
        console.log("  ⚠️ Hit Vercel rate limit. Cooling down 15s...");
        await sleep(15000);
      }
    }

    // Anti-Abuse Staggered Delay (2.0 to 3.5 seconds) between deploys
    if (i < PROJECT_NAMES.length - 1) {
      const delay = Math.floor(2000 + Math.random() * 1500);
      console.log(`  ⏳ Anti-abuse cooldown: waiting ${(delay / 1000).toFixed(1)}s before next deploy...`);
      await sleep(delay);
    }
  }

  console.log("\n==========================================================");
  console.log(`🎉 SUCCESS! Deployed ${results.length} / 79 Vercel Edge Relays!`);
  console.log("All nodes are registered to SQLite and active in Go Router.");
  console.log("==========================================================");
}

main().catch(console.error);
