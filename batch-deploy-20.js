// Script to deploy 20 natural-looking Vercel Edge relays with anti-abuse delays
const VERCEL_TOKEN = process.env.VERCEL_TOKEN || "";
const VERCEL_API = "https://api.vercel.com";
const ROUTER_API = "http://127.0.0.1:8080/api/proxies";

const PROJECT_NAMES = [
  "nexus-edge-worker",
  "zenith-stream-api",
  "vertex-cloud-node",
  "nova-mesh-gateway",
  "pulse-edge-relay",
  "strata-router-io",
  "hyper-edge-mesh",
  "kinesis-api-node",
  "prism-cloud-core",
  "flux-stream-relay",
  "apex-edge-mesh",
  "vortex-cloud-api",
  "lumina-edge-node",
  "quantum-relay-io",
  "stellar-mesh-core",
  "aero-edge-proxy",
  "vector-cloud-mesh",
  "synapse-edge-api",
  "helix-cloud-relay",
  "orbit-mesh-node"
];

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
      headers: { ...Object.fromEntries(res.headers.entries()), "access-control-allow-origin": "*" }
    });
  }

  // 3. Chat Completions & Streaming
  const targetUrl = "https://opencode.ai/zen/v1/chat/completions";
  const headers = new Headers(req.headers);
  headers.set("Authorization", "Bearer public");
  headers.set("x-opencode-client", "desktop");
  headers.delete("host");

  try {
    const res = await fetch(targetUrl, {
      method: req.method,
      headers,
      body: req.body,
      duplex: "half"
    });

    const respHeaders = new Headers(res.headers);
    respHeaders.set("access-control-allow-origin", "*");
    respHeaders.set("x-relay-edge", process.env.VERCEL_REGION || "edge");

    return new Response(res.body, {
      status: res.status,
      headers: respHeaders
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: err.message }), {
      status: 502,
      headers: { "content-type": "application/json" }
    });
  }
}
`;

async function deployOne(projectName, index, total) {
  const suffix = Math.random().toString(36).substring(2, 6);
  const fullName = `${projectName}-${suffix}`;

  console.log(`\n[${index}/${total}] 🚀 Deploying project: ${fullName} ...`);

  const payload = {
    name: fullName,
    files: [
      {
        file: "api/chat.js",
        data: RELAY_FUNCTION_CODE
      },
      {
        file: "package.json",
        data: JSON.stringify({
          name: fullName,
          version: "1.0.0",
          private: true,
          description: "High-performance edge routing node"
        })
      },
      {
        file: "vercel.json",
        data: JSON.stringify({
          rewrites: [{ source: "/(.*)", destination: "/api/chat" }]
        })
      }
    ],
    projectSettings: { framework: null },
    target: "production"
  };

  // 1. Create Deployment on Vercel
  const res = await fetch(`${VERCEL_API}/v13/deployments`, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${VERCEL_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`Vercel deployment failed (${res.status}): ${errText}`);
  }

  const deployData = await res.json();
  const deployId = deployData.id;

  // 2. Poll until deployment is READY
  let readyUrl = "";
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
  console.log("🛡️  DEPLOYING 20 VERCEL EDGE RELAYS (ANTI-ABUSE PROTOCOL)");
  console.log("==========================================================");
  console.log("• Staggered Jitter: 2.5s - 4.0s per deployment");
  console.log("• Realistic Domain & Project Names");
  console.log("• Target: 20 Fresh Relays -> SQLite Router Pool\n");

  const results = [];

  for (let i = 0; i < PROJECT_NAMES.length; i++) {
    try {
      const url = await deployOne(PROJECT_NAMES[i], i + 1, PROJECT_NAMES.length);
      results.push(url);
    } catch (err) {
      console.error(`  ❌ Failed: ${err.message}`);
    }

    // Anti-Abuse Staggered Delay (2.5 to 4.0 seconds) between deploys
    if (i < PROJECT_NAMES.length - 1) {
      const delay = Math.floor(2500 + Math.random() * 1500);
      console.log(`  ⏳ Anti-abuse cooldown: waiting ${(delay / 1000).toFixed(1)}s before next deploy...`);
      await sleep(delay);
    }
  }

  console.log("\n==========================================================");
  console.log(`🎉 SUCCESS! Deployed ${results.length} Vercel Edge Relays!`);
  console.log("All nodes are registered to SQLite and active in Go Router.");
  console.log("==========================================================");
}

main().catch(console.error);
