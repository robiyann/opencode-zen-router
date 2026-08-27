// Comprehensive Security Penetration Test Suite for OpenCode Zen Router
async function runSecurityAudit() {
  console.log("==========================================================");
  console.log("🛡️  PENETRATION TEST & SECURITY AUDIT: OPENCODE ZEN ROUTER");
  console.log("==========================================================\n");

  let passed = 0;
  let failed = 0;

  function assert(title, condition, extra = "") {
    if (condition) {
      console.log(`  ✅ [PASS] ${title} ${extra ? '(' + extra + ')' : ''}`);
      passed++;
    } else {
      console.error(`  ❌ [FAIL] ${title} ${extra ? '(' + extra + ')' : ''}`);
      failed++;
    }
  }

  // TEST SUITE 1: UNAUTHENTICATED ADMIN ACCESS PREVENTION
  console.log("--- [TEST 1] Unauthenticated Admin API Access Defense ---");
  const endpoints = [
    { method: "GET", path: "/api/keys" },
    { method: "POST", path: "/api/keys", body: { name: "hacked_key" } },
    { method: "GET", path: "/api/proxies" },
    { method: "POST", path: "/api/strategy", body: { strategy: "random" } },
    { method: "GET", path: "/api/logs" },
    { method: "POST", path: "/api/deploy-vercel", body: { token: "bad" } },
  ];

  for (const ep of endpoints) {
    const res = await fetch(`http://127.0.0.1:8080${ep.path}`, {
      method: ep.method,
      headers: { "Content-Type": "application/json" },
      body: ep.body ? JSON.stringify(ep.body) : undefined,
    });
    assert(`Blocks unauthenticated ${ep.method} ${ep.path}`, res.status === 401, `HTTP ${res.status}`);
  }

  // TEST SUITE 2: HTTP SECURITY HEADERS AUDIT
  console.log("\n--- [TEST 2] HTTP Security Headers Audit ---");
  const headRes = await fetch("http://127.0.0.1:8080/dashboard");
  assert("X-Frame-Options: DENY (Anti-Clickjacking)", headRes.headers.get("x-frame-options") === "DENY");
  assert("X-Content-Type-Options: nosniff (Anti-MIME Sniffing)", headRes.headers.get("x-content-type-options") === "nosniff");
  assert("X-XSS-Protection enabled", headRes.headers.get("x-xss-protection") === "1; mode=block");

  // TEST SUITE 3: OPENAI ENDPOINT API KEY DEFENSE & SQL INJECTION
  console.log("\n--- [TEST 3] /v1 OpenAI Gateway Endpoint Protection ---");
  
  // 3.1 Missing Key
  const rMissing = await fetch("http://127.0.0.1:8080/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ model: "nemotron-3-ultra-free", messages: [{ role: "user", content: "Hi" }] })
  });
  assert("Rejects request with Missing API Key", rMissing.status === 401, `HTTP ${rMissing.status}`);

  // 3.2 Fake / Invalid Key
  const rFake = await fetch("http://127.0.0.1:8080/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": "Bearer sk-zen-attacker-fake-key" },
    body: JSON.stringify({ model: "nemotron-3-ultra-free", messages: [{ role: "user", content: "Hi" }] })
  });
  assert("Rejects Fake / Non-Existent API Key", rFake.status === 401, `HTTP ${rFake.status}`);

  // 3.3 SQL Injection Payload in API Key
  const sqlPayloads = [
    "' OR '1'='1' --",
    "sk-zen-' UNION SELECT 1,2,3,4,5,6,7,8 --",
    "\"; DROP TABLE api_keys; --"
  ];
  for (const sqli of sqlPayloads) {
    const rSqli = await fetch("http://127.0.0.1:8080/v1/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${sqli}` },
      body: JSON.stringify({ model: "nemotron-3-ultra-free", messages: [{ role: "user", content: "Hi" }] })
    });
    assert(`Immune to SQL Injection payload: ${sqli.substring(0, 15)}...`, rSqli.status === 401, `HTTP ${rSqli.status}`);
  }

  // TEST SUITE 4: LOGIN BRUTE FORCE & RATE LIMITING
  console.log("\n--- [TEST 4] Admin Login Brute Force Lockout Test ---");
  let lockedOut = false;
  for (let attempt = 1; attempt <= 6; attempt++) {
    const res = await fetch("http://127.0.0.1:8080/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ password: `wrong-pass-${attempt}` })
    });
    const d = await res.json();
    if (res.status === 429) {
      lockedOut = true;
      console.log(`  🔒 Triggered IP Lockout on attempt ${attempt}: "${d.error}"`);
      break;
    }
  }
  assert("Brute-force attacker is locked out with HTTP 429", lockedOut);

  // TEST SUMMARY
  console.log("\n==========================================================");
  console.log(`🎯 SECURITY AUDIT FINISHED: ${passed} PASSED, ${failed} FAILED`);
  console.log("==========================================================");
}

runSecurityAudit().catch(console.error);
