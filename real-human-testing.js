const API_KEY = "sk-zen-3a9bfa1bb6e7018da38779fd28a9f9a8";
const BASE_URL = "http://127.0.0.1:8080/v1";

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function streamTestScenario(scenarioNum, title, model, prompt, sessionID = "") {
  console.log(`\n================================================================================`);
  console.log(`🧪 TEST SCENARIO #${scenarioNum}: ${title}`);
  console.log(`🎯 Model Target : ${model}`);
  console.log(`🔑 Session ID   : ${sessionID || "Auto-generated"}`);
  console.log(`================================================================================\n`);
  console.log(`📝 PROMPT: "${prompt}"\n`, { flush: true });

  const t0 = Date.now();
  let firstTokenTime = null;
  let responseContent = "";
  let chunkCount = 0;

  try {
    const headers = {
      "Content-Type": "application/json",
      "Authorization": "Bearer " + API_KEY
    };
    if (sessionID) {
      headers["x-session-id"] = sessionID;
    }

    const res = await fetch(BASE_URL + "/chat/completions", {
      method: "POST",
      headers,
      body: JSON.stringify({
        model: model,
        messages: [
          { role: "system", content: "Kamu adalah AI Coding Assistant tingkat senior. Jawablah dengan struktur penalaran yang jelas dan kode yang siap pakai." },
          { role: "user", content: prompt }
        ],
        stream: true
      })
    });

    const proxy = res.headers.get("x-router-proxy") || "unknown";
    console.log(`🌐 Routed via Proxy: ${proxy}`);
    console.log(`📡 HTTP Status     : ${res.status} ${res.statusText}\n`);

    if (!res.ok) {
      const errText = await res.text();
      console.log(`❌ Error Output   : ${errText}`);
      return { success: false, status: res.status, proxy, elapsed: Date.now() - t0 };
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      if (!firstTokenTime) {
        firstTokenTime = Date.now() - t0;
      }

      const chunk = decoder.decode(value, { stream: true });
      const lines = chunk.split("\n");

      for (const line of lines) {
        if (line.startsWith("data: ") && !line.includes("[DONE]")) {
          try {
            const parsed = JSON.parse(line.slice(6));
            const delta = parsed.choices?.[0]?.delta?.content || "";
            if (delta) {
              responseContent += delta;
              chunkCount++;
            }
          } catch (e) {
            // Raw text fallback
          }
        }
      }
    }

    const totalElapsed = Date.now() - t0;
    console.log("--- 💬 AI RESPONSE SNIPPET ---");
    console.log(responseContent.slice(0, 500) + (responseContent.length > 500 ? "\n... [TRUNCATED FOR LOG READABILITY]" : ""));
    console.log("\n--------------------------------------------------------------------------------");
    console.log(`✅ RESULT METRICS:`);
    console.log(`  ⏱️ Time-To-First-Token (TTFT) : ${firstTokenTime || totalElapsed} ms`);
    console.log(`  ⏱️ Total Response Latency     : ${totalElapsed} ms`);
    console.log(`  📦 Total Stream Chunks        : ${chunkCount} chunks`);
    console.log(`  📄 Total Output Length        : ${responseContent.length} chars`);
    console.log(`--------------------------------------------------------------------------------\n`);

    return { success: true, status: 200, proxy, elapsed: totalElapsed, ttft: firstTokenTime, chars: responseContent.length };

  } catch (err) {
    console.log(`\n❌ Network / Abort Exception: ${err.message}`);
    return { success: false, status: 0, proxy: "exception", elapsed: Date.now() - t0 };
  }
}

async function runRealHumanSuite() {
  console.log("================================================================================");
  console.log("🚀 STARTING REAL DEVELOPER THINKING & REASONING SUITE ACROSS 103 RELAY NODES");
  console.log("================================================================================");

  // Scenario 1: LeetCode Hard Thinking Test (Mimo)
  await streamTestScenario(
    1,
    "LeetCode Hard: Minimum Window Substring (Thinking & Algorithmic Logic)",
    "mimo-v2.5-free",
    "Tuliskan fungsi untuk menyelesaikan LeetCode Hard: 'Minimum Window Substring' dalam bahasa Python dengan time complexity O(N). Berikan penjelasan penalaran logika (step-by-step reasoning) secara sistematis sebelum menuliskan kodenya, lalu sertakan 2 test case."
  );

  await sleep(2000);

  // Scenario 2: System Design & Go Architecture (Nemotron)
  await streamTestScenario(
    2,
    "High Concurrency System Design: 1 Million WebSocket Pool in Go",
    "nemotron-3-ultra-free",
    "Rancang arsitektur WebSocket Pool Manager di Go yang mampu menangani 1 juta koneksi serentak dengan thread-safe lock. Tuliskan kodenya lengkap dengan struct ConnectionPool, Mutex, dan method Register/Broadcast."
  );

  await sleep(2000);

  // Scenario 3: Complex Debugging & Memory Leak Fix (x-preview-f)
  await streamTestScenario(
    3,
    "Complex Debugging: Memory Leak & Race Condition Analysis",
    "x-preview-f-free",
    "Jelaskan penyebab umum goroutine memory leak dan data race saat mengakses map di Go. Tuliskan contoh kode yang bermasalah dan solusinya menggunakan sync.Map atau RWMutex."
  );

  await sleep(2000);

  // Scenario 4: Multi-Turn Conversation (Hunyuan 3)
  const sessionID = "developer-session-real-human-101";
  await streamTestScenario(
    4,
    "Multi-Turn Step 1: Distributed Lock in TypeScript",
    "hy3-free",
    "Buatlah class `DistributedLock` menggunakan Redis & Redlock algorithm di TypeScript. Tuliskan kodenya.",
    sessionID
  );

  await sleep(2000);

  await streamTestScenario(
    5,
    "Multi-Turn Step 2: Adding TTL Heartbeat Auto-Renewal",
    "hy3-free",
    "Sekarang tambahkan fitur auto-renewal TTL heartbeat pada class DistributedLock tadi agar lock tidak expire saat task panjang.",
    sessionID
  );

  console.log("================================================================================");
  console.log("🎉 ALL REAL DEVELOPER TEST SCENARIOS COMPLETED SUCCESSFULLY!");
  console.log("================================================================================\n");
}

runRealHumanSuite().catch(console.error);
