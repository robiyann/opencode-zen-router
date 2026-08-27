const API_KEY = "sk-zen-3a9bfa1bb6e7018da38779fd28a9f9a8";
const BASE_URL = "http://127.0.0.1:8080/v1";

const DEV_PROMPTS = [
  "Tuliskan fungsi fibonacci memoization di Python.",
  "Bagaimana cara membaca file JSON di Node.js?",
  "Buat fungsi binary search di Go.",
  "Tuliskan query SQL INNER JOIN dua tabel.",
  "Buat CSS flexbox center container div.",
  "Tuliskan Dockerfile Node.js minimalis.",
  "Bagaimana cara buat HTTP server di Go?",
  "Tuliskan fungsi debounce di JS.",
  "Buat middleware JWT di Express.js.",
  "Tuliskan custom hook useLocalStorage di React TS."
];

async function sendDevPrompt(devId, promptIndex) {
  const t0 = Date.now();
  const prompt = DEV_PROMPTS[promptIndex % DEV_PROMPTS.length];
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 60000);

  try {
    const res = await fetch(BASE_URL + "/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + API_KEY,
        "x-session-id": `dev-session-${devId}`
      },
      body: JSON.stringify({
        model: "mimo-v2.5-free",
        messages: [{ role: "user", content: prompt }],
        stream: true
      }),
      signal: controller.signal
    });

    const proxyNode = res.headers.get("x-router-proxy") || "unknown";

    if (!res.ok) {
      clearTimeout(timeoutId);
      return { devId, promptIndex, status: res.status, success: false, reason: `HTTP ${res.status}`, proxyNode, elapsed: Date.now() - t0 };
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let fullText = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const chunk = decoder.decode(value, { stream: true });
      const lines = chunk.split("\n");
      for (const line of lines) {
        if (line.startsWith("data: ") && !line.includes("[DONE]")) {
          try {
            const parsed = JSON.parse(line.slice(6));
            const delta = parsed.choices?.[0]?.delta?.content || "";
            if (delta) fullText += delta;
          } catch (e) {}
        }
      }
    }

    clearTimeout(timeoutId);
    const elapsed = Date.now() - t0;
    const isError = fullText.includes('"error":') || fullText.includes("Upstream error");
    const isSuccess = fullText.length > 50 && !isError;

    return {
      devId,
      promptIndex,
      status: 200,
      success: isSuccess,
      reason: isSuccess ? "OK" : "Invalid Payload",
      proxyNode,
      elapsed,
      chars: fullText.length,
      snippet: fullText.slice(0, 80).replace(/\n/g, " ")
    };
  } catch (err) {
    clearTimeout(timeoutId);
    return { devId, promptIndex, status: 0, success: false, reason: "Timeout/Abort", proxyNode: "unknown", elapsed: Date.now() - t0 };
  }
}

async function run60Devs2MinTest() {
  console.log("================================================================================");
  console.log("👥 FLOOD SIMULASI REAL: 60 DEVELOPER NGODING BERSAMAAN SELAMA 2 MENIT (120s)");
  console.log("🎯 Target Model   : mimo-v2.5-free (Xiaomi LLM - 100% Strict)");
  console.log("🌐 Target Armada  : 97 Active Vercel Edge Relays (Go Router)");
  console.log("⏱️ Durasi Tes      : 2 Menit (120 Detik Continuous Coding Session)");
  console.log("================================================================================\n");

  const TOTAL_DEVS = 60;
  const DURATION_MS = 120000; // 2 minutes
  const startTime = Date.now();
  const allResults = [];

  const statusTicker = setInterval(() => {
    const elapsedSec = Math.floor((Date.now() - startTime) / 1000);
    const completed = allResults.length;
    const successCount = allResults.filter(r => r.success).length;
    console.log(`⏱️ [Detik ${elapsedSec}s/120s] Selesai ${completed} request (Sukses: ${successCount}, Gagal: ${completed - successCount})...`);
  }, 10000);

  // Launch 60 developer workers in parallel
  const devWorkers = Array.from({ length: TOTAL_DEVS }, async (_, idx) => {
    const devId = idx + 1;
    let promptCount = 0;

    // Stagger initial launch by 50ms per dev to mimic natural start
    await new Promise(r => setTimeout(r, idx * 50));

    while (Date.now() - startTime < DURATION_MS) {
      promptCount++;
      const result = await sendDevPrompt(devId, promptCount);
      allResults.push(result);

      if (Date.now() - startTime >= DURATION_MS) break;

      // Small 1s typing delay before developer sends next prompt
      await new Promise(r => setTimeout(r, 1000));
    }
  });

  await Promise.all(devWorkers);
  clearInterval(statusTicker);

  const totalDurationSec = ((Date.now() - startTime) / 1000).toFixed(1);
  const totalReqs = allResults.length;
  const totalSuccess = allResults.filter(r => r.success).length;
  const totalFail = totalReqs - totalSuccess;
  const successPct = ((totalSuccess / (totalReqs || 1)) * 100).toFixed(1);
  const activeNodes = new Set(allResults.map(r => r.proxyNode).filter(p => p !== "unknown")).size;
  const avgLatency = (allResults.filter(r => r.success).reduce((a, b) => a + b.elapsed, 0) / (totalSuccess || 1)).toFixed(0);

  console.log("\n================================================================================");
  console.log(`🏁 LAPORAN FLOOD 60 DEVELOPER SELAMA 2 MENIT (DURASI ${totalDurationSec}s)`);
  console.log("================================================================================");
  console.log(`📊 TOTAL DEVELOPER AKTIF      : ${TOTAL_DEVS} Developer (Continuous Load)`);
  console.log(`🚀 TOTAL REQUEST TERKIRIM     : ${totalReqs} Request Koding`);
  console.log(`✅ SUKSES MENDAPAT KODE VALID : ${totalSuccess} Request (${successPct}% SUCCESS RATE! 🎉)`);
  console.log(`⚠️ GAGAL / TIMEOUT            : ${totalFail} Request`);
  console.log(`🌐 NODE VERCEL UNIK DIGUNAKAN : ${activeNodes} / 97 Node`);
  console.log(`⚡ RATA-RATA LATENSI KODE     : ${avgLatency} ms`);
  console.log("================================================================================\n");

  console.log("📋 SAMPEL 10 REQUEST PERTAMA:");
  console.log("--------------------------------------------------------------------------------");
  allResults.slice(0, 10).forEach((r, idx) => {
    const mark = r.success ? "✅ SUKSES 200 OK" : "❌ GAGAL";
    console.log(`  Req #${(idx + 1).toString().padStart(2, "0")} (Dev #${r.devId}) [${mark}] (${r.elapsed}ms) -> Snippet: "${r.snippet}..."`);
  });
  console.log("--------------------------------------------------------------------------------\n");
}

run60Devs2MinTest().catch(console.error);
