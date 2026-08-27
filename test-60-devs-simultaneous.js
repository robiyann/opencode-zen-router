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

async function simulateDeveloper(devId) {
  const t0 = Date.now();
  const prompt = DEV_PROMPTS[devId % DEV_PROMPTS.length];
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 60000); // 60s standard IDE timeout

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
      const errText = await res.text();
      return { devId, prompt, status: res.status, success: false, reason: `HTTP ${res.status}`, proxyNode, elapsed: Date.now() - t0, tokens: 0 };
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
      prompt,
      status: 200,
      success: isSuccess,
      reason: isSuccess ? "OK" : "Invalid Payload",
      proxyNode,
      elapsed,
      chars: fullText.length,
      snippet: fullText.slice(0, 100).replace(/\n/g, " ")
    };

  } catch (err) {
    clearTimeout(timeoutId);
    return { devId, prompt, status: 0, success: false, reason: "Timeout/Abort", proxyNode: "unknown", elapsed: Date.now() - t0, tokens: 0 };
  }
}

async function run60DevsSimulation() {
  console.log("================================================================================");
  console.log("👥 SIMULASI REAL: 60 DEVELOPER NGODING BERSAMAAN DI VS CODE / CURSOR");
  console.log("🎯 Target Model   : mimo-v2.5-free (Xiaomi LLM)");
  console.log("🌐 Target Armada  : 103 Vercel Edge Relays (Go Router)");
  console.log("⏱️ Pola Penggunaan: 60 Dev Aktif (1 Dev mengirim prompt per 1 detik)");
  console.log("================================================================广\n");

  const TOTAL_DEVS = 60;
  const devPromises = [];
  const startAll = Date.now();

  // Launch 60 developers with 1 second stagger (realistic human entry cadence)
  for (let i = 1; i <= TOTAL_DEVS; i++) {
    const devId = i;
    const promise = (async () => {
      // Stagger 1 second per dev launch to match 60 active devs pacing
      await new Promise(r => setTimeout(r, (devId - 1) * 1000));
      return await simulateDeveloper(devId);
    })();
    devPromises.push(promise);
  }

  // Print progress every 10 seconds
  const statusTicker = setInterval(() => {
    const elapsedSec = Math.floor((Date.now() - startAll) / 1000);
    console.log(`⏱️ [Detik ${elapsedSec}s/60s] Memproses simulasi 60 developer...`);
  }, 10000);

  const results = await Promise.all(devPromises);
  clearInterval(statusTicker);

  const totalTime = ((Date.now() - startAll) / 1000).toFixed(1);
  const totalSuccess = results.filter(r => r.success).length;
  const totalFail = results.length - totalSuccess;
  const successPct = ((totalSuccess / results.length) * 100).toFixed(1);
  const activeNodes = new Set(results.map(r => r.proxyNode).filter(p => p !== "unknown")).size;
  const avgLatency = (results.filter(r => r.success).reduce((a, b) => a + b.elapsed, 0) / (totalSuccess || 1)).toFixed(0);

  console.log("\n================================================================================");
  console.log(`🏁 LAPORAN EXPERIENCES 60 DEVELOPER SIMULTAN (DURASI ${totalTime}s)`);
  console.log("================================================================================");
  console.log(`📊 TOTAL DEVELOPER AKTIF      : ${results.length} Developer`);
  console.log(`✅ SUKSES MENDAPAT KODE VALID : ${totalSuccess} Developer (${successPct}%)`);
  console.log(`⚠️ GAGAL / TIMEOUT            : ${totalFail} Developer`);
  console.log(`🌐 NODE VERCEL UNIK DIGUNAKAN : ${activeNodes} / 103 Node`);
  console.log(`⚡ RATA-RATA LATENSI KODE     : ${avgLatency} ms`);
  console.log("================================================================================\n");

  console.log("📋 SAMPLE HASIL PENGALAMAN 10 DEVELOPER PERTAMA:");
  console.log("--------------------------------------------------------------------------------");
  results.slice(0, 10).forEach(r => {
    const mark = r.success ? "✅ SUKSES 200 OK" : "❌ GAGAL";
    console.log(`  Dev #${r.devId.toString().padStart(2, "0")} [${mark}] (${r.elapsed}ms) -> Snippet: "${r.snippet}..."`);
  });
  console.log("--------------------------------------------------------------------------------\n");
}

run60DevsSimulation().catch(console.error);
