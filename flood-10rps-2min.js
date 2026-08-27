const API_KEY = "sk-zen-3a9bfa1bb6e7018da38779fd28a9f9a8";
const BASE_URL = "http://127.0.0.1:8080/v1";

const PROMPTS = [
  "Tuliskan fungsi fibonacci di Python singkat saja.",
  "Tuliskan regex email validation di JS.",
  "Buat fungsi binary search di Go.",
  "Tuliskan query SQL INNER JOIN dua tabel.",
  "Bagaimana cara membaca file di Node.js?",
  "Tuliskan fungsi reverse string di Rust.",
  "Buat CSS flexbox center div.",
  "Tuliskan Dockerfile Node.js minimalis.",
  "Bagaimana cara buat HTTP server di Go?",
  "Tuliskan fungsi debounce di JS."
];

async function sendSingleRequest(reqId) {
  const t0 = Date.now();
  const prompt = PROMPTS[reqId % PROMPTS.length];
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 15000); // 15s timeout

  try {
    const res = await fetch(BASE_URL + "/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + API_KEY,
        "x-session-id": `flood-user-${reqId % 20}`
      },
      body: JSON.stringify({
        model: "mimo-v2.5-free",
        messages: [{ role: "user", content: prompt }],
        stream: true
      }),
      signal: controller.signal
    });

    const proxy = res.headers.get("x-router-proxy") || "unknown";
    if (!res.ok) {
      clearTimeout(timeoutId);
      return { reqId, status: res.status, proxy, elapsed: Date.now() - t0, tokens: 0 };
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let tokens = 0;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const chunk = decoder.decode(value, { stream: true });
      if (chunk.includes("data: ")) tokens++;
      if (chunk.includes("[DONE]")) break;
    }

    clearTimeout(timeoutId);
    return { reqId, status: res.status, proxy, elapsed: Date.now() - t0, tokens };
  } catch (err) {
    clearTimeout(timeoutId);
    return { reqId, status: 0, proxy: "timeout/abort", elapsed: Date.now() - t0, tokens: 0 };
  }
}

async function run10RpsFloodTest() {
  console.log("================================================================================");
  console.log("🔥 FLOOD STRESS TEST: 10 REQUESTS / DETIK SELAMA 2 MENIT (TOTAL 1.200 REQUESTS)");
  console.log("🎯 Target Model   : mimo-v2.5-free (Xiaomi LLM)");
  console.log("🌐 Target Armada  : 103 Vercel Edge Relays");
  console.log("⏱️ Durasi Testing : 120 Detik (10 req/s terus-menerus)");
  console.log("================================================================================\n");

  const DURATION_SECONDS = 120;
  const RATE_PER_SEC = 10;
  let totalLaunched = 0;
  const allResults = [];
  const activeProxies = new Set();

  const startTime = Date.now();
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));

  // Ticker per second
  for (let sec = 1; sec <= DURATION_SECONDS; sec++) {
    const secStart = Date.now();
    
    // Launch 10 requests concurrently for this second
    const batchPromises = [];
    for (let i = 0; i < RATE_PER_SEC; i++) {
      totalLaunched++;
      const currentReqId = totalLaunched;
      batchPromises.push(
        sendSingleRequest(currentReqId).then(res => {
          allResults.push(res);
          if (res.proxy && res.proxy !== "unknown" && res.proxy !== "timeout/abort") {
            activeProxies.add(res.proxy);
          }
          return res;
        })
      );
    }

    // Print status update every 10 seconds
    if (sec % 10 === 0 || sec === 1) {
      const finishedSoFar = allResults.length;
      const successSoFar = allResults.filter(r => r.status === 200).length;
      const failSoFar = finishedSoFar - successSoFar;
      const successRate = finishedSoFar > 0 ? ((successSoFar / finishedSoFar) * 100).toFixed(1) : "0.0";
      console.log(`⏱️ [Detik ${sec.toString().padStart(3, " ")}/120] Launched: ${totalLaunched} | Finished: ${finishedSoFar} | ✅ Sukses: ${successSoFar} (${successRate}%) | ⚠️ Gagal: ${failSoFar} | 🌐 Node Unik: ${activeProxies.size}`);
    }

    // Pace at 1000ms per second
    const elapsedSec = Date.now() - secStart;
    const waitTime = Math.max(0, 1000 - elapsedSec);
    await sleep(waitTime);
  }

  console.log("\n--------------------------------------------------------------------------------");
  console.log("⏳ Semua request (1.200) telah diluncurkan. Menunggu request tersisa selesai...");
  console.log("--------------------------------------------------------------------------------\n");

  // Wait for remaining pending requests up to 15s
  await sleep(15000);

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
  const totalFinished = allResults.length;
  const totalSuccess = allResults.filter(r => r.status === 200).length;
  const totalFailed = totalFinished - totalSuccess;
  const overallSuccessRate = ((totalSuccess / (totalFinished || 1)) * 100).toFixed(1);
  const totalTokens = allResults.filter(r => r.status === 200).reduce((a, b) => a + b.tokens, 0);
  const avgLatency = (allResults.filter(r => r.status === 200).reduce((a, b) => a + b.elapsed, 0) / (totalSuccess || 1)).toFixed(0);

  console.log("================================================================================");
  console.log(`🏁 HASIL AKHIR FLOOD TEST 10 RPS SELAMA 2 MENIT (DURASI ${totalTime}s)`);
  console.log("================================================================================");
  console.log(`📊 TOTAL REQUEST DILUNCURKAN: ${totalLaunched} Request`);
  console.log(`📈 TOTAL REQUEST SELESAI   : ${totalFinished} Request`);
  console.log(`✅ TOTAL SUCCESS (200 OK)  : ${totalSuccess} / ${totalFinished} (${overallSuccessRate}%)`);
  console.log(`⚠️ TOTAL FAIL / TIMEOUT    : ${totalFailed}`);
  console.log(`🌐 JUMLAH NODE RELAY UNIK  : ${activeProxies.size} / 103 Node`);
  console.log(`⚡ Rata-rata Latensi 200 OK : ${avgLatency} ms`);
  console.log(`📦 Total Tokens Streamed    : ${totalTokens} tokens`);
  console.log("================================================================================\n");
}

run10RpsFloodTest().catch(console.error);
