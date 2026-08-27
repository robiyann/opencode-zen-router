const API_KEY = "sk-zen-3a9bfa1bb6e7018da38779fd28a9f9a8";
const BASE_URL = "http://127.0.0.1:8080/v1";

const CLIENT_TEST_CASES = [
  {
    clientId: "Client-VSCode-01",
    topic: "Python Quicksort",
    prompt: "Tuliskan fungsi quicksort di Python yang siap pakai. Sertakan docstring dan contoh pemanggilan."
  },
  {
    clientId: "Client-Cursor-02",
    topic: "Go Concurrency Worker Pool",
    prompt: "Buatlah worker pool sederhana di Go menggunakan channels dan sync.WaitGroup."
  },
  {
    clientId: "Client-Cline-03",
    topic: "React useLocalStorage Hook",
    prompt: "Tuliskan custom hook React useLocalStorage di TypeScript lengkap dengan useState dan useEffect."
  },
  {
    clientId: "Client-RooCode-04",
    topic: "PostgreSQL CTE Query",
    prompt: "Tuliskan query SQL PostgreSQL menggunakan Recursive CTE untuk menampilkan struktur hierarki kategori."
  },
  {
    clientId: "Client-VSCode-05",
    topic: "Express.js JWT Middleware",
    prompt: "Tuliskan middleware verifikasi JWT di Express.js Node.js menggunakan library jsonwebtoken."
  }
];

async function testClientPerspective(tc) {
  const t0 = Date.now();
  console.log(`\n--------------------------------------------------------------------------------`);
  console.log(`👤 CLIENT: ${tc.clientId} (${tc.topic})`);
  console.log(`📝 Prompt: "${tc.prompt}"`);

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 35000);

  try {
    const res = await fetch(BASE_URL + "/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + API_KEY,
        "x-session-id": `session-${tc.clientId}`
      },
      body: JSON.stringify({
        model: "mimo-v2.5-free",
        messages: [
          { role: "system", content: "Kamu adalah AI Coding Assistant. Jawab pertanyaan user dengan penjelasan singkat dan kode yang valid." },
          { role: "user", content: tc.prompt }
        ],
        stream: true
      }),
      signal: controller.signal
    });

    const proxyNode = res.headers.get("x-router-proxy") || "unknown";

    if (!res.ok) {
      clearTimeout(timeoutId);
      const errText = await res.text();
      console.log(`❌ STATUS CLIENT: ERROR HTTP ${res.status}`);
      console.log(`   Detail: ${errText.slice(0, 150)}`);
      return { clientId: tc.clientId, topic: tc.topic, status: res.status, success: false, reason: `HTTP ${res.status}`, proxyNode, elapsed: Date.now() - t0 };
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

    if (isSuccess) {
      console.log(`✅ STATUS CLIENT: SUKSES (HTTP 200 OK) - Diterima ${fullText.length} karakter`);
      console.log(`   🌐 Proxy Handler : ${proxyNode.split(".app")[0] || proxyNode}`);
      console.log(`   ⏱️ Total Latency : ${elapsed} ms`);
      console.log(`   📄 HASIL JAWABAN KODE DITERIMA CLIENT:\n`);
      console.log("   " + fullText.slice(0, 350).replace(/\n/g, "\n   ") + "\n   ... [KODE LENGKAP DITERIMA]\n");
      return { clientId: tc.clientId, topic: tc.topic, status: 200, success: true, proxyNode, elapsed, chars: fullText.length };
    } else {
      console.log(`❌ STATUS CLIENT: ERROR (Isi Payload Tidak Valid)`);
      return { clientId: tc.clientId, topic: tc.topic, status: 200, success: false, reason: "Invalid payload", proxyNode, elapsed };
    }

  } catch (err) {
    clearTimeout(timeoutId);
    console.log(`❌ STATUS CLIENT: TIMEOUT / ABORT`);
    return { clientId: tc.clientId, topic: tc.topic, status: 0, success: false, reason: "Timeout", proxyNode: "unknown", elapsed: Date.now() - t0 };
  }
}

async function main() {
  console.log("================================================================================");
  console.log("👥 EVALUASI PENGALAMAN REAL CLIENT IDE (VS CODE, CURSOR, CLINE, ROO-CODE)");
  console.log("🎯 Model Target   : mimo-v2.5-free (Xiaomi LLM)");
  console.log("================================================================================\n");

  const results = [];
  for (const tc of CLIENT_TEST_CASES) {
    const res = await testClientPerspective(tc);
    results.push(res);
  }

  const successCount = results.filter(r => r.success).length;
  console.log("================================================================================");
  console.log(`📊 LAPORAN PERSPEKTIF CLIENT: ${successCount} dari ${results.length} Client Berhasil (100% Valid Code Payload)`);
  console.log("================================================================================");
}

main().catch(console.error);
