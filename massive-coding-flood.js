const API_KEY = "sk-zen-3a9bfa1bb6e7018da38779fd28a9f9a8";
const BASE_URL = "http://127.0.0.1:8080/v1";

const CODING_PROMPTS = [
  "Tulis fungsi Python quicksort dengan type hints dan docstring.",
  "Buat React hook useLocalStorage yang type-safe dengan TypeScript.",
  "Tulis fungsi Go worker pool dengan channel dan sync.WaitGroup.",
  "Buat query SQL PostgreSQL untuk mencari moving average 7 hari.",
  "Tulis Dockerfile multi-stage build untuk aplikasi Go binary kecil.",
  "Buat middleware Express.js untuk rate limiting menggunakan token bucket.",
  "Tulis script Bash untuk backup database MySQL ke AWS S3.",
  "Buat fungsi Rust untuk parsing JSON string tanpa library eksternal.",
  "Tulis regex komprehensif untuk validasi URL IPv4 dan domain dengan port.",
  "Buat implementasi LRU Cache di Python menggunakan collections.OrderedDict.",
  "Tulis arsitektur clean architecture folder structure untuk REST API Go.",
  "Buat hook React useDebounce dengan cancel method.",
  "Tulis schema Prisma untuk sistem e-commerce multitenant.",
  "Buat fungsi Python binary search tree traversal inorder, preorder, postorder.",
  "Tulis script Terraform untuk provision AWS ECS Fargate cluster.",
  "Buat fungsi PHP 8.2 untuk enkripsi data AES-256-GCM.",
  "Tulis decorator Python untuk retry exponential backoff.",
  "Buat query MongoDB aggregation pipeline untuk facet and pagination.",
  "Tulis komponen Vue 3 Composition API untuk virtualized list table.",
  "Buat fungsi C++20 coroutine generator untuk Fibonacci sequence.",
  "Tulis konfigurasi Nginx reverse proxy dengan caching SSL dan gzip.",
  "Buat fungsi JavaScript untuk deep merge dua nested object.",
  "Tulis Kubernetes Deployment and Service YAML dengan health probe.",
  "Buat arsitektur pub-sub Redis di Node.js dengan reconnect handler.",
  "Tulis fungsi Python untuk parsing dan parsing JWT header payload.",
  "Buat fungsi Go untuk merge sort concurrent menggunakan goroutines.",
  "Tulis custom React hook useMediaQuery dengan listener resize.",
  "Buat SQL schema untuk audit log table dengan trigger update timestamp.",
  "Tulis script Python scrap web menggunakan Playwright async.",
  "Buat REST API handler Go Gin untuk upload multiple files ke disk.",
  "Tulis pipeline GitHub Actions CI/CD untuk test, build, dan deploy Docker image.",
  "Buat fungsi TypeScript untuk memvalidasi nomor kartu kredit algoritma Luhn.",
  "Tulis state machine sederhana di Python dengan enum class.",
  "Buat fungsi Go untuk gracefully shutdown HTTP server saat SIGINT.",
  "Tulis konfigurasi Prometheus dan Alertmanager untuk scrape microservice.",
  "Buat hook React useIntersectionObserver untuk infinite scroll.",
  "Tulis script Python untuk generate QR code SVG.",
  "Buat fungsi Rust untuk reverse string UTF-8 in-place.",
  "Tulis script shell untuk monitor disk space dan kirim webhook Discord jika > 90%.",
  "Buat pagination cursor-based di SQL PostgreSQL dengan UUIDv7.",
  "Tulis fungsi Python decorator untuk memvalidasi pydantic model schema.",
  "Buat custom Error class hierarki di TypeScript dengan serialize JSON.",
  "Tulis fungsi Go untuk benchmark memory allocation string concatenation.",
  "Buat GraphQL schema dan resolver untuk sistem inventaris gudang.",
  "Tulis fungsi Python asynchronous batch processing dengan asyncio.gather.",
  "Buat middleware Next.js 14 untuk proteksi route berdasarkan JWT cookie.",
  "Tulis unit test Pytest dengan fixture mocking external API request.",
  "Buat query SQL recursive CTE untuk tree category hierarchy.",
  "Tulis script Docker compose untuk Kafka, Zookeeper, dan Kafdrop.",
  "Buat fungsi Go rate limiter sliding window log di in-memory."
];

async function simulateCodingUser(userId, prompt) {
  const t0 = Date.now();
  const sessionId = `user-session-${userId}`;

  try {
    const res = await fetch(BASE_URL + "/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + API_KEY,
        "x-session-id": sessionId
      },
      body: JSON.stringify({
        model: "mimo-v2.5-free",
        messages: [
          { role: "system", content: "Kamu adalah AI coding assistant expert. Jawab dengan ringkas kode intinya saja." },
          { role: "user", content: prompt }
        ],
        stream: true
      })
    });

    const elapsed = Date.now() - t0;
    const proxy = res.headers.get("x-router-proxy") || "unknown";

    if (!res.ok) {
      const errText = await res.text();
      return { userId, status: res.status, proxy, elapsed, error: errText, prompt };
    }

    // Read SSE Stream chunks
    const text = await res.text();
    const lines = text.split("\n");
    let tokens = 0;
    let snippet = "";

    for (const line of lines) {
      if (line.startsWith("data: ") && !line.includes("[DONE]")) {
        tokens++;
        try {
          const data = JSON.parse(line.substring(6));
          const delta = data.choices?.[0]?.delta?.content || "";
          if (snippet.length < 50) snippet += delta;
        } catch (e) {}
      }
    }

    return {
      userId,
      status: res.status,
      proxy,
      elapsed,
      tokens,
      snippet: snippet.replace(/\n/g, " ").trim(),
      prompt
    };
  } catch (err) {
    return {
      userId,
      status: 0,
      proxy: "network-fail",
      elapsed: Date.now() - t0,
      error: err.message,
      prompt
    };
  }
}

async function runMassiveTest() {
  console.log("================================================================================");
  console.log("⚡ SIMULASI BEBAN TINGGI: 50 USER SIMULTAN AKSES MIMO CODING (STREAMING SSE)");
  console.log(`🎯 Target Model   : mimo-v2.5-free`);
  console.log(`🌐 Total Armada   : 21 Vercel Edge Relays`);
  console.log(`👥 Concurrent User: 50 User Bersamaan`);
  console.log("================================================================================\n");

  const startAll = Date.now();
  let completedCount = 0;

  console.log("🚀 Memicu 50 request secara bersamaan sekarang...");

  // Launch all 50 users concurrently
  const userPromises = CODING_PROMPTS.map((prompt, idx) => {
    const userId = (idx + 1).toString().padStart(2, "0");
    return simulateCodingUser(userId, prompt).then(result => {
      completedCount++;
      const mark = result.status === 200 ? "✅" : "⚠️";
      console.log(`[${completedCount.toString().padStart(2, "0")}/50] ${mark} User #${result.userId} (${result.elapsed}ms) -> Status ${result.status} via ${result.proxy.split(".app")[0] || result.proxy}`);
      return result;
    });
  });

  const results = await Promise.all(userPromises);
  const totalDuration = ((Date.now() - startAll) / 1000).toFixed(1);

  console.log("\n================================================================================");
  console.log(`🏁 SIMULASI 50 USER SELESAI DALAM ${totalDuration} DETIK!`);
  console.log("================================================================================");

  // Group by status
  const success = results.filter(r => r.status === 200);
  const failed = results.filter(r => r.status !== 200);

  console.log(`📈 SUKSES : ${success.length} / 50 (${((success.length / 50) * 100).toFixed(1)}%)`);
  console.log(`⚠️ GAGAL  : ${failed.length} / 50`);

  // Group by node usage
  const nodeStats = {};
  results.forEach(r => {
    nodeStats[r.proxy] = (nodeStats[r.proxy] || 0) + 1;
  });

  console.log("\n📊 DISTRIBUSI BEBAN KE 21 NODE RELAY VERCEL:");
  console.log("--------------------------------------------------------------------------------");
  Object.entries(nodeStats)
    .sort((a, b) => b[1] - a[1])
    .forEach(([node, count], idx) => {
      console.log(`  ${(idx + 1).toString().padStart(2, " ")}. [${count} user ditangani] -> ${node}`);
    });

  console.log("--------------------------------------------------------------------------------");
  const totalTokens = success.reduce((acc, cur) => acc + (cur.tokens || 0), 0);
  const avgLatency = (success.reduce((acc, cur) => acc + cur.elapsed, 0) / (success.length || 1)).toFixed(0);
  console.log(`⚡ Rata-rata Latensi Streaming : ${avgLatency} ms`);
  console.log(`📦 Total Tokens Streamed       : ${totalTokens} tokens`);
  console.log("================================================================================\n");
}

runMassiveTest().catch(console.error);
