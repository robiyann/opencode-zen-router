const API_KEY = "sk-zen-3a9bfa1bb6e7018da38779fd28a9f9a8";
const BASE_URL = "http://127.0.0.1:8080/v1";

const USER_TOPICS = [
  "Python Quicksort algorithm",
  "React useLocalStorage hook",
  "Go worker pool with channels",
  "PostgreSQL moving average query",
  "Dockerfile multi-stage Go build",
  "Express.js rate limiter middleware",
  "Bash script for MySQL backup",
  "Rust JSON parser without deps",
  "Regex for IPv4 and domain URL",
  "Python LRU Cache implementation",
  "Go clean architecture REST API",
  "React useDebounce custom hook",
  "Prisma schema for e-commerce",
  "Python Binary Search Tree",
  "Terraform script for AWS ECS",
  "PHP 8.2 AES-256-GCM encryption",
  "Python exponential backoff retry",
  "MongoDB aggregation pipeline",
  "Vue 3 Composition API virtual table",
  "C++20 coroutine Fibonacci",
  "Nginx reverse proxy with SSL cache",
  "JS deep merge nested objects",
  "Kubernetes Deployment and Service",
  "Redis pub-sub in Node.js",
  "Python JWT header payload parser",
  "Go concurrent merge sort",
  "React useMediaQuery hook",
  "SQL audit log table trigger",
  "Python Playwright async scraper",
  "Go Gin multi-file upload API",
  "GitHub Actions CI/CD pipeline",
  "TypeScript Luhn credit card check",
  "Python state machine with Enum",
  "Go graceful HTTP server shutdown",
  "Prometheus scrape config",
  "React useIntersectionObserver hook",
  "Python QR code generator SVG",
  "Rust UTF-8 in-place string reverse",
  "Shell script disk space Discord alert",
  "PostgreSQL UUIDv7 cursor pagination",
  "Python Pydantic model validator",
  "TypeScript custom Error hierarchy",
  "Go memory allocation benchmark",
  "GraphQL inventory schema & resolver",
  "Python asyncio.gather batching",
  "Next.js 14 JWT cookie middleware",
  "Pytest mock external API fixture",
  "PostgreSQL recursive CTE tree",
  "Docker Compose Kafka & Kafdrop",
  "Go sliding window rate limiter"
];

async function runUserTurn(userId, topic, turnNum) {
  const t0 = Date.now();
  const sessionId = `user-session-${userId}`;

  let prompt = "";
  if (turnNum === 1) {
    prompt = `Tuliskan implementasi dasar singkat untuk ${topic}. Jawab 1 kalimat kode intinya saja.`;
  } else if (turnNum === 2) {
    prompt = `Tambahkan error handling singkat pada kode ${topic} tadi. Jawab ringkas.`;
  } else {
    prompt = `Tuliskan unit test singkat untuk menguji kode ${topic} tersebut.`;
  }

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 25000); // 25s timeout

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
          { role: "system", content: "Kamu adalah AI coding assistant expert. Jawab dengan sangat ringkas kode intinya saja." },
          { role: "user", content: prompt }
        ],
        stream: true
      }),
      signal: controller.signal
    });

    const elapsed = Date.now() - t0;
    const proxy = res.headers.get("x-router-proxy") || "unknown";

    if (!res.ok) {
      clearTimeout(timeoutId);
      return { userId, turnNum, status: res.status, proxy, elapsed, tokens: 0 };
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let tokens = 0;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const chunk = decoder.decode(value, { stream: true });
      if (chunk.includes("data: ")) {
        tokens++;
      }
      if (chunk.includes("[DONE]")) break;
    }

    clearTimeout(timeoutId);
    return { userId, turnNum, status: res.status, proxy, elapsed: Date.now() - t0, tokens };
  } catch (err) {
    clearTimeout(timeoutId);
    return { userId, turnNum, status: 0, proxy: "timeout/abort", elapsed: Date.now() - t0, tokens: 0 };
  }
}

async function simulateSustainedLoad() {
  console.log("================================================================================");
  console.log("🔥 SUSTAINED MULTI-ROUND LOAD TEST: 50 USERS x 3 TURNS = 150 TOTAL SSE REQUESTS");
  console.log("🎯 Target Model   : mimo-v2.5-free (Xiaomi LLM)");
  console.log("🌐 Proxy Armada   : 103 Vercel Edge Relays");
  console.log("👥 Active Users   : 50 Developer Simultan");
  console.log("🔄 Total Putaran  : 3 Rintisan Koding per User (Total 150 Request SSE)");
  console.log("================================================================================\n");

  const startAll = Date.now();
  const allResults = [];
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));

  for (let round = 1; round <= 3; round++) {
    console.log(`--------------------------------------------------------------------------------`);
    console.log(`🚀 MEMULAI PUTARAN KE-${round} / 3 UNTUK 50 USER SIMULTAN...`);
    console.log(`--------------------------------------------------------------------------------`);
    const roundStart = Date.now();

    const roundPromises = USER_TOPICS.map((topic, idx) => {
      const userId = (idx + 1).toString().padStart(2, "0");
      return runUserTurn(userId, topic, round).then(res => {
        const mark = res.status === 200 ? "✅" : "⚠️";
        console.log(`  [Round ${round}] ${mark} User #${res.userId} (${res.elapsed}ms) -> Status ${res.status} via ${res.proxy.split(".app")[0] || res.proxy}`);
        return res;
      });
    });

    const roundResults = await Promise.all(roundPromises);
    allResults.push(...roundResults);

    const roundSuccess = roundResults.filter(r => r.status === 200).length;
    const roundDuration = ((Date.now() - roundStart) / 1000).toFixed(1);
    console.log(`\n📊 Putaran ${round} Selesai dalam ${roundDuration}s | Sukses: ${roundSuccess}/50 (${((roundSuccess/50)*100).toFixed(1)}%)\n`);

    if (round < 3) {
      console.log(`⏳ Jeda 3 detik sebelum Putaran ${round + 1}...\n`);
      await sleep(3000);
    }
  }

  const totalTime = ((Date.now() - startAll) / 1000).toFixed(1);
  console.log("\n================================================================================");
  console.log(`🏁 SIMULASI BEBAN BERKELANJUTAN 150 REQUEST SELESAI DALAM ${totalTime} DETIK!`);
  console.log("================================================================================");

  const successList = allResults.filter(r => r.status === 200);
  const failList = allResults.filter(r => r.status !== 200);

  console.log(`📈 TOTAL REQUEST SUKSES : ${successList.length} / 150 (${((successList.length / 150) * 100).toFixed(1)}%)`);
  console.log(`⚠️ TOTAL REQUEST GAGAL  : ${failList.length} / 150`);

  const nodeStats = {};
  allResults.forEach(r => {
    nodeStats[r.proxy] = (nodeStats[r.proxy] || 0) + 1;
  });

  const activeNodesCount = Object.keys(nodeStats).filter(k => k !== "unknown" && k !== "timeout/abort").length;
  console.log(`🌐 JUMLAH NODE RELAY BERHASIL DIGUNAKAN: ${activeNodesCount} / 103 NODE`);

  const totalTokens = successList.reduce((acc, cur) => acc + cur.tokens, 0);
  const avgLatency = (successList.reduce((acc, cur) => acc + cur.elapsed, 0) / (successList.length || 1)).toFixed(0);

  console.log(`⚡ Rata-rata Latensi per Turn     : ${avgLatency} ms`);
  console.log(`📦 Total Tokens Streamed (150 Reqs): ${totalTokens} tokens`);
  console.log("================================================================================\n");
}

simulateSustainedLoad().catch(console.error);
