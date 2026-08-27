const API_KEY = "sk-zen-3a9bfa1bb6e7018da38779fd28a9f9a8";
const BASE_URL = "http://127.0.0.1:8080/v1";

async function runScenario(name, model, prompt) {
  console.log(`\n================================================================================`);
  console.log(`🧪 TESTING: ${name}`);
  console.log(`🎯 Model Target : ${model}`);
  console.log(`================================================================================`);

  const t0 = Date.now();
  const res = await fetch(BASE_URL + "/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": "Bearer " + API_KEY
    },
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
  console.log(`📡 Status Code     : ${res.status}`);

  if (!res.ok) {
    const errText = await res.text();
    console.log(`❌ Error Output   : ${errText}`);
    return;
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let content = "";
  let ttft = null;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    if (!ttft) ttft = Date.now() - t0;
    const chunk = decoder.decode(value, { stream: true });
    const lines = chunk.split("\n");
    for (const line of lines) {
      if (line.startsWith("data: ") && !line.includes("[DONE]")) {
        try {
          const parsed = JSON.parse(line.slice(6));
          const delta = parsed.choices?.[0]?.delta?.content || "";
          if (delta) content += delta;
        } catch (e) {}
      }
    }
  }

  const elapsed = Date.now() - t0;
  console.log(`⏱️ Time-To-First-Token (TTFT) : ${ttft || elapsed} ms`);
  console.log(`⏱️ Total Latency               : ${elapsed} ms`);
  console.log(`📄 Total Output Length         : ${content.length} chars`);
  console.log(`\n--- 💬 AI RESPONSE SNIPPET (500 CHARS PREVIEW) ---`);
  console.log(content.slice(0, 500) + (content.length > 500 ? "\n... [TRUNCATED FOR DISPLAY]" : ""));
  console.log(`--------------------------------------------------------------------------------\n`);
}

async function main() {
  console.log("🚀 STARTING SCENARIO 1: LeetCode Hard Minimum Window Substring (Mimo)");
  await runScenario(
    "Test 1: LeetCode Hard Minimum Window Substring (Mimo)",
    "mimo-v2.5-free",
    "Tuliskan fungsi LeetCode Hard Minimum Window Substring di Python O(N) lengkap dengan penjelasan penalaran logika step-by-step."
  );

  console.log("🚀 STARTING SCENARIO 2: High Concurrency 1M WebSocket Manager (Nemotron)");
  await runScenario(
    "Test 2: High Concurrency 1 Million WebSocket Manager di Go (Nemotron)",
    "nemotron-3-ultra-free",
    "Rancang arsitektur WebSocket Pool Manager di Go untuk 1 juta koneksi serentak yang thread-safe dengan Mutex & atomic counter."
  );

  console.log("🚀 STARTING SCENARIO 3: Debugging Goroutine Leak (x-preview-f)");
  await runScenario(
    "Test 3: Debugging Goroutine Memory Leak & Map Race Condition (x-preview-f)",
    "x-preview-f-free",
    "Jelaskan cara mendeteksi dan memperbaiki goroutine memory leak dan data race condition pada map di Go."
  );

  console.log("🎉 ALL REAL DEVELOPER SCENARIOS TESTED SUCCESSFULLY!");
}

main().catch(console.error);
