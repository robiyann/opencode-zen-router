const API_KEY = "sk-zen-3a9bfa1bb6e7018da38779fd28a9f9a8";
const BASE_URL = "http://127.0.0.1:8080/v1";

async function testModels() {
  console.log("==========================================================");
  console.log("🧪 TESTING MODELS: x-preview-f-free & hy3-free");
  console.log("==========================================================\n");

  const modelsToTest = [
    { id: "x-preview-f-free", name: "X-Preview Free", prompt: "Halo! Ceritakan dalam 1 kalimat singkat siapa dirimu." },
    { id: "hy3-free", name: "Hunyuan 3 Free (hy3)", prompt: "Halo! Ceritakan dalam 1 kalimat singkat tentang dirimu." }
  ];

  for (const item of modelsToTest) {
    console.log(`[MODEL TEST] -> ${item.id} (${item.name})`);
    console.log(`Prompt: "${item.prompt}"`);
    const t0 = Date.now();

    try {
      const res = await fetch(BASE_URL + "/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": "Bearer " + API_KEY
        },
        body: JSON.stringify({
          model: item.id,
          messages: [{ role: "user", content: item.prompt }],
          stream: false
        })
      });

      const elapsed = Date.now() - t0;
      const proxy = res.headers.get("x-router-proxy");
      console.log(`  -> HTTP Status : ${res.status}`);
      console.log(`  -> Routed Node : ${proxy}`);
      console.log(`  -> Latency     : ${elapsed}ms`);

      if (res.status === 200) {
        const data = await res.json();
        const reply = data.choices?.[0]?.message?.content || "";
        console.log(`  -> Reply       : "${reply.trim()}"`);
        console.log(`  ✅ [PASS] Model ${item.id} aktif dan merespon normal!`);
      } else {
        const errText = await res.text();
        console.log(`  ❌ [FAIL] Error Body: ${errText}`);
      }
    } catch (e) {
      console.error(`  ❌ [ERROR]: ${e.message}`);
    }

    console.log("----------------------------------------------------------\n");
  }
}

testModels().catch(console.error);
