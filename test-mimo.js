const API_KEY = "sk-zen-3a9bfa1bb6e7018da38779fd28a9f9a8";
const BASE_URL = "http://127.0.0.1:8080/v1";

async function testMimo() {
  console.log("========================================================");
  console.log("🚀 TESTING MODEL: mimo-v2.5-free (ROUTING & ROTATION)");
  console.log("========================================================\n");

  const prompts = [
    { prompt: "Perkenalkan dirimu dalam 1 kalimat singkat.", stream: false },
    { prompt: "Hitung angka 1 sampai 5 saja.", stream: true },
    { prompt: "Apa ibukota Indonesia saat ini? Jawab 1 kata.", stream: false },
    { prompt: "Sebutkan 3 warna primer dalam 1 baris.", stream: true }
  ];

  const routedNodes = [];

  for (let i = 0; i < prompts.length; i++) {
    const item = prompts[i];
    const t0 = Date.now();
    console.log(`[Request #${i + 1}] (${item.stream ? "STREAMING (SSE)" : "NON-STREAMING (JSON)"})`);
    console.log(`Prompt: "${item.prompt}"`);

    const res = await fetch(BASE_URL + "/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + API_KEY
      },
      body: JSON.stringify({
        model: "mimo-v2.5-free",
        messages: [{ role: "user", content: item.prompt }],
        stream: item.stream
      })
    });

    const elapsed = Date.now() - t0;
    const routedNode = res.headers.get("x-router-proxy");
    routedNodes.push(routedNode);

    console.log(`  -> HTTP Status : ${res.status}`);
    console.log(`  -> Routed Node : ${routedNode}`);
    console.log(`  -> Latency     : ${elapsed}ms`);

    if (item.stream) {
      const text = await res.text();
      const lines = text.split("\n");
      let fullContent = "";
      for (const line of lines) {
        if (line.startsWith("data: ") && !line.includes("[DONE]")) {
          try {
            const data = JSON.parse(line.substring(6));
            fullContent += data.choices?.[0]?.delta?.content || "";
          } catch (e) {}
        }
      }
      console.log(`  -> Mimo Reply  : "${fullContent.trim()}"`);
    } else {
      const data = await res.json();
      const reply = data.choices?.[0]?.message?.content || "";
      console.log(`  -> Mimo Reply  : "${reply.trim()}"`);
    }
    console.log("--------------------------------------------------------\n");
  }

  console.log("========================================================");
  console.log("📊 ROTATION VERIFICATION SUMMARY:");
  routedNodes.forEach((node, idx) => {
    console.log(`  • Req #${idx + 1} handled by: ${node}`);
  });
  console.log("========================================================");
}

testMimo().catch(console.error);
