const API_KEY = "sk-zen-3a9bfa1bb6e7018da38779fd28a9f9a8";
const BASE_URL = "http://127.0.0.1:8080/v1";

async function runFloodTest() {
  console.log("==========================================================");
  console.log("🌊 FLOOD TESTING: 25 RAPID REQUESTS ACROSS 21 RELAYS");
  console.log("==========================================================\n");

  const totalRequests = 25;
  const nodeHits = {};
  const results = [];

  for (let i = 1; i <= totalRequests; i++) {
    const t0 = Date.now();
    try {
      const res = await fetch(BASE_URL + "/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": "Bearer " + API_KEY
        },
        body: JSON.stringify({
          model: "nemotron-3-ultra-free",
          messages: [{ role: "user", content: `Ping test #${i}` }],
          stream: false
        })
      });

      const elapsed = Date.now() - t0;
      const proxy = res.headers.get("x-router-proxy") || "unknown";
      nodeHits[proxy] = (nodeHits[proxy] || 0) + 1;

      console.log(`[Req #${i.toString().padStart(2, '0')}] Status: ${res.status} | Time: ${elapsed.toString().padStart(4, ' ')}ms | Node: ${proxy}`);
      results.push({ id: i, status: res.status, proxy, elapsed });
    } catch (err) {
      console.error(`[Req #${i}] Failed: ${err.message}`);
    }
  }

  console.log("\n==========================================================");
  console.log("📊 ROUND-ROBIN DISTRIBUTION BREAKDOWN:");
  console.log("==========================================================");

  const sortedNodes = Object.entries(nodeHits).sort((a, b) => b[1] - a[1]);
  console.log(`Unique Nodes Activated: ${sortedNodes.length} nodes!`);
  console.log("----------------------------------------------------------");
  sortedNodes.forEach(([node, count], idx) => {
    console.log(` ${(idx + 1).toString().padStart(2, ' ')}. [${count}x requests] -> ${node}`);
  });

  console.log("==========================================================");
  const successCount = results.filter(r => r.status === 200).length;
  console.log(`🎯 OVERALL RESULT: ${successCount}/${totalRequests} SUCCEEDED (${((successCount/totalRequests)*100).toFixed(1)}%)`);
  console.log("==========================================================");
}

runFloodTest().catch(console.error);
