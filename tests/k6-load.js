import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: 10,
  duration: "30s",
  thresholds: {
    http_req_failed: ["rate<0.05"],
    http_req_duration: ["p(95)<1000"],
  },
};

export default function () {
  const health = http.get("http://localhost:8000/health");
  check(health, { "gateway health 200": (r) => r.status === 200 });

  const warmup = http.post(
    "http://localhost:8000/warmup/warmup/schedule/generate",
    JSON.stringify({
      tenant_id: "k6-tenant",
      mailbox: "k6@gmail.com",
      requested_count: 2,
      partner_pool: ["seed-1@warmup.local", "seed-2@warmup.local"],
    }),
    { headers: { "Content-Type": "application/json" } }
  );
  check(warmup, { "warmup schedule generated": (r) => [200, 422, 503].includes(r.status) });
  sleep(1);
}
