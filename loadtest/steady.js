import http from "k6/http";
import { check } from "k6";

const BASE = __ENV.BASE_URL || "http://localhost:8080";
const ADMIN = __ENV.ADMIN_KEY || "dev-admin-key";
const RECEIVER = __ENV.RECEIVER_URL || "http://demo-receiver:9090/hook";

export const options = {
  scenarios: {
    steady: {
      executor: "ramping-arrival-rate",
      startRate: 10,
      timeUnit: "1s",
      preAllocatedVUs: 50,
      stages: [
        { duration: "30s", target: 100 },
        { duration: "2m", target: 100 },
        { duration: "15s", target: 0 },
      ],
    },
  },
  thresholds: {
    http_req_duration: ["p(95)<100"], // ingest p95 under 100ms
    checks: ["rate>0.99"],
  },
};

export function setup() {
  const auth = { headers: { authorization: `Bearer ${ADMIN}`, "content-type": "application/json" } };
  const app = http.post(`${BASE}/api/v1/applications`, JSON.stringify({ name: `load-${Date.now()}` }), auth).json();
  http.post(`${BASE}/api/v1/applications/${app.id}/endpoints`,
    JSON.stringify({ url: RECEIVER, event_types: [] }), auth);
  return { appId: app.id, appKey: app.api_key };
}

const EVENTS = ["order.created", "order.updated", "user.created", "invoice.paid"];

export default function (data) {
  const res = http.post(
    `${BASE}/api/v1/applications/${data.appId}/messages`,
    JSON.stringify({
      event_type: EVENTS[Math.floor(Math.random() * EVENTS.length)],
      payload: { n: Math.random(), at: Date.now() },
    }),
    { headers: { authorization: `Bearer ${data.appKey}`, "content-type": "application/json" } },
  );
  check(res, { "accepted": (r) => r.status === 202 });
}
