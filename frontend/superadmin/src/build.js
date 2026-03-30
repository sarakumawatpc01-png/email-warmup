import fs from 'fs';
import path from 'path';

const distDir = path.resolve('src/dist');
fs.mkdirSync(distDir, { recursive: true });

const html = `<!doctype html>
<html>
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Superadmin Control Plane</title>
    <script src="https://cdn.tailwindcss.com"></script>
  </head>
  <body class="bg-gray-100 text-gray-900">
    <div class="max-w-7xl mx-auto p-6">
      <h1 class="text-2xl font-bold mb-4">Superadmin Control Plane</h1>
      <div class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <div class="bg-white p-4 rounded shadow">MRR: $24,500</div>
        <div class="bg-white p-4 rounded shadow">Clients: 72</div>
        <div class="bg-white p-4 rounded shadow">Emails Sent: 1.2M</div>
        <div class="bg-white p-4 rounded shadow">System Health: <span id="global-health">Loading...</span></div>
      </div>

      <div class="bg-white p-4 rounded shadow mb-4">
        <div class="flex flex-wrap gap-2 mb-3">
          <button class="tab-btn px-3 py-2 rounded bg-indigo-600 text-white" data-tab="internal">Internal Network IDs</button>
          <button class="tab-btn px-3 py-2 rounded bg-gray-200" data-tab="clients">Client Mailboxes</button>
          <button class="tab-btn px-3 py-2 rounded bg-gray-200" data-tab="health">Health & Analytics</button>
          <button class="tab-btn px-3 py-2 rounded bg-gray-200" data-tab="payments">Payment Gateway Setup</button>
        </div>

        <div id="tab-internal" class="tab-pane">
          <h2 class="font-semibold mb-2">Internal Seed Network Registry</h2>
          <div class="grid md:grid-cols-4 gap-2 mb-2">
            <input id="internal-tenant" class="border rounded px-2 py-1" placeholder="tenant_id" />
            <input id="internal-mailbox" class="border rounded px-2 py-1" placeholder="mailbox" />
            <input id="internal-notes" class="border rounded px-2 py-1" placeholder="notes" />
            <button id="add-internal" class="bg-indigo-600 text-white rounded px-3 py-1">Add / Update</button>
          </div>
          <pre id="internal-list" class="bg-gray-50 border rounded p-2 text-xs overflow-auto max-h-72"></pre>
        </div>

        <div id="tab-clients" class="tab-pane hidden">
          <h2 class="font-semibold mb-2">Client Mailboxes</h2>
          <div class="grid md:grid-cols-4 gap-2 mb-2">
            <input id="health-tenant" class="border rounded px-2 py-1" placeholder="tenant_id" />
            <input id="health-mailbox" class="border rounded px-2 py-1" placeholder="mailbox" />
            <input id="health-limit" class="border rounded px-2 py-1" value="20" />
            <button id="fetch-health" class="bg-indigo-600 text-white rounded px-3 py-1">Fetch Health</button>
          </div>
          <pre id="mailbox-health" class="bg-gray-50 border rounded p-2 text-xs overflow-auto max-h-72"></pre>
        </div>

        <div id="tab-health" class="tab-pane hidden">
          <h2 class="font-semibold mb-2">Operations (DLQ + Audit)</h2>
          <div class="grid md:grid-cols-3 gap-2 mb-2">
            <input id="dlq-index" class="border rounded px-2 py-1" value="0" />
            <input id="dlq-approved-by" class="border rounded px-2 py-1" value="superadmin" />
            <button id="replay-dlq" class="bg-amber-600 text-white rounded px-3 py-1">Replay DLQ Item</button>
          </div>
          <pre id="dlq-data" class="bg-gray-50 border rounded p-2 text-xs overflow-auto max-h-44 mb-3"></pre>
          <pre id="audit-data" class="bg-gray-50 border rounded p-2 text-xs overflow-auto max-h-44"></pre>
        </div>

        <div id="tab-payments" class="tab-pane hidden">
          <h2 class="font-semibold mb-2">Payment Gateway Setup</h2>
          <p class="text-sm text-gray-700 mb-2">Supports Stripe, Razorpay, PhonePe, Paytm setup and readiness checks.</p>
          <div class="grid md:grid-cols-4 gap-2 mb-2">
            <select id="payment-provider" class="border rounded px-2 py-1">
              <option value="stripe">Stripe</option>
              <option value="razorpay">Razorpay</option>
              <option value="phonepe">PhonePe</option>
              <option value="paytm">Paytm</option>
            </select>
            <input id="payment-key" class="border rounded px-2 py-1" placeholder="key_id / merchant_id" />
            <input id="payment-secret" class="border rounded px-2 py-1" placeholder="secret (optional)" />
            <button id="save-payment" class="bg-indigo-600 text-white rounded px-3 py-1">Save Setup</button>
          </div>
          <pre id="payment-data" class="bg-gray-50 border rounded p-2 text-xs overflow-auto max-h-72"></pre>
        </div>
      </div>

      <div class="bg-white p-4 rounded shadow">
        <h2 class="font-semibold mb-2">Recent Logs</h2>
        <p class="text-sm text-gray-700">Control plane enabled. Review audit log panel for sensitive actions.</p>
      </div>
    </div>
    <script>
      const adminHeaders = {
        'Content-Type': 'application/json',
        'x-admin-api-key': localStorage.getItem('superadmin_api_key') || '',
        'x-admin-actor': localStorage.getItem('superadmin_actor') || 'superadmin-ui',
        'Authorization': localStorage.getItem('superadmin_auth_token') ? ('Bearer ' + localStorage.getItem('superadmin_auth_token')) : '',
      };
      const q = (id) => document.getElementById(id);
      function show(tab) {
        document.querySelectorAll('.tab-pane').forEach((el) => el.classList.add('hidden'));
        document.querySelectorAll('.tab-btn').forEach((el) => {
          el.classList.remove('bg-indigo-600', 'text-white');
          el.classList.add('bg-gray-200');
        });
        q('tab-' + tab).classList.remove('hidden');
        document.querySelector('[data-tab="' + tab + '"]').classList.add('bg-indigo-600', 'text-white');
      }
      document.querySelectorAll('.tab-btn').forEach((btn) => btn.onclick = () => show(btn.dataset.tab));

      async function loadInternal() {
        const res = await fetch('/warmup/admin/internal-mailboxes');
        q('internal-list').textContent = JSON.stringify(await res.json(), null, 2);
      }
      async function loadDlqAndAudit() {
        const dlq = await fetch('/warmup/worker/dlq');
        q('dlq-data').textContent = JSON.stringify(await dlq.json(), null, 2);
        const audit = await fetch('/warmup/admin/audit-logs?limit=20');
        q('audit-data').textContent = JSON.stringify(await audit.json(), null, 2);
      }
      async function loadPayments() {
        const res = await fetch('/billing/admin/payments/providers', { headers: adminHeaders });
        q('payment-data').textContent = JSON.stringify(await res.json(), null, 2);
      }

      q('add-internal').onclick = async () => {
        await fetch('/warmup/admin/internal-mailboxes', {
          method: 'POST',
          headers: adminHeaders,
          body: JSON.stringify({
            tenant_id: q('internal-tenant').value,
            mailbox: q('internal-mailbox').value,
            notes: q('internal-notes').value
          })
        });
        await loadInternal();
      };
      q('fetch-health').onclick = async () => {
        const url = '/warmup/admin/mailbox-health?tenant_id=' + encodeURIComponent(q('health-tenant').value)
          + '&mailbox=' + encodeURIComponent(q('health-mailbox').value)
          + '&limit=' + encodeURIComponent(q('health-limit').value || '20');
        const res = await fetch(url);
        q('mailbox-health').textContent = JSON.stringify(await res.json(), null, 2);
      };
      q('replay-dlq').onclick = async () => {
        await fetch('/warmup/worker/dlq/replay', {
          method: 'POST',
          headers: adminHeaders,
          body: JSON.stringify({
            item_index: Number(q('dlq-index').value || '0'),
            approved_by: q('dlq-approved-by').value || 'superadmin',
            reason: 'manual_replay_from_superadmin'
          })
        });
        await loadDlqAndAudit();
      };
      q('save-payment').onclick = async () => {
        const provider = q('payment-provider').value;
        const payload = provider === 'stripe' || provider === 'razorpay'
          ? { key_id: q('payment-key').value, secret: q('payment-secret').value }
          : { merchant_id: q('payment-key').value, secret: q('payment-secret').value };
        await fetch('/billing/admin/payments/providers/' + provider + '/setup', {
          method: 'POST',
          headers: adminHeaders,
          body: JSON.stringify(payload)
        });
        await loadPayments();
      };

      fetch('/warmup/health')
        .then((r) => r.json())
        .then((data) => { q('global-health').textContent = data.status || 'unknown'; })
        .catch(() => { q('global-health').textContent = 'degraded'; });
      loadInternal();
      loadDlqAndAudit();
      loadPayments();
    </script>
  </body>
</html>`;

fs.writeFileSync(path.join(distDir, 'index.html'), html);
