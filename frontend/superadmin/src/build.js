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
      <div class="flex items-center justify-between mb-4">
        <h1 class="text-2xl font-bold">Superadmin Control Plane</h1>
        <div class="flex items-center gap-2 text-sm">
          <span id="superadmin-user" class="text-gray-600"></span>
          <button id="superadmin-logout" class="hidden bg-slate-800 text-white rounded px-3 py-1">Logout</button>
        </div>
      </div>
      <div id="banner" class="hidden mb-3 rounded border px-3 py-2 text-sm"></div>

      <div id="superadmin-auth" class="bg-white p-4 rounded shadow mb-6">
        <h2 class="font-semibold mb-2">Superadmin Login / Setup</h2>
        <p class="text-sm text-gray-700 mb-3">Login if your superadmin exists. Use signup once to bootstrap in a fresh environment.</p>
        <div class="grid md:grid-cols-5 gap-2 mb-2">
          <input id="superadmin-email" class="border rounded px-2 py-1 md:col-span-2" placeholder="email" />
          <input id="superadmin-password" type="password" class="border rounded px-2 py-1" placeholder="password" />
          <input id="superadmin-tenant" class="border rounded px-2 py-1" placeholder="tenant_id (signup)" value="system" />
          <input id="superadmin-api-key" class="border rounded px-2 py-1" placeholder="x-admin-api-key (optional)" />
        </div>
        <div class="flex gap-2">
          <button id="superadmin-login" class="bg-indigo-600 text-white rounded px-3 py-1">Login</button>
          <button id="superadmin-signup" class="bg-emerald-600 text-white rounded px-3 py-1">Signup Superadmin</button>
        </div>
      </div>

      <div id="superadmin-dashboard" class="hidden">
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
            <button class="tab-btn px-3 py-2 rounded bg-gray-200" data-tab="billing">Billing Admin</button>
            <button class="tab-btn px-3 py-2 rounded bg-gray-200" data-tab="audit">Audit Logs</button>
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

          <div id="tab-billing" class="tab-pane hidden">
            <h2 class="font-semibold mb-2">Billing Admin Integration</h2>
            <div class="grid md:grid-cols-5 gap-2 mb-2">
              <input id="billing-limit" class="border rounded px-2 py-1" value="20" />
              <button id="billing-refresh-runs" class="bg-indigo-600 text-white rounded px-3 py-1">Reconciliation Runs</button>
              <button id="billing-refresh-queue" class="bg-indigo-600 text-white rounded px-3 py-1">Queue</button>
              <button id="billing-refresh-dlq" class="bg-indigo-600 text-white rounded px-3 py-1">DLQ</button>
              <button id="billing-worker-pass" class="bg-amber-600 text-white rounded px-3 py-1">Run Worker Pass</button>
            </div>
            <div class="grid md:grid-cols-3 gap-2 mb-2">
              <button id="billing-refresh-effects" class="bg-gray-800 text-white rounded px-3 py-1">Effects</button>
              <button id="billing-refresh-inbox" class="bg-gray-800 text-white rounded px-3 py-1">Inbox</button>
              <button id="billing-refresh-outbox" class="bg-gray-800 text-white rounded px-3 py-1">Outbox</button>
            </div>
            <pre id="billing-data" class="bg-gray-50 border rounded p-2 text-xs overflow-auto max-h-72"></pre>
          </div>

          <div id="tab-audit" class="tab-pane hidden">
            <h2 class="font-semibold mb-2">Audit Controls (filter/export/retention)</h2>
            <div class="grid md:grid-cols-5 gap-2 mb-2">
              <input id="audit-actor" class="border rounded px-2 py-1" placeholder="actor" />
              <input id="audit-action" class="border rounded px-2 py-1" placeholder="action/event" />
              <input id="audit-resource-type" class="border rounded px-2 py-1" placeholder="resource_type" />
              <input id="audit-since" class="border rounded px-2 py-1" placeholder="since ISO8601 (e.g. 2026-03-31T00:00:00Z)" />
              <button id="audit-filter" class="bg-indigo-600 text-white rounded px-3 py-1">Filter</button>
            </div>
            <div class="grid md:grid-cols-4 gap-2 mb-2">
              <button id="audit-export-warmup" class="bg-gray-800 text-white rounded px-3 py-1">Export Warmup CSV</button>
              <button id="audit-export-billing" class="bg-gray-800 text-white rounded px-3 py-1">Export Billing CSV</button>
              <input id="audit-retention-days" class="border rounded px-2 py-1" value="90" />
              <button id="audit-retention-run" class="bg-amber-600 text-white rounded px-3 py-1">Run Retention</button>
            </div>
            <pre id="audit-filter-output" class="bg-gray-50 border rounded p-2 text-xs overflow-auto max-h-72"></pre>
          </div>
        </div>
      </div>
    </div>
    <script>
      const q = (id) => document.getElementById(id);
      const TOKEN_KEY = 'superadmin_auth_token';
      const PROFILE_KEY = 'superadmin_profile';
      const API_KEY = 'superadmin_api_key';
      const ACTOR_KEY = 'superadmin_actor';
      const banner = (text, level = 'info') => {
        const el = q('banner');
        el.classList.remove('hidden', 'border-red-200', 'bg-red-50', 'text-red-700', 'border-emerald-200', 'bg-emerald-50', 'text-emerald-700');
        if (level === 'error') {
          el.classList.add('border-red-200', 'bg-red-50', 'text-red-700');
        } else {
          el.classList.add('border-emerald-200', 'bg-emerald-50', 'text-emerald-700');
        }
        el.textContent = text;
      };
      const validateEmail = (email) => /^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$/.test(email || '');
      const validateTenant = (tenant) => typeof tenant === 'string' && tenant.trim().length >= 2;
      const withLoading = async (btn, action) => {
        const original = btn.textContent;
        btn.disabled = true;
        btn.textContent = 'Working...';
        try {
          await action();
        } finally {
          btn.disabled = false;
          btn.textContent = original;
        }
      };
      const adminHeaders = () => ({
        'Content-Type': 'application/json',
        'x-admin-api-key': localStorage.getItem(API_KEY) || '',
        'x-admin-actor': localStorage.getItem(ACTOR_KEY) || 'superadmin-ui',
        'Authorization': localStorage.getItem(TOKEN_KEY) ? ('Bearer ' + localStorage.getItem(TOKEN_KEY)) : '',
      });
      const setSignedOut = () => {
        localStorage.removeItem(TOKEN_KEY);
        localStorage.removeItem(PROFILE_KEY);
        q('superadmin-auth').classList.remove('hidden');
        q('superadmin-dashboard').classList.add('hidden');
        q('superadmin-logout').classList.add('hidden');
        q('superadmin-user').textContent = '';
      };
      const setSignedIn = (token, profile) => {
        localStorage.setItem(TOKEN_KEY, token);
        localStorage.setItem(PROFILE_KEY, JSON.stringify(profile));
        q('superadmin-auth').classList.add('hidden');
        q('superadmin-dashboard').classList.remove('hidden');
        q('superadmin-logout').classList.remove('hidden');
        q('superadmin-user').textContent = profile && profile.email ? (profile.email + ' (' + (profile.role || 'superadmin') + ')') : 'Logged in';
      };
      const parseAuthForm = () => {
        const email = q('superadmin-email').value.trim().toLowerCase();
        const password = q('superadmin-password').value;
        const tenantId = q('superadmin-tenant').value.trim();
        const apiKey = q('superadmin-api-key').value.trim();
        if (!validateEmail(email)) {
          throw new Error('Please provide a valid email.');
        }
        if (!password || password.length < 8) {
          throw new Error('Password must be at least 8 characters.');
        }
        if (apiKey) {
          localStorage.setItem(API_KEY, apiKey);
        }
        if (!localStorage.getItem(ACTOR_KEY)) {
          localStorage.setItem(ACTOR_KEY, 'superadmin-ui');
        }
        return { email, password, tenantId };
      };
      async function apiFetch(url, opts = {}) {
        const options = { ...opts };
        options.headers = { ...(opts.headers || {}), ...adminHeaders() };
        const res = await fetch(url, options);
        const isCsv = (res.headers.get('content-type') || '').includes('text/csv');
        const payload = isCsv ? await res.text() : await res.json().catch(() => ({}));
        if (!res.ok) {
          const message = isCsv ? 'CSV export failed' : (payload.detail || payload.error || 'Request failed');
          throw new Error(message);
        }
        return { payload, isCsv };
      }
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
      function downloadCsv(filename, text) {
        const blob = new Blob([text], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        a.click();
        URL.revokeObjectURL(url);
      }
      async function loadInternal() {
        const data = await apiFetch('/api/warmup/admin/internal-mailboxes');
        q('internal-list').textContent = JSON.stringify(data.payload, null, 2);
      }
      async function loadDlqAndAudit() {
        const dlq = await apiFetch('/api/warmup/worker/dlq', { headers: {} });
        q('dlq-data').textContent = JSON.stringify(dlq.payload, null, 2);
        const audit = await apiFetch('/api/warmup/admin/audit-logs?limit=20');
        q('audit-data').textContent = JSON.stringify(audit.payload, null, 2);
      }
      async function loadPayments() {
        const data = await apiFetch('/api/billing/admin/payments/providers');
        q('payment-data').textContent = JSON.stringify(data.payload, null, 2);
      }
      async function loadBilling(endpoint, targetId) {
        const limit = Math.max(1, Number(q('billing-limit').value || '20'));
        const data = await apiFetch(endpoint + (endpoint.includes('?') ? '' : ('?limit=' + encodeURIComponent(limit))));
        q(targetId).textContent = JSON.stringify(data.payload, null, 2);
      }

      q('superadmin-login').onclick = () => withLoading(q('superadmin-login'), async () => {
        const { email, password } = parseAuthForm();
        const res = await fetch('/api/auth/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email, password })
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
          throw new Error(data.detail || data.error || 'Login failed');
        }
        const verify = await fetch('/api/auth/verify-token', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ token: data.access_token })
        });
        const profile = await verify.json().catch(() => ({}));
        if (!verify.ok) {
          throw new Error(profile.detail || 'Token verification failed');
        }
        if (profile.role !== 'superadmin') {
          throw new Error('This dashboard requires a superadmin account.');
        }
        setSignedIn(data.access_token, profile);
        banner('Logged in successfully.');
        await Promise.resolve()
          .then(() => loadInternal())
          .then(() => loadDlqAndAudit())
          .then(() => loadPayments());
      }).catch((error) => banner(error.message || 'Login failed', 'error'));

      q('superadmin-signup').onclick = () => withLoading(q('superadmin-signup'), async () => {
        const { email, password, tenantId } = parseAuthForm();
        if (!validateTenant(tenantId)) {
          throw new Error('tenant_id must be at least 2 characters for signup.');
        }
        const res = await fetch('/api/auth/signup', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email, password, role: 'superadmin', tenant_id: tenantId })
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
          throw new Error(data.detail || data.error || 'Signup failed');
        }
        const verify = await fetch('/api/auth/verify-token', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ token: data.access_token })
        });
        const profile = await verify.json().catch(() => ({}));
        if (!verify.ok) {
          throw new Error(profile.detail || 'Token verification failed');
        }
        setSignedIn(data.access_token, profile);
        banner('Superadmin bootstrap complete.');
        await Promise.resolve()
          .then(() => loadInternal())
          .then(() => loadDlqAndAudit())
          .then(() => loadPayments());
      }).catch((error) => banner(error.message || 'Signup failed', 'error'));

      q('superadmin-logout').onclick = () => {
        setSignedOut();
        banner('Logged out.');
      };

      q('add-internal').onclick = () => withLoading(q('add-internal'), async () => {
        const tenant = q('internal-tenant').value.trim();
        const mailbox = q('internal-mailbox').value.trim().toLowerCase();
        const notes = q('internal-notes').value.trim();
        if (!validateTenant(tenant)) {
          banner('tenant_id must be at least 2 characters.', 'error');
          return;
        }
        if (!validateEmail(mailbox)) {
          banner('Please provide a valid mailbox email.', 'error');
          return;
        }
        await apiFetch('/api/warmup/admin/internal-mailboxes', {
          method: 'POST',
          body: JSON.stringify({ tenant_id: tenant, mailbox, notes })
        });
        banner('Internal mailbox updated successfully.');
        await loadInternal();
      });
      q('fetch-health').onclick = () => withLoading(q('fetch-health'), async () => {
        const tenant = q('health-tenant').value.trim();
        const mailbox = q('health-mailbox').value.trim().toLowerCase();
        const limit = Math.max(1, Math.min(Number(q('health-limit').value || '20'), 100));
        if (!validateTenant(tenant)) {
          banner('tenant_id must be at least 2 characters.', 'error');
          return;
        }
        if (!validateEmail(mailbox)) {
          banner('Please provide a valid mailbox email.', 'error');
          return;
        }
        const url = '/api/warmup/admin/mailbox-health?tenant_id=' + encodeURIComponent(tenant)
          + '&mailbox=' + encodeURIComponent(mailbox)
          + '&limit=' + encodeURIComponent(String(limit));
        const data = await apiFetch(url);
        q('mailbox-health').textContent = JSON.stringify(data.payload, null, 2);
      });
      q('replay-dlq').onclick = () => withLoading(q('replay-dlq'), async () => {
        await apiFetch('/api/warmup/worker/dlq/replay', {
          method: 'POST',
          body: JSON.stringify({
            item_index: Number(q('dlq-index').value || '0'),
            approved_by: q('dlq-approved-by').value || 'superadmin',
            reason: 'manual_replay_from_superadmin'
          })
        });
        banner('DLQ replay requested.');
        await loadDlqAndAudit();
      });
      q('save-payment').onclick = () => withLoading(q('save-payment'), async () => {
        const provider = q('payment-provider').value;
        const key = q('payment-key').value.trim();
        const secret = q('payment-secret').value.trim();
        if (!key) {
          banner('Payment key/merchant id is required.', 'error');
          return;
        }
        const payload = provider === 'stripe' || provider === 'razorpay'
          ? { key_id: key, secret }
          : { merchant_id: key, secret };
        await apiFetch('/api/billing/admin/payments/providers/' + provider + '/setup', {
          method: 'POST',
          body: JSON.stringify(payload)
        });
        banner('Payment provider setup saved.');
        await loadPayments();
      });
      q('billing-refresh-runs').onclick = () => withLoading(q('billing-refresh-runs'), async () => loadBilling('/api/billing/admin/payments/reconciliation/runs', 'billing-data'));
      q('billing-refresh-queue').onclick = () => withLoading(q('billing-refresh-queue'), async () => loadBilling('/api/billing/admin/payments/reconciliation/queue', 'billing-data'));
      q('billing-refresh-dlq').onclick = () => withLoading(q('billing-refresh-dlq'), async () => loadBilling('/api/billing/admin/payments/reconciliation/dlq', 'billing-data'));
      q('billing-refresh-effects').onclick = () => withLoading(q('billing-refresh-effects'), async () => loadBilling('/api/billing/admin/payments/effects', 'billing-data'));
      q('billing-refresh-inbox').onclick = () => withLoading(q('billing-refresh-inbox'), async () => loadBilling('/api/billing/admin/events/inbox', 'billing-data'));
      q('billing-refresh-outbox').onclick = () => withLoading(q('billing-refresh-outbox'), async () => loadBilling('/api/billing/admin/events/outbox', 'billing-data'));
      q('billing-worker-pass').onclick = () => withLoading(q('billing-worker-pass'), async () => {
        const data = await apiFetch('/api/billing/admin/payments/reconciliation/worker/pass', { method: 'POST', body: JSON.stringify({}) });
        q('billing-data').textContent = JSON.stringify(data.payload, null, 2);
        banner('Reconciliation worker pass completed.');
      });
      q('audit-filter').onclick = () => withLoading(q('audit-filter'), async () => {
        const actor = q('audit-actor').value.trim();
        const action = q('audit-action').value.trim();
        const resourceType = q('audit-resource-type').value.trim();
        const since = q('audit-since').value.trim();
        if (since && isNaN(Date.parse(since))) {
          banner('since must be a valid ISO timestamp.', 'error');
          return;
        }
        const query = new URLSearchParams({ limit: '100' });
        if (actor) query.set('actor', actor);
        if (action) query.set('action', action);
        if (resourceType) query.set('resource_type', resourceType);
        if (since) query.set('since', since);
        const warmup = await apiFetch('/api/warmup/admin/audit-logs?' + query.toString());
        const billing = await apiFetch('/api/billing/admin/audit-logs?' + query.toString());
        q('audit-filter-output').textContent = JSON.stringify({ warmup: warmup.payload, billing: billing.payload }, null, 2);
        banner('Audit filters applied.');
      });
      q('audit-export-warmup').onclick = () => withLoading(q('audit-export-warmup'), async () => {
        const data = await apiFetch('/api/warmup/admin/audit-logs/export?limit=500');
        downloadCsv('warmup-audit-logs.csv', data.payload);
      });
      q('audit-export-billing').onclick = () => withLoading(q('audit-export-billing'), async () => {
        const data = await apiFetch('/api/billing/admin/audit-logs/export?limit=500');
        downloadCsv('billing-audit-logs.csv', data.payload);
      });
      q('audit-retention-run').onclick = () => withLoading(q('audit-retention-run'), async () => {
        const days = Math.max(1, Number(q('audit-retention-days').value || '90'));
        const warmup = await apiFetch('/api/warmup/admin/audit-logs/retention?dry_run=false', { method: 'POST' });
        const billing = await apiFetch('/api/billing/admin/audit-logs/retention', { method: 'POST', body: JSON.stringify({ dry_run: false, retention_days: days }) });
        q('audit-filter-output').textContent = JSON.stringify({ warmup: warmup.payload, billing: billing.payload }, null, 2);
        banner('Audit retention completed.');
      });

      fetch('/api/warmup/health')
        .then((r) => r.json())
        .then((data) => { q('global-health').textContent = data.status || 'unknown'; })
        .catch(() => { q('global-health').textContent = 'degraded'; });

      (() => {
        const token = localStorage.getItem(TOKEN_KEY);
        const profile = (() => {
          try {
            return JSON.parse(localStorage.getItem(PROFILE_KEY) || 'null');
          } catch {
            return null;
          }
        })();
        const apiKey = localStorage.getItem(API_KEY);
        if (apiKey) {
          q('superadmin-api-key').value = apiKey;
        }
        if (token) {
          setSignedIn(token, profile || { email: 'superadmin', role: 'superadmin' });
          Promise.resolve()
            .then(() => loadInternal())
            .then(() => loadDlqAndAudit())
            .then(() => loadPayments())
            .catch((error) => banner(error.message || 'Initial data load failed', 'error'));
        } else {
          setSignedOut();
        }
      })();
    </script>
  </body>
</html>`;

fs.writeFileSync(path.join(distDir, 'index.html'), html);
