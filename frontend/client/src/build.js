import fs from 'fs';
import path from 'path';

const distDir = path.resolve('src/dist');
fs.mkdirSync(distDir, { recursive: true });

const html = `<!doctype html>
<html>
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Client Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
  </head>
  <body class="bg-slate-50 text-slate-900">
    <div class="max-w-5xl mx-auto p-6">
      <div class="flex items-center justify-between mb-4">
        <h1 class="text-2xl font-bold">Client Dashboard</h1>
        <div class="flex items-center gap-2 text-sm">
          <span id="client-user" class="text-slate-600"></span>
          <button id="client-logout" class="hidden bg-slate-800 text-white rounded px-3 py-1">Logout</button>
        </div>
      </div>
      <div id="client-banner" class="hidden mb-3 rounded border px-3 py-2 text-sm"></div>

      <div id="client-auth" class="bg-white p-4 rounded shadow mb-6">
        <h2 class="font-semibold mb-2">Login / Signup</h2>
        <p class="text-sm text-slate-600 mb-3">Use login if account exists, otherwise create a client account.</p>
        <div class="grid md:grid-cols-4 gap-2 mb-2">
          <input id="client-auth-email" class="border rounded px-2 py-1 md:col-span-2" placeholder="email" />
          <input id="client-auth-password" type="password" class="border rounded px-2 py-1" placeholder="password" />
          <input id="client-auth-tenant" class="border rounded px-2 py-1" placeholder="tenant_id (for signup)" />
        </div>
        <div class="flex gap-2">
          <button id="client-login" class="bg-indigo-600 text-white rounded px-3 py-1">Login</button>
          <button id="client-signup" class="bg-emerald-600 text-white rounded px-3 py-1">Signup</button>
        </div>
      </div>

      <div id="client-dashboard" class="hidden">
        <div class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
          <div class="bg-white p-4 rounded shadow">Campaigns: 3</div>
          <div class="bg-white p-4 rounded shadow">Leads: 1,240</div>
          <div class="bg-white p-4 rounded shadow">Warm-up Jobs: 2</div>
          <div class="bg-white p-4 rounded shadow">CRM Open Items: 18</div>
        </div>
        <div class="bg-white p-4 rounded shadow">
          <h2 class="font-semibold mb-2">Quick Actions</h2>
          <div class="grid md:grid-cols-4 gap-2 mb-3">
            <input id="client-email" class="border rounded px-2 py-1 md:col-span-2" placeholder="lead email" />
            <input id="client-company" class="border rounded px-2 py-1" placeholder="company (optional)" />
            <button id="client-add-lead" class="bg-indigo-600 text-white rounded px-3 py-1">Upload Lead</button>
          </div>
          <div class="grid md:grid-cols-4 gap-2 mb-3">
            <input id="client-tenant" class="border rounded px-2 py-1" placeholder="tenant_id" />
            <input id="client-mailbox" class="border rounded px-2 py-1" placeholder="mailbox" />
            <input id="client-domain-age" class="border rounded px-2 py-1" value="45" />
            <button id="client-start-warmup" class="bg-emerald-600 text-white rounded px-3 py-1">Start Warm-up</button>
          </div>
          <pre id="client-output" class="bg-gray-50 border rounded p-2 text-xs overflow-auto max-h-72"></pre>
        </div>
      </div>
    </div>
    <script>
      const q = (id) => document.getElementById(id);
      const TOKEN_KEY = 'client_auth_token';
      const PROFILE_KEY = 'client_profile';
      const banner = (text, level = 'info') => {
        const el = q('client-banner');
        el.classList.remove('hidden', 'border-red-200', 'bg-red-50', 'text-red-700', 'border-emerald-200', 'bg-emerald-50', 'text-emerald-700');
        if (level === 'error') {
          el.classList.add('border-red-200', 'bg-red-50', 'text-red-700');
        } else {
          el.classList.add('border-emerald-200', 'bg-emerald-50', 'text-emerald-700');
        }
        el.textContent = text;
      };
      const validateEmail = (email) => /^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$/.test(email || '');
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
      const authHeaders = () => {
        const token = localStorage.getItem(TOKEN_KEY);
        return {
          'Content-Type': 'application/json',
          'Authorization': token ? ('Bearer ' + token) : '',
        };
      };
      const setSignedOut = () => {
        localStorage.removeItem(TOKEN_KEY);
        localStorage.removeItem(PROFILE_KEY);
        q('client-auth').classList.remove('hidden');
        q('client-dashboard').classList.add('hidden');
        q('client-logout').classList.add('hidden');
        q('client-user').textContent = '';
      };
      const setSignedIn = (token, profile) => {
        localStorage.setItem(TOKEN_KEY, token);
        localStorage.setItem(PROFILE_KEY, JSON.stringify(profile));
        q('client-auth').classList.add('hidden');
        q('client-dashboard').classList.remove('hidden');
        q('client-logout').classList.remove('hidden');
        q('client-user').textContent = profile && profile.email ? (profile.email + ' (' + (profile.role || 'client') + ')') : 'Logged in';
        if (profile && profile.tenant_id) {
          q('client-tenant').value = profile.tenant_id;
        }
      };
      const parseAuthForm = () => {
        const email = q('client-auth-email').value.trim().toLowerCase();
        const password = q('client-auth-password').value;
        const tenantId = q('client-auth-tenant').value.trim();
        if (!validateEmail(email)) {
          throw new Error('Please provide a valid email.');
        }
        if (!password || password.length < 8) {
          throw new Error('Password must be at least 8 characters.');
        }
        return { email, password, tenantId };
      };

      q('client-login').onclick = async () => {
        try {
          await withLoading(q('client-login'), async () => {
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
            setSignedIn(data.access_token, profile);
            banner('Logged in successfully.');
          });
        } catch (error) {
          banner(error.message || 'Login failed', 'error');
        }
      };

      q('client-signup').onclick = async () => {
        try {
          await withLoading(q('client-signup'), async () => {
            const { email, password, tenantId } = parseAuthForm();
            if (!tenantId || tenantId.length < 2) {
              throw new Error('tenant_id must be at least 2 characters for signup.');
            }
            const res = await fetch('/api/auth/signup', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ email, password, role: 'client', tenant_id: tenantId })
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
            banner('Signup complete. You are now logged in.');
          });
        } catch (error) {
          banner(error.message || 'Signup failed', 'error');
        }
      };

      q('client-logout').onclick = () => {
        setSignedOut();
        banner('Logged out.');
      };

      q('client-add-lead').onclick = () => withLoading(q('client-add-lead'), async () => {
        const email = q('client-email').value.trim().toLowerCase();
        const company = q('client-company').value.trim();
        if (!validateEmail(email)) {
          banner('Please provide a valid lead email address.', 'error');
          return;
        }
        try {
          const res = await fetch('/api/leads/leads', {
            method: 'POST',
            headers: authHeaders(),
            body: JSON.stringify({ email, company: company || null })
          });
          const data = await res.json();
          if (!res.ok) {
            throw new Error(data.detail || data.error || 'Unable to create lead');
          }
          banner('Lead uploaded successfully.');
          q('client-output').textContent = JSON.stringify(data, null, 2);
        } catch (error) {
          banner(error.message || 'Lead upload failed', 'error');
        }
      });
      q('client-start-warmup').onclick = () => withLoading(q('client-start-warmup'), async () => {
        const tenantId = q('client-tenant').value.trim();
        const mailbox = q('client-mailbox').value.trim().toLowerCase();
        const domainAgeDays = Number(q('client-domain-age').value || '0');
        if (!tenantId || tenantId.length < 2) {
          banner('tenant_id must be at least 2 characters.', 'error');
          return;
        }
        if (!validateEmail(mailbox)) {
          banner('Please provide a valid mailbox email.', 'error');
          return;
        }
        if (!Number.isFinite(domainAgeDays) || domainAgeDays < 0) {
          banner('Domain age must be a non-negative number.', 'error');
          return;
        }
        try {
          const res = await fetch('/api/warmup/warmup/jobs', {
            method: 'POST',
            headers: authHeaders(),
            body: JSON.stringify({
              tenant_id: tenantId,
              mailbox,
              domain_age_days: domainAgeDays,
              blacklist_detected: false
            })
          });
          const data = await res.json();
          if (!res.ok) {
            throw new Error(data.detail || data.error || 'Unable to start warmup');
          }
          banner('Warmup started successfully.');
          q('client-output').textContent = JSON.stringify(data, null, 2);
        } catch (error) {
          banner(error.message || 'Warmup start failed', 'error');
        }
      });

      (() => {
        const token = localStorage.getItem(TOKEN_KEY);
        const profile = (() => {
          try {
            return JSON.parse(localStorage.getItem(PROFILE_KEY) || 'null');
          } catch {
            return null;
          }
        })();
        if (token) {
          setSignedIn(token, profile || { email: 'authenticated-user', role: 'client' });
        } else {
          setSignedOut();
        }
      })();
    </script>
  </body>
</html>`;

fs.writeFileSync(path.join(distDir, 'index.html'), html);
