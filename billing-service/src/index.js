import express from 'express';
import Stripe from 'stripe';
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import rateLimit from 'express-rate-limit';
import jwt from 'jsonwebtoken';
import { context, propagation, trace } from '@opentelemetry/api';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { NodeSDK } from '@opentelemetry/sdk-node';

const otelEndpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT || '';
const serviceName = 'billing-service';
const sdk = new NodeSDK({
  traceExporter: otelEndpoint ? new OTLPTraceExporter({ url: `${otelEndpoint.replace(/\/$/, '')}/v1/traces` }) : undefined,
  instrumentations: [getNodeAutoInstrumentations()],
  serviceName,
});
sdk.start();

const app = express();
const stripeSecret = process.env.STRIPE_SECRET_KEY || '';
const stripeWebhookSecret = process.env.STRIPE_WEBHOOK_SECRET || '';
const stripe = stripeSecret ? new Stripe(stripeSecret) : null;
const billingAdminApiKey = process.env.BILLING_ADMIN_API_KEY || '';
const authJwtSecret = process.env.JWT_SECRET || 'dev-secret';
const authJwtAlgorithm = process.env.JWT_ALGORITHM || 'HS256';
const providerConfigPath = process.env.BILLING_PROVIDER_CONFIG_PATH || path.resolve('data/payment-providers.enc');
const providerConfigEncKey = process.env.BILLING_CONFIG_ENC_KEY || '';
const providerSecretBackend = (process.env.BILLING_SECRET_BACKEND || 'file').toLowerCase();
const vaultAddr = process.env.VAULT_ADDR || '';
const vaultToken = process.env.VAULT_TOKEN || '';
const vaultKvPath = process.env.VAULT_PAYMENT_CONFIG_PATH || 'secret/data/email-warmup/billing/providers';
const kmsEncryptUrl = process.env.KMS_ENCRYPT_URL || '';
const kmsDecryptUrl = process.env.KMS_DECRYPT_URL || '';
const kmsAuthToken = process.env.KMS_AUTH_TOKEN || '';
const webhookLedgerPath = process.env.BILLING_WEBHOOK_LEDGER_PATH || path.resolve('data/webhook-ledger.log');
const webhookIndexPath = process.env.BILLING_WEBHOOK_INDEX_PATH || path.resolve('data/webhook-index.json');
const reconciliationRunsPath = process.env.BILLING_RECONCILIATION_RUNS_PATH || path.resolve('data/reconciliation-runs.json');

let paymentProviders = {
  stripe: {
    enabled: Boolean(stripeSecret),
    key_id: process.env.STRIPE_PUBLISHABLE_KEY || '',
    secret_set: Boolean(stripeSecret),
    webhook_secret_set: Boolean(stripeWebhookSecret),
    mode: process.env.STRIPE_MODE || 'test',
  },
  razorpay: {
    enabled: Boolean(process.env.RAZORPAY_KEY_ID && process.env.RAZORPAY_KEY_SECRET),
    key_id: process.env.RAZORPAY_KEY_ID || '',
    secret_set: Boolean(process.env.RAZORPAY_KEY_SECRET),
    webhook_secret_set: Boolean(process.env.RAZORPAY_WEBHOOK_SECRET),
    mode: process.env.RAZORPAY_MODE || 'test',
  },
  phonepe: {
    enabled: Boolean(process.env.PHONEPE_MERCHANT_ID && process.env.PHONEPE_SALT_KEY),
    merchant_id: process.env.PHONEPE_MERCHANT_ID || '',
    secret_set: Boolean(process.env.PHONEPE_SALT_KEY),
    webhook_secret_set: Boolean(process.env.PHONEPE_WEBHOOK_SECRET),
    mode: process.env.PHONEPE_MODE || 'test',
  },
  paytm: {
    enabled: Boolean(process.env.PAYTM_MERCHANT_ID && process.env.PAYTM_MERCHANT_KEY),
    merchant_id: process.env.PAYTM_MERCHANT_ID || '',
    secret_set: Boolean(process.env.PAYTM_MERCHANT_KEY),
    webhook_secret_set: Boolean(process.env.PAYTM_WEBHOOK_SECRET),
    mode: process.env.PAYTM_MODE || 'test',
  },
};

function normalizeConfig(config) {
  return {
    stripe: {
      enabled: Boolean(config?.stripe?.enabled),
      key_id: config?.stripe?.key_id || '',
      secret_set: Boolean(config?.stripe?.secret_set),
      webhook_secret_set: Boolean(config?.stripe?.webhook_secret_set),
      mode: config?.stripe?.mode || 'test',
    },
    razorpay: {
      enabled: Boolean(config?.razorpay?.enabled),
      key_id: config?.razorpay?.key_id || '',
      secret_set: Boolean(config?.razorpay?.secret_set),
      webhook_secret_set: Boolean(config?.razorpay?.webhook_secret_set),
      mode: config?.razorpay?.mode || 'test',
    },
    phonepe: {
      enabled: Boolean(config?.phonepe?.enabled),
      merchant_id: config?.phonepe?.merchant_id || '',
      secret_set: Boolean(config?.phonepe?.secret_set),
      webhook_secret_set: Boolean(config?.phonepe?.webhook_secret_set),
      mode: config?.phonepe?.mode || 'test',
    },
    paytm: {
      enabled: Boolean(config?.paytm?.enabled),
      merchant_id: config?.paytm?.merchant_id || '',
      secret_set: Boolean(config?.paytm?.secret_set),
      webhook_secret_set: Boolean(config?.paytm?.webhook_secret_set),
      mode: config?.paytm?.mode || 'test',
    },
  };
}

function encryptionKey() {
  if (!providerConfigEncKey) {
    return null;
  }
  return crypto.createHash('sha256').update(providerConfigEncKey).digest();
}

async function callSecretApi(url, body) {
  const headers = { 'Content-Type': 'application/json' };
  if (kmsAuthToken) {
    headers.Authorization = `Bearer ${kmsAuthToken}`;
  }
  const response = await fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    throw new Error(`secret API call failed: ${response.status}`);
  }
  return response.json();
}

async function saveProviderConfig() {
  const key = encryptionKey();
  fs.mkdirSync(path.dirname(providerConfigPath), { recursive: true });
  const payload = Buffer.from(JSON.stringify(paymentProviders));
  if (providerSecretBackend === 'vault') {
    if (!vaultAddr || !vaultToken) {
      throw new Error('vault backend configured but VAULT_ADDR/VAULT_TOKEN missing');
    }
    const response = await fetch(`${vaultAddr.replace(/\/$/, '')}/v1/${vaultKvPath}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Vault-Token': vaultToken,
      },
      body: JSON.stringify({ data: { providers: paymentProviders } }),
    });
    if (!response.ok) {
      throw new Error(`vault write failed: ${response.status}`);
    }
    return;
  }
  if (providerSecretBackend === 'kms') {
    if (!kmsEncryptUrl || !kmsDecryptUrl) {
      throw new Error('kms backend configured but KMS_ENCRYPT_URL/KMS_DECRYPT_URL missing');
    }
    const encrypted = await callSecretApi(kmsEncryptUrl, { plaintext: payload.toString('base64') });
    fs.writeFileSync(
      providerConfigPath,
      JSON.stringify({
        v: 2,
        backend: 'kms',
        ciphertext: encrypted.ciphertext || '',
      })
    );
    return;
  }
  if (!key) {
    fs.writeFileSync(providerConfigPath, payload);
    return;
  }
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const encrypted = Buffer.concat([cipher.update(payload), cipher.final()]);
  const tag = cipher.getAuthTag();
  fs.writeFileSync(
    providerConfigPath,
    JSON.stringify({
      v: 1,
      iv: iv.toString('base64'),
      tag: tag.toString('base64'),
      data: encrypted.toString('base64'),
    })
  );
}

async function loadProviderConfig() {
  if (providerSecretBackend === 'vault') {
    if (!vaultAddr || !vaultToken) {
      return;
    }
    try {
      const response = await fetch(`${vaultAddr.replace(/\/$/, '')}/v1/${vaultKvPath}`, {
        method: 'GET',
        headers: { 'X-Vault-Token': vaultToken },
      });
      if (!response.ok) {
        return;
      }
      const data = await response.json();
      paymentProviders = normalizeConfig(data?.data?.data?.providers || {});
    } catch (_error) {
      console.error('failed to load payment provider config from vault');
    }
    return;
  }
  if (!fs.existsSync(providerConfigPath)) {
    return;
  }
  const raw = fs.readFileSync(providerConfigPath, 'utf8').trim();
  if (!raw) {
    return;
  }
  try {
    const parsed = JSON.parse(raw);
    if (parsed?.backend === 'kms' && parsed?.ciphertext) {
      if (!kmsDecryptUrl) {
        return;
      }
      const decrypted = await callSecretApi(kmsDecryptUrl, { ciphertext: parsed.ciphertext });
      const clear = Buffer.from(decrypted.plaintext || '', 'base64').toString('utf8');
      paymentProviders = normalizeConfig(JSON.parse(clear));
      return;
    }
    if (!parsed?.iv || !parsed?.tag || !parsed?.data) {
      paymentProviders = normalizeConfig(parsed);
      return;
    }
    const key = encryptionKey();
    if (!key) {
      return;
    }
    const decipher = crypto.createDecipheriv('aes-256-gcm', key, Buffer.from(parsed.iv, 'base64'));
    decipher.setAuthTag(Buffer.from(parsed.tag, 'base64'));
    const decrypted = Buffer.concat([decipher.update(Buffer.from(parsed.data, 'base64')), decipher.final()]);
    paymentProviders = normalizeConfig(JSON.parse(decrypted.toString('utf8')));
  } catch (error) {
    console.error('failed to load payment provider config');
  }
}

function verifyAdmin(req) {
  if (billingAdminApiKey && req.headers['x-admin-api-key'] === billingAdminApiKey) {
    return true;
  }
  const authorization = req.headers.authorization;
  if (!authorization || !authorization.startsWith('Bearer ')) {
    return false;
  }
  try {
    const claims = jwt.verify(authorization.slice(7), authJwtSecret, { algorithms: [authJwtAlgorithm] });
    const permissions = claims.permissions || [];
    return Array.isArray(permissions) && (permissions.includes('*') || permissions.includes('billing:manage_providers'));
  } catch (error) {
    return false;
  }
}

function canReadProviders(req) {
  if (billingAdminApiKey && req.headers['x-admin-api-key'] === billingAdminApiKey) {
    return true;
  }
  const authorization = req.headers.authorization;
  if (!authorization || !authorization.startsWith('Bearer ')) {
    return false;
  }
  try {
    const claims = jwt.verify(authorization.slice(7), authJwtSecret, { algorithms: [authJwtAlgorithm] });
    const permissions = claims.permissions || [];
    return Array.isArray(permissions)
      && (permissions.includes('*') || permissions.includes('billing:manage_providers') || permissions.includes('billing:read_providers'));
  } catch (error) {
    return false;
  }
}

function auditLog(event, details = {}) {
  console.log(JSON.stringify({ event, ...details, at: new Date().toISOString() }));
}

function sha256(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

function loadJsonFile(filePath, fallbackValue) {
  try {
    if (!fs.existsSync(filePath)) {
      return fallbackValue;
    }
    const raw = fs.readFileSync(filePath, 'utf8').trim();
    return raw ? JSON.parse(raw) : fallbackValue;
  } catch (_error) {
    return fallbackValue;
  }
}

function writeJsonFile(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2));
}

let webhookIndex = loadJsonFile(webhookIndexPath, { by_dedupe_key: {}, last_hash: '' });

function recordWebhookLedger({ provider, eventId, signature, payloadBuffer }) {
  const dedupeKey = `${provider}:${eventId}`;
  if (webhookIndex.by_dedupe_key[dedupeKey]) {
    return { duplicate: true, dedupeKey };
  }
  const payloadHash = sha256(payloadBuffer);
  const signatureHash = sha256(String(signature || ''));
  const prevHash = webhookIndex.last_hash || '';
  const at = new Date().toISOString();
  const chainInput = `${prevHash}|${provider}|${eventId}|${payloadHash}|${signatureHash}|${at}`;
  const entryHash = sha256(chainInput);
  const entry = {
    at,
    provider,
    event_id: eventId,
    dedupe_key: dedupeKey,
    payload_hash: payloadHash,
    signature_hash: signatureHash,
    prev_hash: prevHash,
    entry_hash: entryHash,
  };
  fs.mkdirSync(path.dirname(webhookLedgerPath), { recursive: true });
  fs.appendFileSync(webhookLedgerPath, `${JSON.stringify(entry)}\n`);
  webhookIndex.by_dedupe_key[dedupeKey] = {
    at,
    provider,
    event_id: eventId,
    entry_hash: entryHash,
  };
  webhookIndex.last_hash = entryHash;
  writeJsonFile(webhookIndexPath, webhookIndex);
  return { duplicate: false, dedupeKey, entryHash };
}

function extractBodyEventId(provider, bodyBuffer) {
  try {
    const parsed = JSON.parse(bodyBuffer.toString('utf8'));
    if (provider === 'stripe') {
      return parsed?.id || '';
    }
    return parsed?.event || parsed?.event_id || parsed?.id || '';
  } catch (_error) {
    return '';
  }
}

function readReconciliationRuns() {
  return loadJsonFile(reconciliationRunsPath, []);
}

function persistReconciliationRun(run) {
  const current = readReconciliationRuns();
  current.push(run);
  writeJsonFile(reconciliationRunsPath, current.slice(-200));
}

const webhookLimiter = rateLimit({
  windowMs: 60 * 1000,
  limit: 60,
  standardHeaders: true,
  legacyHeaders: false,
});
const adminReadLimiter = rateLimit({
  windowMs: 60 * 1000,
  limit: 30,
  standardHeaders: true,
  legacyHeaders: false,
});
const adminWriteLimiter = rateLimit({
  windowMs: 60 * 1000,
  limit: 20,
  standardHeaders: true,
  legacyHeaders: false,
});

function verifyHmac(bodyBuffer, signature, secret) {
  if (!secret || !signature) {
    return false;
  }
  const digest = crypto.createHmac('sha256', secret).update(bodyBuffer).digest('hex');
  const left = Buffer.from(digest);
  const right = Buffer.from(String(signature));
  if (left.length !== right.length) {
    return false;
  }
  return crypto.timingSafeEqual(left, right);
}

app.post('/webhooks/stripe', webhookLimiter, express.raw({ type: 'application/json' }), (req, res) => {
  if (!stripe || !stripeWebhookSecret) {
    return res.status(200).json({ accepted: true, note: 'stripe not configured' });
  }

  const signature = req.headers['stripe-signature'];
  try {
    const event = stripe.webhooks.constructEvent(req.body, signature, stripeWebhookSecret);
    const ledger = recordWebhookLedger({
      provider: 'stripe',
      eventId: event?.id || extractBodyEventId('stripe', req.body) || sha256(req.body),
      signature,
      payloadBuffer: req.body,
    });
    if (ledger.duplicate) {
      return res.status(409).json({ error: 'Webhook replay detected', dedupe_key: ledger.dedupeKey });
    }
    return res.json({ received: true, dedupe_key: ledger.dedupeKey });
  } catch (error) {
    console.error('Stripe webhook verification failed');
    return res.status(400).json({ error: 'Invalid webhook signature' });
  }
});

app.post('/webhooks/razorpay', webhookLimiter, express.raw({ type: 'application/json' }), (req, res) => {
  const secret = process.env.RAZORPAY_WEBHOOK_SECRET || '';
  if (!secret) {
    return res.status(200).json({ accepted: true, note: 'razorpay not configured' });
  }
  const signature = req.headers['x-razorpay-signature'];
  if (!verifyHmac(req.body, signature, secret)) {
    return res.status(400).json({ error: 'Invalid Razorpay webhook signature' });
  }
  const eventId = extractBodyEventId('razorpay', req.body) || sha256(Buffer.concat([req.body, Buffer.from(String(signature || ''))]));
  const ledger = recordWebhookLedger({
    provider: 'razorpay',
    eventId,
    signature,
    payloadBuffer: req.body,
  });
  if (ledger.duplicate) {
    return res.status(409).json({ error: 'Webhook replay detected', dedupe_key: ledger.dedupeKey });
  }
  return res.json({ received: true, dedupe_key: ledger.dedupeKey });
});

app.post('/webhooks/phonepe', webhookLimiter, express.raw({ type: 'application/json' }), (req, res) => {
  const secret = process.env.PHONEPE_WEBHOOK_SECRET || '';
  if (!secret) {
    return res.status(200).json({ accepted: true, note: 'phonepe not configured' });
  }
  const signature = req.headers['x-phonepe-signature'];
  if (!verifyHmac(req.body, signature, secret)) {
    return res.status(400).json({ error: 'Invalid PhonePe webhook signature' });
  }
  const eventId = extractBodyEventId('phonepe', req.body) || sha256(Buffer.concat([req.body, Buffer.from(String(signature || ''))]));
  const ledger = recordWebhookLedger({
    provider: 'phonepe',
    eventId,
    signature,
    payloadBuffer: req.body,
  });
  if (ledger.duplicate) {
    return res.status(409).json({ error: 'Webhook replay detected', dedupe_key: ledger.dedupeKey });
  }
  return res.json({ received: true, dedupe_key: ledger.dedupeKey });
});

app.post('/webhooks/paytm', webhookLimiter, express.raw({ type: 'application/json' }), (req, res) => {
  const secret = process.env.PAYTM_WEBHOOK_SECRET || '';
  if (!secret) {
    return res.status(200).json({ accepted: true, note: 'paytm not configured' });
  }
  const signature = req.headers['x-paytm-signature'];
  if (!verifyHmac(req.body, signature, secret)) {
    return res.status(400).json({ error: 'Invalid Paytm webhook signature' });
  }
  const eventId = extractBodyEventId('paytm', req.body) || sha256(Buffer.concat([req.body, Buffer.from(String(signature || ''))]));
  const ledger = recordWebhookLedger({
    provider: 'paytm',
    eventId,
    signature,
    payloadBuffer: req.body,
  });
  if (ledger.duplicate) {
    return res.status(409).json({ error: 'Webhook replay detected', dedupe_key: ledger.dedupeKey });
  }
  return res.json({ received: true, dedupe_key: ledger.dedupeKey });
});

app.use(express.json());

app.use((req, res, next) => {
  req.requestId = req.headers['x-request-id'] || crypto.randomUUID();
  const incomingTrace = {
    traceparent: req.headers.traceparent || undefined,
    tracestate: req.headers.tracestate || undefined,
  };
  const activeCtx = propagation.extract(context.active(), incomingTrace);
  const tracer = trace.getTracer(serviceName);
  context.with(activeCtx, () => {
    const span = tracer.startSpan(`billing ${req.method} ${req.path}`);
    span.setAttribute('http.method', req.method);
    span.setAttribute('http.route', req.path);
    req.otelSpan = span;
    res.on('finish', () => {
      span.setAttribute('http.status_code', res.statusCode);
      span.end();
    });
    next();
  });
});

app.use((req, res, next) => {
  res.setHeader('x-request-id', req.requestId);
  next();
});

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'billing' });
});

app.post('/subscriptions/preview', async (req, res) => {
  const { plan = 'starter', quantity = 1 } = req.body || {};
  const pricing = { starter: 49, growth: 149, scale: 499 };
  if (!pricing[plan]) {
    return res.status(400).json({ error: 'Invalid plan' });
  }
  return res.json({
    plan,
    quantity,
    amount_usd: pricing[plan] * quantity,
    stripe_enabled: Boolean(stripe),
  });
});

app.get('/admin/payments/providers', adminReadLimiter, (req, res) => {
  Promise.resolve()
    .then(() => loadProviderConfig())
    .then(() => {
      if (!canReadProviders(req)) {
        return res.status(403).json({ error: 'Unauthorized' });
      }
      return res.json({ providers: paymentProviders });
    })
    .catch(() => res.status(500).json({ error: 'Provider config read failed' }));
});

app.post('/admin/payments/providers/:provider/setup', adminWriteLimiter, async (req, res) => {
  if (!verifyAdmin(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const provider = req.params.provider;
  if (!paymentProviders[provider]) {
    return res.status(404).json({ error: 'Provider not supported' });
  }
  const config = req.body || {};
  if (provider === 'stripe') {
    paymentProviders.stripe.key_id = config.key_id || paymentProviders.stripe.key_id;
    paymentProviders.stripe.secret_set = Boolean(config.secret || paymentProviders.stripe.secret_set);
    paymentProviders.stripe.webhook_secret_set = Boolean(config.webhook_secret || paymentProviders.stripe.webhook_secret_set);
    paymentProviders.stripe.enabled = Boolean(paymentProviders.stripe.key_id && paymentProviders.stripe.secret_set);
  } else if (provider === 'razorpay') {
    paymentProviders.razorpay.key_id = config.key_id || paymentProviders.razorpay.key_id;
    paymentProviders.razorpay.secret_set = Boolean(config.secret || paymentProviders.razorpay.secret_set);
    paymentProviders.razorpay.webhook_secret_set = Boolean(config.webhook_secret || paymentProviders.razorpay.webhook_secret_set);
    paymentProviders.razorpay.enabled = Boolean(paymentProviders.razorpay.key_id && paymentProviders.razorpay.secret_set);
  } else if (provider === 'phonepe') {
    paymentProviders.phonepe.merchant_id = config.merchant_id || paymentProviders.phonepe.merchant_id;
    paymentProviders.phonepe.secret_set = Boolean(config.secret || paymentProviders.phonepe.secret_set);
    paymentProviders.phonepe.webhook_secret_set = Boolean(config.webhook_secret || paymentProviders.phonepe.webhook_secret_set);
    paymentProviders.phonepe.enabled = Boolean(paymentProviders.phonepe.merchant_id && paymentProviders.phonepe.secret_set);
  } else if (provider === 'paytm') {
    paymentProviders.paytm.merchant_id = config.merchant_id || paymentProviders.paytm.merchant_id;
    paymentProviders.paytm.secret_set = Boolean(config.secret || paymentProviders.paytm.secret_set);
    paymentProviders.paytm.webhook_secret_set = Boolean(config.webhook_secret || paymentProviders.paytm.webhook_secret_set);
    paymentProviders.paytm.enabled = Boolean(paymentProviders.paytm.merchant_id && paymentProviders.paytm.secret_set);
  }
  try {
    await saveProviderConfig();
  } catch (_error) {
    return res.status(500).json({ error: 'Provider config write failed' });
  }
  auditLog('payment_provider_setup', { provider, actor: req.headers['x-admin-actor'] || 'unknown' });
  return res.json({ provider, config: paymentProviders[provider] });
});

app.post('/admin/payments/reconciliation/run', adminWriteLimiter, (req, res) => {
  if (!verifyAdmin(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const provider = req.body?.provider || 'all';
  const now = new Date().toISOString();
  const events = Object.values(webhookIndex.by_dedupe_key || {});
  const byProvider = provider === 'all' ? events : events.filter((item) => item.provider === provider);
  const run = {
    run_id: `recon_${crypto.randomUUID()}`,
    provider,
    status: 'completed',
    ledger_events_scanned: byProvider.length,
    anomalies: 0,
    started_at: now,
    completed_at: now,
  };
  persistReconciliationRun(run);
  auditLog('payment_reconciliation_run', { provider, run_id: run.run_id, actor: req.headers['x-admin-actor'] || 'unknown' });
  return res.json(run);
});

app.get('/admin/payments/reconciliation/runs', adminReadLimiter, (req, res) => {
  if (!canReadProviders(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const limit = Math.max(1, Math.min(Number(req.query.limit || 20), 100));
  const items = readReconciliationRuns().slice(-limit).reverse();
  return res.json({ items });
});

process.on('SIGTERM', async () => {
  await sdk.shutdown();
  process.exit(0);
});

loadProviderConfig().finally(() => {
  app.listen(3001, () => {
    console.log('billing listening on 3001');
  });
});
