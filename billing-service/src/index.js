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
const ordersStatePath = process.env.BILLING_ORDERS_STATE_PATH || path.resolve('data/orders.json');
const subscriptionsStatePath = process.env.BILLING_SUBSCRIPTIONS_STATE_PATH || path.resolve('data/subscriptions.json');
const refundsStatePath = process.env.BILLING_REFUNDS_STATE_PATH || path.resolve('data/refunds.json');
const disputesStatePath = process.env.BILLING_DISPUTES_STATE_PATH || path.resolve('data/disputes.json');
const effectStatePath = process.env.BILLING_EFFECT_STATE_PATH || path.resolve('data/effects.json');
const eventInboxPath = process.env.BILLING_INBOX_STATE_PATH || path.resolve('data/inbox.json');
const eventOutboxPath = process.env.BILLING_OUTBOX_STATE_PATH || path.resolve('data/outbox.json');
const reconciliationQueuePath = process.env.BILLING_RECON_QUEUE_PATH || path.resolve('data/reconciliation-queue.json');
const reconciliationDlqPath = process.env.BILLING_RECON_DLQ_PATH || path.resolve('data/reconciliation-dlq.json');
const webhookNonceIndexPath = process.env.BILLING_WEBHOOK_NONCE_INDEX_PATH || path.resolve('data/webhook-nonce-index.json');
const auditLogPath = process.env.BILLING_AUDIT_LOG_PATH || path.resolve('data/audit-logs.json');
const auditRetentionDays = Number(process.env.BILLING_AUDIT_RETENTION_DAYS || 90);
const webhookTimestampToleranceSeconds = Number(process.env.BILLING_WEBHOOK_TIMESTAMP_TOLERANCE_SECONDS || 300);
const webhookNonceTtlSeconds = Number(process.env.BILLING_WEBHOOK_NONCE_TTL_SECONDS || 900);
const reconciliationMaxAttempts = Number(process.env.BILLING_RECONCILIATION_MAX_ATTEMPTS || 5);
const reconciliationWorkerIntervalMs = Number(process.env.BILLING_RECONCILIATION_WORKER_INTERVAL_MS || 0);
const reconciliationMaxBackoffMs = Number(process.env.BILLING_RECONCILIATION_MAX_BACKOFF_MS || 60000);
const externalEffectRetention = Number(process.env.BILLING_EXTERNAL_EFFECT_RETENTION || 2000);

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
  const entry = {
    event,
    actor: details.actor || 'system',
    resource_type: details.resource_type || 'billing',
    resource_id: details.resource_id || '',
    details,
    at: new Date().toISOString(),
  };
  auditLogsState.items.push(entry);
  auditLogsState.items = auditLogsState.items.slice(-5000);
  persistAuditLogsState();
  console.log(JSON.stringify(entry));
}

function buildAuditContext(req) {
  return {
    actor: req.headers['x-admin-actor'] || 'unknown',
    correlation_id: req.correlationId || req.requestId || null,
    request_id: req.requestId || null,
  };
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
  if (readSafe(webhookIndex.by_dedupe_key, dedupeKey)) {
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
  writeSafe(webhookIndex.by_dedupe_key, dedupeKey, {
    at,
    provider,
    event_id: eventId,
    entry_hash: entryHash,
  });
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

let ordersState = loadJsonFile(ordersStatePath, { by_id: {}, by_idempotency: {} });
let subscriptionsState = loadJsonFile(subscriptionsStatePath, { by_tenant: {} });
let refundsState = loadJsonFile(refundsStatePath, { by_id: {}, by_order: {} });
let disputesState = loadJsonFile(disputesStatePath, { by_id: {}, by_order: {} });
let externalEffects = loadJsonFile(effectStatePath, { by_key: {}, order: [] });
let eventInbox = loadJsonFile(eventInboxPath, { by_key: {} });
let eventOutbox = loadJsonFile(eventOutboxPath, { items: [] });
let reconciliationQueue = loadJsonFile(reconciliationQueuePath, { items: [] });
let reconciliationDlq = loadJsonFile(reconciliationDlqPath, { items: [] });
let webhookNonceIndex = loadJsonFile(webhookNonceIndexPath, { by_provider: {} });
let auditLogsState = loadJsonFile(auditLogPath, { items: [] });

function persistOrdersState() {
  writeJsonFile(ordersStatePath, ordersState);
}

function persistSubscriptionsState() {
  writeJsonFile(subscriptionsStatePath, subscriptionsState);
}

function persistRefundsState() {
  writeJsonFile(refundsStatePath, refundsState);
}

function persistDisputesState() {
  writeJsonFile(disputesStatePath, disputesState);
}

function persistEffectsState() {
  writeJsonFile(effectStatePath, externalEffects);
}

function persistInboxState() {
  writeJsonFile(eventInboxPath, eventInbox);
}

function persistOutboxState() {
  writeJsonFile(eventOutboxPath, eventOutbox);
}

function persistReconciliationQueueState() {
  writeJsonFile(reconciliationQueuePath, reconciliationQueue);
}

function persistReconciliationDlqState() {
  writeJsonFile(reconciliationDlqPath, reconciliationDlq);
}

function persistWebhookNonceState() {
  writeJsonFile(webhookNonceIndexPath, webhookNonceIndex);
}

function persistAuditLogsState() {
  writeJsonFile(auditLogPath, auditLogsState);
}

function nowIso() {
  return new Date().toISOString();
}

function orderRef(prefix) {
  return `${prefix}_${crypto.randomUUID().replace(/-/g, '').slice(0, 16)}`;
}

function normalizeProvider(provider) {
  const value = String(provider || '').toLowerCase();
  return value || 'manual';
}

function isUnsafeObjectKey(value) {
  const key = String(value || '');
  return key === '__proto__' || key === 'prototype' || key === 'constructor';
}

function readSafe(container, key) {
  if (!container || isUnsafeObjectKey(key)) {
    return undefined;
  }
  return container[key];
}

function writeSafe(container, key, value) {
  if (!container || isUnsafeObjectKey(key)) {
    return false;
  }
  container[key] = value;
  return true;
}

function purgeExpiredNonces(provider, nowEpoch) {
  const byProvider = webhookNonceIndex.by_provider || {};
  const nonceMap = readSafe(byProvider, provider) || {};
  for (const [nonce, expiresAt] of Object.entries(nonceMap)) {
    if (Number(expiresAt) <= nowEpoch) {
      delete nonceMap[nonce];
    }
  }
  writeSafe(byProvider, provider, nonceMap);
  webhookNonceIndex.by_provider = byProvider;
}

function validateWebhookEnvelope(provider, req) {
  const timestampHeader = req.headers['x-webhook-timestamp'];
  const nonce = req.headers['x-webhook-nonce'];
  if (!timestampHeader || !nonce) {
    return { ok: false, status: 400, reason: 'Missing webhook nonce/timestamp headers' };
  }
  const timestamp = Number(timestampHeader);
  if (!Number.isFinite(timestamp)) {
    return { ok: false, status: 400, reason: 'Invalid webhook timestamp' };
  }
  const nowEpoch = Math.floor(Date.now() / 1000);
  if (Math.abs(nowEpoch - timestamp) > webhookTimestampToleranceSeconds) {
    return { ok: false, status: 400, reason: 'Webhook timestamp outside tolerance window' };
  }
  purgeExpiredNonces(provider, nowEpoch);
  const byProvider = webhookNonceIndex.by_provider || {};
  const nonceMap = readSafe(byProvider, provider) || {};
  if (readSafe(nonceMap, nonce)) {
    return { ok: false, status: 409, reason: 'Webhook nonce replay detected' };
  }
  writeSafe(nonceMap, nonce, nowEpoch + webhookNonceTtlSeconds);
  writeSafe(byProvider, provider, nonceMap);
  webhookNonceIndex.by_provider = byProvider;
  persistWebhookNonceState();
  return { ok: true };
}

function recordInboxEvent(source, messageId, payload) {
  const key = `${source}:${messageId}`;
  const existing = readSafe(eventInbox.by_key, key);
  if (existing) {
    return { duplicate: true, event: existing };
  }
  const event = {
    source,
    message_id: messageId,
    payload,
    recorded_at: nowIso(),
  };
  writeSafe(eventInbox.by_key, key, event);
  persistInboxState();
  return { duplicate: false, event };
}

function recordOutboxEvent(topic, dedupeKey, payload) {
  const existing = (eventOutbox.items || []).find((item) => item.dedupe_key === dedupeKey);
  if (existing) {
    return existing;
  }
  const event = {
    event_id: orderRef('evt'),
    topic,
    dedupe_key: dedupeKey,
    payload,
    status: 'pending',
    created_at: nowIso(),
  };
  eventOutbox.items.push(event);
  eventOutbox.items = eventOutbox.items.slice(-5000);
  persistOutboxState();
  return event;
}

function registerExternalEffect(effectKey, payload) {
  const existing = readSafe(externalEffects.by_key, effectKey);
  if (existing) {
    return { duplicate: true, effect: existing };
  }
  const effect = {
    effect_key: effectKey,
    payload,
    created_at: nowIso(),
  };
  writeSafe(externalEffects.by_key, effectKey, effect);
  externalEffects.order.push(effectKey);
  while (externalEffects.order.length > externalEffectRetention) {
    const key = externalEffects.order.shift();
    if (key && !isUnsafeObjectKey(key)) {
      delete externalEffects.by_key[key];
    }
  }
  persistEffectsState();
  return { duplicate: false, effect };
}

function queueReconciliationTask(reason, payload, dedupeKey) {
  const existing = reconciliationQueue.items.find((item) => item.dedupe_key === dedupeKey);
  if (existing) {
    return existing;
  }
  const task = {
    task_id: orderRef('rqn'),
    reason,
    payload,
    dedupe_key: dedupeKey,
    retry_count: 0,
    run_after_epoch_ms: Date.now(),
    created_at: nowIso(),
  };
  reconciliationQueue.items.push(task);
  reconciliationQueue.items = reconciliationQueue.items.slice(-2000);
  persistReconciliationQueueState();
  return task;
}

function runReconciliationWorkerPass() {
  const nowMs = Date.now();
  const pending = [];
  const ready = [];
  for (const task of reconciliationQueue.items) {
    if (Number(task.run_after_epoch_ms || 0) <= nowMs) {
      ready.push(task);
    } else {
      pending.push(task);
    }
  }
  for (const task of ready) {
    if (task.payload && task.payload.force_fail) {
      task.retry_count = Number(task.retry_count || 0) + 1;
      if (task.retry_count >= reconciliationMaxAttempts) {
        reconciliationDlq.items.push({
          ...task,
          status: 'dead_lettered',
          dead_lettered_at: nowIso(),
        });
        reconciliationDlq.items = reconciliationDlq.items.slice(-1000);
      } else {
        const boundedExponent = Math.min(Number(task.retry_count || 0), 20);
        const delayMs = Math.min((2 ** boundedExponent) * 1000, reconciliationMaxBackoffMs);
        task.run_after_epoch_ms = nowMs + delayMs;
        pending.push(task);
      }
      continue;
    }
    const effectKey = `recon:${task.dedupe_key}`;
    registerExternalEffect(effectKey, { reason: task.reason, payload: task.payload });
    recordOutboxEvent('billing.reconciliation.completed', effectKey, {
      task_id: task.task_id,
      reason: task.reason,
      payload: task.payload,
    });
  }
  reconciliationQueue.items = pending;
  persistReconciliationQueueState();
  persistReconciliationDlqState();
  return {
    processed: ready.length,
    queued: reconciliationQueue.items.length,
    dlq: reconciliationDlq.items.length,
  };
}

function upsertSubscriptionFromOrder(order) {
  const existing = readSafe(subscriptionsState.by_tenant, order.tenant_id);
  const subscription = {
    subscription_id: existing?.subscription_id || orderRef('sub'),
    tenant_id: order.tenant_id,
    provider: order.provider,
    plan: order.plan || existing?.plan || 'starter',
    status: 'active',
    order_id: order.order_id,
    started_at: existing?.started_at || nowIso(),
    updated_at: nowIso(),
  };
  writeSafe(subscriptionsState.by_tenant, order.tenant_id, subscription);
  persistSubscriptionsState();
  return subscription;
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
  const envelope = validateWebhookEnvelope('stripe', req);
  if (!envelope.ok) {
    return res.status(envelope.status).json({ error: envelope.reason, anomaly_flag: true });
  }

  const signature = req.headers['stripe-signature'];
  try {
    const event = stripe.webhooks.constructEvent(req.body, signature, stripeWebhookSecret);
    const eventId = event?.id || extractBodyEventId('stripe', req.body) || sha256(req.body);
    const inbox = recordInboxEvent('stripe', eventId, event);
    if (inbox.duplicate) {
      return res.status(409).json({ error: 'Webhook replay detected', dedupe_key: `stripe:${eventId}` });
    }
    const ledger = recordWebhookLedger({
      provider: 'stripe',
      eventId,
      signature,
      payloadBuffer: req.body,
    });
    if (ledger.duplicate) {
      return res.status(409).json({ error: 'Webhook replay detected', dedupe_key: ledger.dedupeKey });
    }
    queueReconciliationTask('webhook_ingested', { provider: 'stripe', event_id: eventId }, `webhook:stripe:${eventId}`);
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
  const envelope = validateWebhookEnvelope('razorpay', req);
  if (!envelope.ok) {
    return res.status(envelope.status).json({ error: envelope.reason, anomaly_flag: true });
  }
  const signature = req.headers['x-razorpay-signature'];
  if (!verifyHmac(req.body, signature, secret)) {
    return res.status(400).json({ error: 'Invalid Razorpay webhook signature' });
  }
  const eventId = extractBodyEventId('razorpay', req.body) || sha256(req.body);
  const inbox = recordInboxEvent('razorpay', eventId, req.body.toString('utf8'));
  if (inbox.duplicate) {
    return res.status(409).json({ error: 'Webhook replay detected', dedupe_key: `razorpay:${eventId}` });
  }
  const ledger = recordWebhookLedger({
    provider: 'razorpay',
    eventId,
    signature,
    payloadBuffer: req.body,
  });
  if (ledger.duplicate) {
    return res.status(409).json({ error: 'Webhook replay detected', dedupe_key: ledger.dedupeKey });
  }
  queueReconciliationTask('webhook_ingested', { provider: 'razorpay', event_id: eventId }, `webhook:razorpay:${eventId}`);
  return res.json({ received: true, dedupe_key: ledger.dedupeKey });
});

app.post('/webhooks/phonepe', webhookLimiter, express.raw({ type: 'application/json' }), (req, res) => {
  const secret = process.env.PHONEPE_WEBHOOK_SECRET || '';
  if (!secret) {
    return res.status(200).json({ accepted: true, note: 'phonepe not configured' });
  }
  const envelope = validateWebhookEnvelope('phonepe', req);
  if (!envelope.ok) {
    return res.status(envelope.status).json({ error: envelope.reason, anomaly_flag: true });
  }
  const signature = req.headers['x-phonepe-signature'];
  if (!verifyHmac(req.body, signature, secret)) {
    return res.status(400).json({ error: 'Invalid PhonePe webhook signature' });
  }
  const eventId = extractBodyEventId('phonepe', req.body) || sha256(req.body);
  const inbox = recordInboxEvent('phonepe', eventId, req.body.toString('utf8'));
  if (inbox.duplicate) {
    return res.status(409).json({ error: 'Webhook replay detected', dedupe_key: `phonepe:${eventId}` });
  }
  const ledger = recordWebhookLedger({
    provider: 'phonepe',
    eventId,
    signature,
    payloadBuffer: req.body,
  });
  if (ledger.duplicate) {
    return res.status(409).json({ error: 'Webhook replay detected', dedupe_key: ledger.dedupeKey });
  }
  queueReconciliationTask('webhook_ingested', { provider: 'phonepe', event_id: eventId }, `webhook:phonepe:${eventId}`);
  return res.json({ received: true, dedupe_key: ledger.dedupeKey });
});

app.post('/webhooks/paytm', webhookLimiter, express.raw({ type: 'application/json' }), (req, res) => {
  const secret = process.env.PAYTM_WEBHOOK_SECRET || '';
  if (!secret) {
    return res.status(200).json({ accepted: true, note: 'paytm not configured' });
  }
  const envelope = validateWebhookEnvelope('paytm', req);
  if (!envelope.ok) {
    return res.status(envelope.status).json({ error: envelope.reason, anomaly_flag: true });
  }
  const signature = req.headers['x-paytm-signature'];
  if (!verifyHmac(req.body, signature, secret)) {
    return res.status(400).json({ error: 'Invalid Paytm webhook signature' });
  }
  const eventId = extractBodyEventId('paytm', req.body) || sha256(req.body);
  const inbox = recordInboxEvent('paytm', eventId, req.body.toString('utf8'));
  if (inbox.duplicate) {
    return res.status(409).json({ error: 'Webhook replay detected', dedupe_key: `paytm:${eventId}` });
  }
  const ledger = recordWebhookLedger({
    provider: 'paytm',
    eventId,
    signature,
    payloadBuffer: req.body,
  });
  if (ledger.duplicate) {
    return res.status(409).json({ error: 'Webhook replay detected', dedupe_key: ledger.dedupeKey });
  }
  queueReconciliationTask('webhook_ingested', { provider: 'paytm', event_id: eventId }, `webhook:paytm:${eventId}`);
  return res.json({ received: true, dedupe_key: ledger.dedupeKey });
});

app.use(express.json());

app.use((req, res, next) => {
  req.requestId = req.headers['x-request-id'] || crypto.randomUUID();
  req.correlationId = req.headers['x-correlation-id'] || req.requestId;
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
  res.setHeader('x-correlation-id', req.correlationId);
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

app.post('/orders', adminWriteLimiter, (req, res) => {
  if (!verifyAdmin(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const {
    tenant_id,
    provider = 'manual',
    amount,
    currency = 'USD',
    idempotency_key,
    metadata = {},
    plan = 'starter',
  } = req.body || {};
  if (!tenant_id || !amount || !idempotency_key) {
    return res.status(400).json({ error: 'tenant_id, amount and idempotency_key are required' });
  }
  const idemKey = `order:create:${idempotency_key}`;
  const existingOrderId = readSafe(ordersState.by_idempotency, idemKey);
  const existingOrder = existingOrderId ? readSafe(ordersState.by_id, existingOrderId) : undefined;
  if (existingOrderId && existingOrder) {
    return res.json({ idempotent: true, order: existingOrder });
  }
  const order = {
    order_id: orderRef('ord'),
    tenant_id,
    provider: normalizeProvider(provider),
    amount,
    currency,
    plan,
    metadata,
    status: 'created',
    created_at: nowIso(),
    updated_at: nowIso(),
  };
  writeSafe(ordersState.by_id, order.order_id, order);
  writeSafe(ordersState.by_idempotency, idemKey, order.order_id);
  persistOrdersState();
  const started = upsertSubscriptionFromOrder(order);
  recordOutboxEvent('billing.order.created', `order:${order.order_id}:created`, order);
  queueReconciliationTask('order_created', { order_id: order.order_id, provider: order.provider }, `order:${order.order_id}:created`);
  auditLog('billing_order_created', { order_id: order.order_id, tenant_id: order.tenant_id, ...buildAuditContext(req) });
  return res.status(201).json({ idempotent: false, order, subscription: started });
});

app.get('/orders/:orderId', adminReadLimiter, (req, res) => {
  if (!canReadProviders(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const order = readSafe(ordersState.by_id, req.params.orderId);
  if (!order) {
    return res.status(404).json({ error: 'Order not found' });
  }
  return res.json(order);
});

app.get('/orders', adminReadLimiter, (req, res) => {
  if (!canReadProviders(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const provider = req.query.provider ? normalizeProvider(req.query.provider) : '';
  const tenant = req.query.tenant_id ? String(req.query.tenant_id) : '';
  let items = Object.values(ordersState.by_id || {});
  if (provider) {
    items = items.filter((item) => item.provider === provider);
  }
  if (tenant) {
    items = items.filter((item) => item.tenant_id === tenant);
  }
  return res.json({ items });
});

app.post('/orders/:orderId/cancel', adminWriteLimiter, (req, res) => {
  if (!verifyAdmin(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const order = readSafe(ordersState.by_id, req.params.orderId);
  if (!order) {
    return res.status(404).json({ error: 'Order not found' });
  }
  if (order.status === 'cancelled') {
    return res.json({ idempotent: true, order });
  }
  const updatedOrder = { ...order, status: 'cancelled', updated_at: nowIso() };
  writeSafe(ordersState.by_id, req.params.orderId, updatedOrder);
  persistOrdersState();
  recordOutboxEvent('billing.order.cancelled', `order:${updatedOrder.order_id}:cancelled`, updatedOrder);
  queueReconciliationTask(
    'order_cancelled',
    { order_id: updatedOrder.order_id, provider: updatedOrder.provider },
    `order:${updatedOrder.order_id}:cancelled`
  );
  auditLog('billing_order_cancelled', { order_id: updatedOrder.order_id, tenant_id: updatedOrder.tenant_id, ...buildAuditContext(req) });
  return res.json({ idempotent: false, order: updatedOrder });
});

app.post('/subscriptions/start', adminWriteLimiter, (req, res) => {
  if (!verifyAdmin(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const { tenant_id, provider = 'manual', plan = 'starter' } = req.body || {};
  if (!tenant_id) {
    return res.status(400).json({ error: 'tenant_id is required' });
  }
  const subscription = {
    subscription_id: orderRef('sub'),
    tenant_id,
    provider: normalizeProvider(provider),
    plan,
    status: 'active',
    started_at: nowIso(),
    updated_at: nowIso(),
  };
  writeSafe(subscriptionsState.by_tenant, tenant_id, subscription);
  persistSubscriptionsState();
  recordOutboxEvent('billing.subscription.started', `subscription:${tenant_id}:started`, subscription);
  auditLog('billing_subscription_started', { tenant_id, subscription_id: subscription.subscription_id, ...buildAuditContext(req) });
  return res.status(201).json(subscription);
});

app.post('/subscriptions/:tenantId/upgrade', adminWriteLimiter, (req, res) => {
  if (!verifyAdmin(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const subscription = readSafe(subscriptionsState.by_tenant, req.params.tenantId);
  if (!subscription) {
    return res.status(404).json({ error: 'Subscription not found' });
  }
  const plan = req.body?.plan || 'growth';
  const updatedSubscription = { ...subscription, plan, status: 'active', updated_at: nowIso() };
  writeSafe(subscriptionsState.by_tenant, req.params.tenantId, updatedSubscription);
  persistSubscriptionsState();
  recordOutboxEvent('billing.subscription.upgraded', `subscription:${updatedSubscription.tenant_id}:upgrade:${plan}`, updatedSubscription);
  auditLog('billing_subscription_upgraded', { tenant_id: updatedSubscription.tenant_id, plan, ...buildAuditContext(req) });
  return res.json(updatedSubscription);
});

app.post('/subscriptions/:tenantId/downgrade', adminWriteLimiter, (req, res) => {
  if (!verifyAdmin(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const subscription = readSafe(subscriptionsState.by_tenant, req.params.tenantId);
  if (!subscription) {
    return res.status(404).json({ error: 'Subscription not found' });
  }
  const plan = req.body?.plan || 'starter';
  const updatedSubscription = { ...subscription, plan, status: 'active', updated_at: nowIso() };
  writeSafe(subscriptionsState.by_tenant, req.params.tenantId, updatedSubscription);
  persistSubscriptionsState();
  recordOutboxEvent('billing.subscription.downgraded', `subscription:${updatedSubscription.tenant_id}:downgrade:${plan}`, updatedSubscription);
  auditLog('billing_subscription_downgraded', { tenant_id: updatedSubscription.tenant_id, plan, ...buildAuditContext(req) });
  return res.json(updatedSubscription);
});

app.post('/subscriptions/:tenantId/cancel', adminWriteLimiter, (req, res) => {
  if (!verifyAdmin(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const subscription = readSafe(subscriptionsState.by_tenant, req.params.tenantId);
  if (!subscription) {
    return res.status(404).json({ error: 'Subscription not found' });
  }
  const updatedSubscription = { ...subscription, status: 'cancelled', updated_at: nowIso() };
  writeSafe(subscriptionsState.by_tenant, req.params.tenantId, updatedSubscription);
  persistSubscriptionsState();
  recordOutboxEvent('billing.subscription.cancelled', `subscription:${updatedSubscription.tenant_id}:cancelled`, updatedSubscription);
  auditLog('billing_subscription_cancelled', { tenant_id: updatedSubscription.tenant_id, ...buildAuditContext(req) });
  return res.json(updatedSubscription);
});

app.get('/subscriptions/:tenantId/current', adminReadLimiter, (req, res) => {
  if (!canReadProviders(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const subscription = readSafe(subscriptionsState.by_tenant, req.params.tenantId);
  if (!subscription) {
    return res.status(404).json({ error: 'Subscription not found' });
  }
  return res.json(subscription);
});

app.post('/orders/:orderId/refunds', adminWriteLimiter, (req, res) => {
  if (!verifyAdmin(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const order = readSafe(ordersState.by_id, req.params.orderId);
  if (!order) {
    return res.status(404).json({ error: 'Order not found' });
  }
  const { amount = order.amount, reason = 'customer_request', idempotency_key = '' } = req.body || {};
  const effectKey = `refund:${order.order_id}:${idempotency_key || sha256(JSON.stringify({ amount, reason }))}`;
  const effect = registerExternalEffect(effectKey, { order_id: order.order_id, amount, reason });
  if (effect.duplicate) {
    const existingRefund = Object.values(refundsState.by_id).find((item) => item.effect_key === effectKey);
    if (existingRefund) {
      return res.json({ idempotent: true, refund: existingRefund });
    }
  }
  const refund = {
    refund_id: orderRef('rfd'),
    order_id: order.order_id,
    tenant_id: order.tenant_id,
    provider: order.provider,
    amount,
    reason,
    status: 'succeeded',
    effect_key: effectKey,
    created_at: nowIso(),
  };
  writeSafe(refundsState.by_id, refund.refund_id, refund);
  const existingByOrder = readSafe(refundsState.by_order, order.order_id) || [];
  writeSafe(refundsState.by_order, order.order_id, [...existingByOrder, refund.refund_id]);
  persistRefundsState();
  recordOutboxEvent('billing.refund.succeeded', `refund:${refund.refund_id}:succeeded`, refund);
  queueReconciliationTask('refund_created', { refund_id: refund.refund_id, order_id: order.order_id }, `refund:${refund.refund_id}`);
  auditLog('billing_refund_created', { refund_id: refund.refund_id, order_id: order.order_id, ...buildAuditContext(req) });
  return res.status(201).json({ idempotent: false, refund });
});

app.get('/orders/:orderId/refunds', adminReadLimiter, (req, res) => {
  if (!canReadProviders(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const ids = readSafe(refundsState.by_order, req.params.orderId) || [];
  return res.json({ items: ids.map((id) => readSafe(refundsState.by_id, id)).filter(Boolean) });
});

app.post('/orders/:orderId/disputes', adminWriteLimiter, (req, res) => {
  if (!verifyAdmin(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const order = readSafe(ordersState.by_id, req.params.orderId);
  if (!order) {
    return res.status(404).json({ error: 'Order not found' });
  }
  const { reason = 'fraudulent', evidence = {} } = req.body || {};
  const dispute = {
    dispute_id: orderRef('dsp'),
    order_id: order.order_id,
    tenant_id: order.tenant_id,
    provider: order.provider,
    reason,
    evidence,
    status: 'open',
    created_at: nowIso(),
    updated_at: nowIso(),
  };
  writeSafe(disputesState.by_id, dispute.dispute_id, dispute);
  const existingDisputes = readSafe(disputesState.by_order, order.order_id) || [];
  writeSafe(disputesState.by_order, order.order_id, [...existingDisputes, dispute.dispute_id]);
  persistDisputesState();
  recordOutboxEvent('billing.dispute.opened', `dispute:${dispute.dispute_id}:opened`, dispute);
  queueReconciliationTask('dispute_opened', { dispute_id: dispute.dispute_id, order_id: order.order_id }, `dispute:${dispute.dispute_id}:opened`);
  auditLog('billing_dispute_opened', { dispute_id: dispute.dispute_id, order_id: order.order_id, ...buildAuditContext(req) });
  return res.status(201).json(dispute);
});

app.post('/disputes/:disputeId/resolve', adminWriteLimiter, (req, res) => {
  if (!verifyAdmin(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const dispute = readSafe(disputesState.by_id, req.params.disputeId);
  if (!dispute) {
    return res.status(404).json({ error: 'Dispute not found' });
  }
  const resolution = req.body?.resolution || 'won';
  const resolvedDispute = {
    ...dispute,
    status: resolution === 'lost' ? 'lost' : 'won',
    resolution,
    updated_at: nowIso(),
  };
  writeSafe(disputesState.by_id, req.params.disputeId, resolvedDispute);
  persistDisputesState();
  recordOutboxEvent('billing.dispute.resolved', `dispute:${resolvedDispute.dispute_id}:resolved:${resolution}`, resolvedDispute);
  auditLog('billing_dispute_resolved', { dispute_id: resolvedDispute.dispute_id, resolution, ...buildAuditContext(req) });
  return res.json(resolvedDispute);
});

app.get('/orders/:orderId/disputes', adminReadLimiter, (req, res) => {
  if (!canReadProviders(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const ids = readSafe(disputesState.by_order, req.params.orderId) || [];
  return res.json({ items: ids.map((id) => readSafe(disputesState.by_id, id)).filter(Boolean) });
});

app.get('/admin/payments/effects', adminReadLimiter, (req, res) => {
  if (!canReadProviders(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const limit = Math.max(1, Math.min(Number(req.query.limit || 50), 200));
  const keys = externalEffects.order.slice(-limit).reverse();
  return res.json({ items: keys.map((key) => readSafe(externalEffects.by_key, key)).filter(Boolean) });
});

app.get('/admin/events/inbox', adminReadLimiter, (req, res) => {
  if (!canReadProviders(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const items = Object.values(eventInbox.by_key || {}).slice(-100);
  return res.json({ items });
});

app.get('/admin/events/outbox', adminReadLimiter, (req, res) => {
  if (!canReadProviders(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const limit = Math.max(1, Math.min(Number(req.query.limit || 100), 500));
  return res.json({ items: (eventOutbox.items || []).slice(-limit).reverse() });
});

app.post('/admin/payments/reconciliation/worker/pass', adminWriteLimiter, (req, res) => {
  if (!verifyAdmin(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const result = runReconciliationWorkerPass();
  auditLog('payment_reconciliation_worker_pass', { ...result, ...buildAuditContext(req) });
  return res.json(result);
});

app.get('/admin/payments/reconciliation/queue', adminReadLimiter, (req, res) => {
  if (!canReadProviders(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  return res.json({ items: reconciliationQueue.items });
});

app.get('/admin/payments/reconciliation/dlq', adminReadLimiter, (req, res) => {
  if (!canReadProviders(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  return res.json({ items: reconciliationDlq.items });
});

if (reconciliationWorkerIntervalMs > 0) {
  setInterval(() => {
    try {
      runReconciliationWorkerPass();
    } catch (error) {
      console.error('reconciliation worker pass failed', error);
    }
  }, reconciliationWorkerIntervalMs);
}

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
  auditLog('payment_provider_setup', { provider, ...buildAuditContext(req) });
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
  auditLog('payment_reconciliation_run', { provider, run_id: run.run_id, ...buildAuditContext(req) });
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

app.get('/admin/audit-logs', adminReadLimiter, (req, res) => {
  if (!canReadProviders(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const limit = Math.max(1, Math.min(Number(req.query.limit || 100), 500));
  const actor = req.query.actor ? String(req.query.actor) : '';
  const action = req.query.action ? String(req.query.action) : '';
  const resourceType = req.query.resource_type ? String(req.query.resource_type) : '';
  const since = req.query.since ? String(req.query.since) : '';
  const sinceTs = since ? Date.parse(since) : null;
  if (since && Number.isNaN(sinceTs)) {
    return res.status(400).json({ error: 'Invalid since timestamp' });
  }
  const items = (auditLogsState.items || [])
    .filter((item) => !actor || item.actor === actor)
    .filter((item) => !action || item.event === action)
    .filter((item) => !resourceType || item.resource_type === resourceType)
    .filter((item) => !sinceTs || Date.parse(item.at) >= sinceTs)
    .slice(-limit)
    .reverse();
  return res.json({ items });
});

app.get('/admin/audit-logs/export', adminReadLimiter, (req, res) => {
  if (!canReadProviders(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const limit = Math.max(1, Math.min(Number(req.query.limit || 200), 1000));
  const items = (auditLogsState.items || []).slice(-limit).reverse();
  const header = 'at,actor,event,resource_type,resource_id,details';
  const rows = items.map((item) => {
    const details = JSON.stringify(item.details || {}).replaceAll('"', '""');
    return `"${item.at}","${item.actor || ''}","${item.event || ''}","${item.resource_type || ''}","${item.resource_id || ''}","${details}"`;
  });
  res.setHeader('content-type', 'text/csv');
  res.setHeader('content-disposition', 'attachment; filename=billing-audit-logs.csv');
  return res.send(`${header}\n${rows.join('\n')}`);
});

app.post('/admin/audit-logs/retention', adminWriteLimiter, (req, res) => {
  if (!verifyAdmin(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  const dryRun = Boolean(req.body?.dry_run);
  const retentionDays = Math.max(1, Number(req.body?.retention_days || auditRetentionDays));
  const cutoffMs = Date.now() - (retentionDays * 24 * 60 * 60 * 1000);
  const existing = auditLogsState.items || [];
  const staleCount = existing.filter((item) => Date.parse(item.at) < cutoffMs).length;
  if (!dryRun && staleCount > 0) {
    auditLogsState.items = existing.filter((item) => Date.parse(item.at) >= cutoffMs);
    persistAuditLogsState();
  }
  auditLog('billing_audit_retention_enforced', {
    actor: req.headers['x-admin-actor'] || 'unknown',
    resource_type: 'audit',
    resource_id: 'billing',
    dry_run: dryRun,
    retention_days: retentionDays,
    deleted: staleCount,
    ...buildAuditContext(req),
  });
  return res.json({ deleted: staleCount, dry_run: dryRun, retention_days: retentionDays });
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
