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

function saveProviderConfig() {
  const key = encryptionKey();
  fs.mkdirSync(path.dirname(providerConfigPath), { recursive: true });
  const payload = Buffer.from(JSON.stringify(paymentProviders));
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

function loadProviderConfig() {
  if (!fs.existsSync(providerConfigPath)) {
    return;
  }
  const raw = fs.readFileSync(providerConfigPath, 'utf8').trim();
  if (!raw) {
    return;
  }
  try {
    const parsed = JSON.parse(raw);
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

loadProviderConfig();

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
    stripe.webhooks.constructEvent(req.body, signature, stripeWebhookSecret);
    return res.json({ received: true });
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
  return res.json({ received: true });
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
  return res.json({ received: true });
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
  return res.json({ received: true });
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
  if (!canReadProviders(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  return res.json({ providers: paymentProviders });
});

app.post('/admin/payments/providers/:provider/setup', adminWriteLimiter, (req, res) => {
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
  saveProviderConfig();
  auditLog('payment_provider_setup', { provider, actor: req.headers['x-admin-actor'] || 'unknown' });
  return res.json({ provider, config: paymentProviders[provider] });
});

process.on('SIGTERM', async () => {
  await sdk.shutdown();
  process.exit(0);
});

app.listen(3001, () => {
  console.log('billing listening on 3001');
});
