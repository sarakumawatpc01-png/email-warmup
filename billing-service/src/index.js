import express from 'express';
import Stripe from 'stripe';
import crypto from 'crypto';
import rateLimit from 'express-rate-limit';
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

const paymentProviders = {
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

function verifyAdmin(req) {
  if (!billingAdminApiKey) {
    return true;
  }
  return req.headers['x-admin-api-key'] === billingAdminApiKey;
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
  if (!verifyAdmin(req)) {
    return res.status(403).json({ error: 'Admin API key required' });
  }
  return res.json({ providers: paymentProviders });
});

app.post('/admin/payments/providers/:provider/setup', adminWriteLimiter, (req, res) => {
  if (!verifyAdmin(req)) {
    return res.status(403).json({ error: 'Admin API key required' });
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
