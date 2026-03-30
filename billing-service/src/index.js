import express from 'express';
import Stripe from 'stripe';
import crypto from 'crypto';
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

app.post('/webhooks/stripe', express.raw({ type: 'application/json' }), (req, res) => {
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

process.on('SIGTERM', async () => {
  await sdk.shutdown();
  process.exit(0);
});

app.listen(3001, () => {
  console.log('billing listening on 3001');
});
