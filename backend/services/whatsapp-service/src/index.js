import express from 'express';

const app = express();
app.use(express.json());

const messages = [];

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'whatsapp-service' });
});

app.post('/messages', (req, res) => {
  const { tenantId, to, template, campaignId, body } = req.body || {};
  if (!tenantId || !to || (!template && !body)) {
    return res.status(400).json({ error: 'tenantId, to, and template/body are required' });
  }

  const item = {
    id: `wa-${messages.length + 1}`,
    tenantId,
    to,
    template: template || null,
    body: body || null,
    campaignId: campaignId || null,
    status: 'queued',
    createdAt: new Date().toISOString(),
  };
  messages.push(item);
  return res.status(201).json(item);
});

app.get('/messages', (req, res) => {
  const { tenantId } = req.query;
  const filtered = tenantId ? messages.filter((m) => m.tenantId === tenantId) : messages;
  return res.json({ items: filtered });
});

app.listen(3002, () => {
  console.log('whatsapp-service listening on 3002');
});
