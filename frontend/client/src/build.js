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
      <h1 class="text-2xl font-bold mb-4">Client Dashboard</h1>
      <div class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <div class="bg-white p-4 rounded shadow">Campaigns: 3</div>
        <div class="bg-white p-4 rounded shadow">Leads: 1,240</div>
        <div class="bg-white p-4 rounded shadow">Warm-up Jobs: 2</div>
        <div class="bg-white p-4 rounded shadow">CRM Open Items: 18</div>
      </div>
      <div class="bg-white p-4 rounded shadow">
        <h2 class="font-semibold mb-2">Quick Actions</h2>
        <ul class="list-disc ml-6">
          <li>Create Campaign</li>
          <li>Upload Leads</li>
          <li>Start Warm-up</li>
          <li>Run Verification</li>
        </ul>
      </div>
    </div>
  </body>
</html>`;

fs.writeFileSync(path.join(distDir, 'index.html'), html);
