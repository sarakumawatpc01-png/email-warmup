import fs from 'fs';
import path from 'path';

const distDir = path.resolve('src/dist');
fs.mkdirSync(distDir, { recursive: true });

const html = `<!doctype html>
<html>
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Admin Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
  </head>
  <body class="bg-gray-100 text-gray-900">
    <div class="max-w-6xl mx-auto p-6">
      <h1 class="text-2xl font-bold mb-4">Admin Dashboard</h1>
      <div class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <div class="bg-white p-4 rounded shadow">MRR: $24,500</div>
        <div class="bg-white p-4 rounded shadow">Clients: 72</div>
        <div class="bg-white p-4 rounded shadow">Emails Sent: 1.2M</div>
        <div class="bg-white p-4 rounded shadow">System Health: Good</div>
      </div>
      <div class="bg-white p-4 rounded shadow">
        <h2 class="font-semibold mb-2">Recent Logs</h2>
        <p class="text-sm text-gray-700">No critical incidents in the last 24 hours.</p>
      </div>
    </div>
  </body>
</html>`;

fs.writeFileSync(path.join(distDir, 'index.html'), html);
