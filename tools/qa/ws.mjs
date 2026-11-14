const B = 'http://127.0.0.1:18080';
const at = new Date(Date.now() + 3000).toISOString();
const r = await fetch(B + '/v1/notifications', { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Correlation-ID': 'qa-ws-1' },
  body: JSON.stringify({ to: '+905551234567', channel: 'sms', content: 'ws qa', scheduledAt: at }) });
const n = await r.json();
console.log('created', n.status, n.id);
const ws = new WebSocket('ws://127.0.0.1:18080/v1/ws?notificationId=' + n.id);
const seen = [];
ws.onmessage = (e) => { const m = JSON.parse(e.data); seen.push(m.status); console.log('event', JSON.stringify(m).slice(0, 260)); if (m.status === 'sent') { console.log('order:', seen.join(' -> ')); ws.close(); } };
ws.onerror = (e) => { console.log('ws error', e.message || e.type); };
ws.onclose = (e) => { console.log('closed', e.code); process.exit(0); };
setTimeout(() => { console.log('TIMEOUT; seen', seen.join(' -> ')); process.exit(1); }, 15000);
