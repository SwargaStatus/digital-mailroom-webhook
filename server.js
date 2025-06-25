// digital-mailroom-webhook/index.js
import express from 'express';
import axios   from 'axios';

const app = express();
app.use(express.json());

// ────────────────  CONFIG (hard-coded for quick test)  ────────────────
const MONDAY_API_TOKEN        = 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjUzMDYzOTcxOSwiYWFpIjoxMSwidWlkIjo2Nzg2NjA4MywiaWFkIjoiMjAyNS0wNi0yNFQyMjoxNjowMC42NTJaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MjYyMDQ5OTgsInJnbiI6InVzZTEifQ.zv9EsISZnchs7WKSqN2t3UU1GwcLrzPGeaP7ssKIla8';
const MONDAY_FILE_BOARD_ID    = 9445652448;
const MONDAY_EXTRACT_BOARD_ID = 9446325745;

const IB_API_TOKEN     = '8m91cy8nOMrZcZI5YspXpOPQtJ6p8o';
const IB_DEPLOYMENT_ID = '0197a3fe-599d-7bac-a34b-81704cc83beb';
const IB_WORKSPACE     = 'nileshn_sturgeontire.com';
const IB_CONTEXT       = 'sturgeontire';

// ────────────────  AXIOS HELPERS  ────────────────
const monday = axios.create({
  baseURL: 'https://api.monday.com/v2',
  headers: { Authorization: `Bearer ${MONDAY_API_TOKEN}` }
});

const instabase = axios.create({
  baseURL: 'https://aihub.instabase.com/api/v2',
  headers: { Authorization: `Bearer ${IB_API_TOKEN}`, 'IB-Context': IB_CONTEXT }
});

// ────────────────  WEBHOOK  ────────────────
app.post('/webhook/monday-to-instabase', (req, res) => {
  if (req.body?.challenge) return res.json({ challenge: req.body.challenge });
  res.json({ ok: true });
  handleWebhook(req.body).catch(console.error);
});

// ────────────────  CORE  ────────────────
async function handleWebhook(payload) {
  const evt = payload?.event;
  if (!evt) return;

  const { pulseId: itemId, columnId, value } = evt;
  if (columnId !== 'status' || value?.label?.text !== 'Processing') return;

  const files = await fetchFiles(itemId);
  if (!files.length) return console.log('No PDFs on item', itemId);

  const extracted = await runInstabase(files);
  if (!extracted.length) return;

  const docs = groupByInvoice(extracted);
  await writeBack(docs, itemId);

  console.log(`✓ Completed item ${itemId}`);
}

// ────────────────  MONDAY  ────────────────
async function fetchFiles(itemId) {
  const q = `
    query ($ids: [Int]) {
      items (ids: $ids) {
        column_values { id type value text }
      }
    }`;
  const { data } = await monday.post('', { query: q, variables: { ids: [itemId] }});

  const col = data.data.items[0].column_values
             .find(c => c.type === 'file' || c.id.includes('file'));
  if (!col?.value) return [];

  const fileObjs = JSON.parse(col.value).files || [];
  return Promise.all(fileObjs.map(async f => ({
    name: f.name,
    url : await assetUrl(f.assetId) || f.url || col.text
  })));
}

async function assetUrl(assetId) {
  const q = `
    query ($id: Int!) {
      assets (ids: [$id]) { public_url }
    }`;
  const { data } = await monday.post('', { query: q, variables: { id: assetId }});
  return data.data.assets[0]?.public_url;
}

// ────────────────  INSTABASE  ────────────────
async function runInstabase(files) {
  const { data: batch } = await instabase.post('/batches', {
    name: `mailroom-${Date.now()}`, workspace: IB_WORKSPACE
  });

  let uploaded = 0;
  for (const f of files) {
    if (!f.url) continue;
    const { data: buf } = await axios.get(f.url, { responseType: 'arraybuffer' });
    if (buf.slice(0, 4).toString() !== '%PDF') continue;   // skip HTML pages
    await instabase.put(
      `/batches/${batch.id}/files/${encodeURIComponent(f.name)}`,
      buf, { headers: { 'Content-Type': 'application/pdf' } }
    );
    uploaded++;
  }
  if (!uploaded) return [];

  const { data: run } = await instabase.post(
    `/deployments/${IB_DEPLOYMENT_ID}/runs`, { batch_id: batch.id });

  let status = 'RUNNING', tries = 0;
  while ((status === 'RUNNING' || status === 'PENDING') && tries++ < 60) {
    await new Promise(r => setTimeout(r, 5000));
    status = (await instabase.get(`/runs/${run.id}`)).data.status;
  }
  if (status !== 'COMPLETE') throw new Error(`Instabase run ${run.id} ${status}`);

  return (await instabase.get(`/runs/${run.id}/results`)).data.files;
}

// ────────────────  POST-PROCESS  ────────────────
function groupByInvoice(files) {
  const m = {};
  for (const f of files) {
    f.documents.forEach(d => {
      const inv = d.fields.invoice_number?.value;
      if (!inv) return;
      (m[inv] ||= { ...d.fields, pages: [] }).pages.push({
        file: f.original_file_name, fields: d.fields
      });
    });
  }
  return Object.values(m);
}

async function writeBack(docs, srcId) {
  for (const d of docs) {
    const cols = {
      source_request_id: srcId,
      document_number  : d.invoice_number?.value,
      document_type    : d.document_type?.value,
      amount           : d.total_amount?.value,
      supplier         : d.supplier_name?.value,
      document_date    : d.document_date?.value,
      due_date         : d.due_date?.value,
      extraction_status: 'Extracted'
    };
    const m = `
      mutation ($b:Int!, $n:String!, $v:JSON!) {
        create_item(board_id:$b, item_name:$n, column_values:$v){ id }
      }`;
    await monday.post('', {
      query: m,
      variables: {
        b: MONDAY_EXTRACT_BOARD_ID,
        n: `${(d.document_type?.value || 'DOC').toUpperCase()} ${d.invoice_number?.value}`,
        v: JSON.stringify(cols)
      }
    });
  }
}

// ────────────────  HEALTH & BOOT  ────────────────
app.get('/health', (_, res) => res.json({ ok: true, ts: Date.now() }));
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`Webhook running on :${PORT}`));
