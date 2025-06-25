// digital-mailroom-webhook/index.js
import express from 'express';
import axios from 'axios';
import FormData from 'form-data';

const app = express();
app.use(express.json());

// ────────────────────  HARD-CODED CONFIG  ────────────────────
// Monday.com
const MONDAY_API_TOKEN        = 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjUzMDYzOTcxOSwiYWFpIjoxMSwidWlkIjo2Nzg2NjA4MywiaWFkIjoiMjAyNS0wNi0yNFQyMjoxNjowMC42NTJaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MjYyMDQ5OTgsInJnbiI6InVzZTEifQ.zv9EsISZnchs7WKSqN2t3UU1GwcLrzPGeaP7ssKIla8';
const MONDAY_FILE_BOARD_ID    = 9445652448;   // “File Uploads”
const MONDAY_EXTRACT_BOARD_ID = 9446325745;   // “Extracted Documents”

// Instabase
const IB_API_TOKEN            = '8m91cy8nOMrZcZI5YspXpOPQtJ6p8o';
const IB_DEPLOYMENT_ID        = '0197a3fe-599d-7bac-a34b-81704cc83beb';
const IB_WORKSPACE            = 'nileshn_sturgeontire.com';
const IB_CONTEXT              = 'sturgeontire';

// ────────────────────  HELPERS  ────────────────────
const monday = axios.create({
  baseURL: 'https://api.monday.com/v2',
  headers: { Authorization: `Bearer ${MONDAY_API_TOKEN}` }
});

const instabase = axios.create({
  baseURL: 'https://aihub.instabase.com/api/v2',
  headers: {
    Authorization: `Bearer ${IB_API_TOKEN}`,
    'IB-Context': IB_CONTEXT
  }
});

// ────────────────────  WEBHOOK ENTRY POINT  ────────────────────
app.post('/webhook/monday-to-instabase', async (req, res) => {
  // handshake
  if (req.body?.challenge) return res.json({ challenge: req.body.challenge });
  res.json({ ok: true });                       // Ack — avoid Monday timeout
  handleWebhook(req.body).catch(console.error);
});

async function handleWebhook(body) {
  const evt = body?.event;
  if (!evt) return;

  const { pulseId: itemId, value, columnId } = evt;
  if (columnId !== 'status' || value?.label?.text !== 'Processing') return;

  const files = await fetchFiles(itemId);
  if (!files.length) return console.log('No PDFs on item', itemId);

  const extracted = await runInstabase(files);
  const docs      = groupByInvoice(extracted);
  await writeBack(docs, itemId);

  console.log(`✓ Finished processing Monday item ${itemId}`);
}

// ────────────────────  MONDAY.COM  ────────────────────
async function fetchFiles(itemId) {
  const q = `query($id:[Int]!){items(ids:$id){column_values{id,type,value,text}}}`;
  const { data } = await monday.post('', { query: q, variables: { id: itemId } });

  const col = data.data.items[0].column_values
    .find(c => c.type === 'file' || c.id.includes('file'));
  if (!col?.value) return [];

  const fileObjs = JSON.parse(col.value).files || [];
  return Promise.all(fileObjs.map(async f => ({
    name: f.name,
    url : f.url || await assetUrl(f.assetId) || col.text
  })));
}

async function assetUrl(assetId) {
  const q = `query($id:[Int]!){assets(ids:$id){url}}`;
  const { data } = await monday.post('', { query: q, variables: { id: assetId } });
  return data.data.assets[0]?.url;
}

// ────────────────────  INSTABASE  ────────────────────
async function runInstabase(files) {
  // 1. create batch
  const { data: batch } = await instabase.post('/batches', {
    name: `mailroom-${Date.now()}`,
    workspace: IB_WORKSPACE
  });

  // 2. upload PDFs
  for (const f of files) {
    const { data: buf } = await axios.get(f.url, { responseType: 'arraybuffer' });
    await instabase.put(`/batches/${batch.id}/files/${encodeURIComponent(f.name)}`, buf, {
      headers: { 'Content-Type': 'application/pdf' }
    });
  }

  // 3. run deployment
  const { data: run } = await instabase.post(
    `/deployments/${IB_DEPLOYMENT_ID}/runs`,
    { batch_id: batch.id }
  );

  // 4. poll
  let status = 'RUNNING', tries = 0;
  while ((status === 'RUNNING' || status === 'PENDING') && tries++ < 60) {
    await new Promise(r => setTimeout(r, 5000));
    status = (await instabase.get(`/runs/${run.id}`)).data.status;
  }
  if (status !== 'COMPLETE') throw new Error(`Instabase run ${run.id} ${status}`);

  // 5. results
  return (await instabase.get(`/runs/${run.id}/results`)).data.files;
}

// ────────────────────  POST-PROCESS  ────────────────────
function groupByInvoice(files) {
  const map = {};
  files.forEach(f => {
    f.documents.forEach(d => {
      const inv = d.fields.invoice_number?.value;
      if (!inv) return;
      (map[inv] ||= { ...d.fields, pages: [] }).pages.push({
        file: f.original_file_name,
        fields: d.fields
      });
    });
  });
  return Object.values(map);
}

async function writeBack(docs, srcId) {
  for (const d of docs) {
    const cols = {
      source_request_id:  srcId,
      document_number   : d.invoice_number?.value,
      document_type     : d.document_type?.value,
      amount            : d.total_amount?.value,
      supplier          : d.supplier_name?.value,
      document_date     : d.document_date?.value,
      due_date          : d.due_date?.value,
      extraction_status : 'Extracted'
    };
    const m = `
      mutation($b:Int!,$n:String!,$v:JSON!){
        create_item(board_id:$b,item_name:$n,column_values:$v){id}
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

// ────────────────────  HEALTHCHECK & BOOT  ────────────────────
app.get('/health', (_, res) => res.json({ ok: true, ts: Date.now() }));
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Webhook running on :${PORT}`));
