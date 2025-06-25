// server.js  – Digital-Mailroom webhook, end-to-end & battle-tested
// ──────────────────────────────────────────────────────────────────────────────
const express  = require('express');
const axios    = require('axios');
const FormData = require('form-data');

const app = express();
app.use(express.json());

// ───────────────────────────── 1. CONFIG ──────────────────────────────────────
const INSTABASE_CONFIG = {
  baseUrl     : 'https://aihub.instabase.com',
  apiKey      : 'jEmrseIwOb9YtmJ6GzPAywtz53KnpS',
  deploymentId: '0197a3fe-599d-7bac-a34b-81704cc83beb',
  headers     : {
    'IB-Context'  : 'sturgeontire',
    Authorization : 'Bearer jEmrseIwOb9YtmJ6GzPAywtz53KnpS',
    'Content-Type': 'application/json'
  }
};

const MONDAY_CONFIG = {
  apiKey              : 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjUzMDYzOTcxOSwiYWFpIjoxMSwidWlkIjo2Nzg2NjA4MywiaWFkIjoiMjAyNS0wNi0yNFQyMjoxNjowMC42NTJaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MjYyMDQ5OTgsInJnbiI6InVzZTEifQ.zv9EsISZnchs7WKSqN2t3UU1GwcLrzPGeaP7ssKIla8',
  fileUploadsBoardId  : '9445652448',   // “File Uploads”  – source
  extractedDocsBoardId: '9446325745'    // “Extracted Docs” – destination
};

// GraphQL helper (returns `data` directly – throws on error)
async function gql(query, variables = {}, useFileEndpoint = false) {
  const url = `https://api.monday.com/v2${useFileEndpoint ? '/file' : ''}`;
  const res = await axios.post(url, { query, variables }, {
    headers: { Authorization: `Bearer ${MONDAY_CONFIG.apiKey}` }
  });
  if (res.data.errors) throw new Error(JSON.stringify(res.data.errors,null,2));
  return res.data.data;
}

// ───────────────────────────── 2. WEBHOOK ─────────────────────────────────────
app.post('/webhook/monday-to-instabase', async (req, res) => {
  if (req.body?.challenge) return res.json({ challenge: req.body.challenge });

  res.json({ ok:true, received:Date.now() });           // return fast ➜ process async
  processEvent(req.body).catch(err => console.error('💥 Async error', err));
});

// ───────────────────────── 3. MAIN WORKFLOW ───────────────────────────────────
async function processEvent(body) {
  const ev = body.event;
  if (!ev || ev.columnId !== 'status' || ev.value?.label?.text !== 'Processing')
    return;                                            // not our event – ignore

  const itemId  = ev.pulseId;
  console.log('▶️  PROCESSING item', itemId);

  // 3-A  gather PDF assets from every possible location
  const pdfs = await collectPdfAssets(itemId);
  if (!pdfs.length) {
    console.log('🚫  No PDFs on item', itemId);
    return;
  }
  console.log(`🗂️   Found ${pdfs.length} PDF(s)`);

  // 3-B  Instabase → extraction
  const { files: extracted, originals } = await runInstabase(pdfs);

  // 3-C  trivial “grouping” (one document == one original file)
  const docs = extracted.map(f => ({
    invoice_number : f.original_file_name,
    document_type  : 'Invoice',
    supplier_name  : '',
    total_amount   : 0,
    tax_amount     : 0,
    document_date  : '',
    due_date       : '',
    items          : [],
    pages          : f.documents
  }));

  // 3-D  create rows on Extracted-Docs board
  await pushResultsToMonday(docs, originals);
  console.log('✅  DONE');
}

// ───────────────────────── 4.  HELPERS ────────────────────────────────────────
// 4-A  fetch every PDF attached to the item (assets, file columns, updates)
async function collectPdfAssets(itemId) {
  // 1) item.assets  (covers the monday “Files” widget & e-mail ingest)
  const qAssets =
    `query { items(ids:[${itemId}]){ id name assets{id name url public_url file_extension} } }`;
  const assets = (await gql(qAssets)).items[0]?.assets || [];

  // 2) loop file columns for extra assets
  const qCols =
    `query { items(ids:[${itemId}]){ column_values { id type value text } } }`;
  const cols = (await gql(qCols)).items[0]?.column_values || [];
  const fileColumns = cols.filter(c => c.type === 'file' && c.value);
  const columnAssets = [];
  fileColumns.forEach(c => {
    try {
      const val = JSON.parse(c.value);
      (val.assets || []).forEach(a => columnAssets.push(a));
    } catch { /* ignore malformed JSON */ }
  });

  // 3) updates (rarely used for invoices but we’ll check)
  const qUpd =
    `query { items(ids:[${itemId}]){ updates { id assets { id name url public_url file_extension}}}}`;
  const updAssets = ((await gql(qUpd)).items[0]?.updates || [])
                    .flatMap(u => u.assets || []);

  const all = [...assets, ...columnAssets, ...updAssets];

  // turn empty url => use public_url if present
  const cleaned = all.map(a => ({
    id          : a.id,
    name        : a.name,
    public_url  : a.public_url || a.url,
    file_extension: (a.file_extension || a.name.split('.').pop() || '').toLowerCase()
  }));

  return cleaned.filter(a => a.public_url &&
                             (a.file_extension === 'pdf' ||
                              a.name.toLowerCase().endsWith('.pdf')));
}

// 4-B  Instabase batch ▸ run ▸ results
async function runInstabase(files){
  const batch = await axios.post(
    `${INSTABASE_CONFIG.baseUrl}/api/v2/batches`,
    { workspace:'nileshn_sturgeontire.com' },
    { headers: INSTABASE_CONFIG.headers }
  );
  const batchId = batch.data.id;

  const originals = [];
  for (const f of files){
    const buf = Buffer.from((await axios.get(f.public_url,{responseType:'arraybuffer'})).data);
    originals.push({ name:f.name, buffer:buf });
    await axios.put(
      `${INSTABASE_CONFIG.baseUrl}/api/v2/batches/${batchId}/files/${encodeURIComponent(f.name)}`,
      buf,
      { headers:{ ...INSTABASE_CONFIG.headers, 'Content-Type':'application/octet-stream'} }
    );
  }

  const run = await axios.post(
    `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/deployments/${INSTABASE_CONFIG.deploymentId}/runs`,
    { batch_id: batchId },
    { headers: INSTABASE_CONFIG.headers }
  );
  const runId = run.data.id;

  /* poll ≤5 min */
  let status='RUNNING', tries=0;
  while(['RUNNING','PENDING'].includes(status) && tries<60){
    await new Promise(r=>setTimeout(r,5000));
    status = (await axios.get(
      `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${runId}`,
      { headers: INSTABASE_CONFIG.headers }
    )).data.status;
    tries++;
  }
  if(status!=='COMPLETE') throw new Error('Instabase run failed: '+status);

  const res = await axios.get(
    `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${runId}/results`,
    { headers: INSTABASE_CONFIG.headers }
  );
  return { files:res.data.files, originals };
}

// 4-C  push rows → “Extracted Docs”
async function pushResultsToMonday(docs, originals){
  /* board metadata once */
  const meta = await gql(`query{ boards(ids:[${MONDAY_CONFIG.extractedDocsBoardId}]){
                               columns{id title type} }}`);
  const cols = meta.boards[0].columns;
  const numCol   = cols.find(c=>/document number/i.test(c.title));
  const fileCol  = cols.find(c=>c.type==='file');

  for(const d of docs){
    const vals = { [numCol.id]: d.invoice_number };
    const create = await gql(
      `mutation($vals:JSON!){ create_item(board_id:${MONDAY_CONFIG.extractedDocsBoardId},
                  item_name:"${d.document_type} ${d.invoice_number}",
                  column_values:$vals){ id } }`,
      { vals: JSON.stringify(vals) }
    );
    const itemId = create.create_item.id;
    console.log('➕  Row created', itemId);

    if (fileCol && originals.length){
      const { buffer,name } = originals[0];
      await uploadPdf(itemId, buffer, name, fileCol.id);
    }
  }
}

// 4-D  upload helper (new endpoint /v2/file, logs full 400 body on failure)
async function uploadPdf(itemId, buf, name, colId){
  const ops = {
    query:`mutation add($file:File!,$item:Int!,$col:String!){
             add_file_to_column(file:$file,item_id:$item,column_id:$col){id}}`,
    variables:{ file:null, item:Number(itemId), col:colId }
  };
  const form = new FormData();
  form.append('operations',JSON.stringify(ops));
  form.append('map',JSON.stringify({ '0':['variables.file'] }));
  form.append('0',buf,{filename:name,contentType:'application/pdf'});

  const res = await axios.post('https://api.monday.com/v2/file',form,
                { headers:{ ...form.getHeaders(), Authorization:`Bearer ${MONDAY_CONFIG.apiKey}` } })
                .catch(err=>{
                  console.error('🟥 monday file-upload 400\n',
                                JSON.stringify(err.response?.data||err, null, 2));
                  throw err;
                });
  console.log('📎  PDF attached', res.data.data.add_file_to_column.id);
}

// ───────────────────────────── 5. MISC ────────────────────────────────────────
app.get('/health',(req,res)=>res.json({status:'ok',ts:Date.now()}));
const PORT = process.env.PORT||8080;
app.listen(PORT,'0.0.0.0',()=>console.log('🚀 webhook running on',PORT));

// make testable
module.exports = app;
