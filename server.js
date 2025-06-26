/*  server.js – Digital-Mailroom webhook (stable June-25-2025 build)
   ───────────────────────────────────────────────────────────────── */

const express  = require('express');
const axios    = require('axios');
const FormData = require('form-data');
const app      = express();
app.use(express.json());

/* ───── 1. CONFIG – hard-coded as requested ────────────────────── */
const INSTABASE = {
  base : 'https://aihub.instabase.com',
  key  : 'jEmrseIwOb9YtmJ6GzPAywtz53KnpS',
  dep  : '0197a3fe-599d-7bac-a34b-81704cc83beb',
  hdr  : {
    'IB-Context'  : 'sturgeontire',
    Authorization : 'Bearer jEmrseIwOb9YtmJ6GzPAywtz53KnpS',
    'Content-Type': 'application/json'
  }
};

const MONDAY = {
  key     : 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjUzMDYzOTcxOSwiYWFpIjoxMSwidWlkIjo2Nzg2NjA4MywiaWFkIjoiMjAyNS0wNi0yNFQyMjoxNjowMC42NTJaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MjYyMDQ5OTgsInJnbiI6InVzZTEifQ.zv9EsISZnchs7WKSqN2t3UU1GwcLrzPGeaP7ssKIla8',
  uploads : '9445652448',   // File-Uploads board
  output  : '9446325745'    // Extracted-Docs board
};

/* ───── 2. WEBHOOK ──────────────────────────────────────────────── */
app.post('/webhook/monday-to-instabase', async (req, res) => {
  if (req.body?.challenge) return res.json({ challenge: req.body.challenge });
  res.json({ ok: true });               // respond fast – process later
  try { await processEvent(req.body); }
  catch (e) { console.error('❌ async error', e); }
});

/* ───── 3. MAIN FLOW ────────────────────────────────────────────── */
async function processEvent(body) {
  const ev = body.event;
  if (!ev) return;

  const { pulseId:itemId, columnId, value } = ev;
  if (columnId !== 'status' || value?.label?.text !== 'Processing') return;

  console.log('▶️  Start item', itemId);

  /* 1️⃣  collect PDFs (assets + file-column + update attachments) */
  const pdfAssets = await collectPdfAssets(itemId);
  if (!pdfAssets.length) { console.log('🛈 No PDFs on item', itemId); return; }

  /* 2️⃣  Instabase OCR / extraction */
  const { files: extracted, originals } = await instabaseRun(pdfAssets);

  /* 3️⃣  Very light grouping (1 doc ⇢ 1 file) – keep your old logic if needed */
  const docs = extracted.map(f => ({
    invoice_number : f.original_file_name,
    document_type  : 'Invoice',
    items          : [],
    pages          : f.documents
  }));

  /* 4️⃣  Write to “Extracted Documents” board */
  await writeToMonday(docs, originals);
}

/* ───── 4-A.  Collect PDF assets ────────────────────────────────── */
async function collectPdfAssets(itemId) {
  const gql = q => axios.post(
    'https://api.monday.com/v2',
    { query: q },
    { headers:{ Authorization:`Bearer ${MONDAY.key}` }, timeout: 10000 }
  );

  /* a) item.assets */
  const q1 = `query { items (ids:[${itemId}]) {
                 assets { id name file_extension public_url url }
                 column_values { id type value text }
               } }`;
  const { data } = await gql(q1);
  const item    = data.data.items?.[0] || {};
  const list    = [];

  console.log('   •', item.assets?.length || 0, 'assets in item.assets');
  item.assets?.forEach(a=>console.log('     ·', a.id, a.name));

  (item.assets||[]).forEach(a => list.push(a));

  /* b) any FILE column that embeds an asset but wasn’t in item.assets */
  const fileCols = (item.column_values||[])
    .filter(c => c.type==='file')
    .map(c => { try { return JSON.parse(c.value||'{}'); } catch{return {}; }})
    .flatMap(v => v.files || []);
  console.log('   •', fileCols.length, 'assets in file columns');
  fileCols.forEach(a=>console.log('     ·', a.assetId, a.fileName));
  list.push(...fileCols.map(f => ({
    id:f.assetId, name:f.fileName, file_extension:f.fileExtension
  })));

  /* c) assets attached to updates */
  const qUpdates = `query { updates (item_id:${itemId}) { assets {
                     id name file_extension public_url url } } }`;
  const upRes = await gql(qUpdates);
  const updAssets = upRes.data.data.updates.flatMap(u => u.assets || []);
  console.log('   •', updAssets.length, 'assets in updates');
  updAssets.forEach(a=>console.log('     ·', a.id, a.name));
  list.push(...updAssets);

  /* d) de-duplicate on id */
  const uniq = Object.values(
    list.reduce((acc,a)=>{ acc[a.id]=acc[a.id]||a; return acc;}, {})
  ).filter(a => (a.file_extension||'').toLowerCase()==='pdf');

  /* e) ensure we have a usable URL */
  const needURL = uniq.filter(a => !(a.public_url||a.url));
  if (needURL.length) {
    const ids = needURL.map(a=>a.id).join(',');
    const q2 = `query { assets (ids:[${ids}]) { id public_url url } }`;
    const { data:{data:{assets}} } = await gql(q2);
    const map = Object.fromEntries(assets.map(a=>[a.id, a]));
    needURL.forEach(a => Object.assign(a, map[a.id]||{}));
  }

  const ready = uniq.filter(a => a.public_url || a.url)
                    .map(a => ({
                      name       : a.name,
                      public_url : a.public_url || a.url,
                      id         : a.id
                    }));

  console.log('   →', ready.length, 'PDF(s) ready');
  return ready;
}

/* ───── 4-B.  Instabase run ─────────────────────────────────────── */
async function instabaseRun(files) {
  const ib = INSTABASE;

  /* batch */
  console.log('☁️  Instabase - create batch');
  const batch = await axios.post(`${ib.base}/api/v2/batches`,
                { workspace:'nileshn_sturgeontire.com' },
                { headers: ib.hdr, timeout:10000 });
  const batchId = batch.data.id;

  const originals = [];

  /* upload */
  for (const f of files) {
    console.log('   ↑ uploading', f.name);
    const buf = Buffer.from((await axios.get(f.public_url,
                     { responseType:'arraybuffer', timeout:20000 })).data);
    originals.push({ name:f.name, buffer:buf });

    await axios.put(
      `${ib.base}/api/v2/batches/${batchId}/files/${encodeURIComponent(f.name)}`,
      buf,
      { headers:{ ...ib.hdr, 'Content-Type':'application/octet-stream' },
        timeout: 30000 }
    );
  }

  /* run */
  const runRes = await axios.post(
    `${ib.base}/api/v2/apps/deployments/${ib.dep}/runs`,
    { batch_id:batchId },
    { headers: ib.hdr, timeout:10000 }
  );
  const runId = runRes.data.id;

  /* poll */
  let status='RUNNING', n=0;
  while(['RUNNING','PENDING'].includes(status) && n<60){
    await new Promise(r=>setTimeout(r,5000));
    status = (await axios.get(`${ib.base}/api/v2/apps/runs/${runId}`,
              { headers:ib.hdr, timeout:10000 })).data.status;
    n++;
  }
  if(status!=='COMPLETE') throw new Error('Instabase run failed: '+status);

  const res = await axios.get(`${ib.base}/api/v2/apps/runs/${runId}/results`,
              { headers:ib.hdr, timeout:20000 });
  return { files: res.data.files, originals };
}

/* ───── 4-C.  Write results to monday board ─────────────────────── */
async function writeToMonday(docs, originals){
  const auth = { Authorization:`Bearer ${MONDAY.key}` };

  /* board columns */
  const meta = await axios.post('https://api.monday.com/v2',
    { query:`query{ boards(ids:[${MONDAY.output}]){ columns{id title type} }}`},
    { headers:auth, timeout:10000 });
  const cols = meta.data.data.boards[0].columns;
  const numCol = cols.find(c=>/Document Number/i.test(c.title));
  const fileCol= cols.find(c=>c.type==='file');

  for(const d of docs){
    const vals = { [numCol.id]: d.invoice_number };

    const create = await axios.post('https://api.monday.com/v2',{
      query:`mutation{ create_item(board_id:${MONDAY.output},
        item_name:"${d.document_type} ${d.invoice_number}",
        column_values:${JSON.stringify(JSON.stringify(vals))}){id}}`
    },{ headers:auth, timeout:10000 });

    const itemId = create.data.data.create_item.id;

    if(fileCol && originals.length){
      await uploadFileToMonday(itemId, originals[0], fileCol.id);
    }
  }
}

/* ───── 4-D.  File upload helper ────────────────────────────────── */
async function uploadFileToMonday(itemId, {buffer,name}, colId){
  const ops = {
    query:`mutation ($file: File!, $itemId: Int!, $col: String!){
             add_file_to_column(file:$file,item_id:$itemId,column_id:$col){id}}`,
    variables:{ file:null, itemId:Number(itemId), col:colId }
  };
  const form = new FormData();
  form.append('operations', JSON.stringify(ops));
  form.append('map', JSON.stringify({ '0':['variables.file'] }));
  form.append('0', buffer, { filename:name, contentType:'application/pdf' });

  const { data } = await axios.post(
    'https://api.monday.com/v2/file',
    form,
    { headers:{ ...form.getHeaders(), Authorization:`Bearer ${MONDAY.key}` },
      timeout:20000 }
  );
  if(data.errors) throw new Error(JSON.stringify(data.errors));
  console.log('📎 PDF attached →', data.data.add_file_to_column.id);
}

/* ───── 5. misc ─────────────────────────────────────────────────── */
app.get('/health',(req,res)=>res.json({ status:'healthy', ts:Date.now() }));
const PORT = process.env.PORT||8080;
app.listen(PORT,'0.0.0.0',()=>console.log('🚀 webhook on',PORT));

module.exports = app;
