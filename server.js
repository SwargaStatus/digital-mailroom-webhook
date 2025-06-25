// server.js  – Digital-Mailroom webhook (compact, with working file-upload)
// ---------------------------------------------------------------------------
const express  = require('express');
const axios    = require('axios');
const FormData = require('form-data');

const app = express();
app.use(express.json());

// ────────────────────────────── 1.  CONFIG  ───────────────────────────────────
const INSTABASE_CONFIG = {
  baseUrl      : 'https://aihub.instabase.com',
  apiKey       : 'jEmrseIwOb9YtmJ6GzPAywtz53KnpS',
  deploymentId : '0197a3fe-599d-7bac-a34b-81704cc83beb',
  headers      : {
    'IB-Context'   : 'sturgeontire',
    Authorization  : 'Bearer jEmrseIwOb9YtmJ6GzPAywtz53KnpS',
    'Content-Type' : 'application/json'
  }
};

const MONDAY_CONFIG = {
  apiKey              : 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjUzMDYzOTcxOSwiYWFpIjoxMSwidWlkIjo2Nzg2NjA4MywiaWFkIjoiMjAyNS0wNi0yNFQyMjoxNjowMC42NTJaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MjYyMDQ5OTgsInJnbiI6InVzZTEifQ.zv9EsISZnchs7WKSqN2t3UU1GwcLrzPGeaP7ssKIla8',
  fileUploadsBoardId  : '9445652448',   // where users drop PDFs
  extractedDocsBoardId: '9446325745'    // where we write the results
};

// ───────────────────────────── 2.  WEBHOOK  ───────────────────────────────────
app.post('/webhook/monday-to-instabase', async (req, res) => {
  if (req.body?.challenge) return res.json({ challenge: req.body.challenge }); // handshake
  res.json({ ok: true, received: Date.now() });                                 // 200 fast

  try   { await processWebhook(req.body); }
  catch (e){ console.error('⚠️  async error', e); }
});

// ─────────────────────────── 3. MAIN WORKFLOW ─────────────────────────────────
async function processWebhook(body) {
  const ev = body.event;
  if (!ev || ev.columnId !== 'status' || ev.value?.label?.text !== 'Processing') return;

  const itemId = ev.pulseId;

  /* 1️⃣  download PDFs from the “File Uploads” board */
  const pdfs = await fetchPdfAssets(itemId, MONDAY_CONFIG.fileUploadsBoardId);
  if (!pdfs.length) return console.log('No pdfs on item', itemId);

  /* 2️⃣  Instabase → structured data */
  const { files: extracted, originalFiles } = await runInstabase(pdfs);

  /* 3️⃣  very small grouping stub (same output schema you had) */
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

  /* 4️⃣  create items on “Extracted Docs” + attach first PDF */
  await createItemsInMonday(docs, originalFiles);
}

// ──────────────────────────── 4.  HELPERS  ────────────────────────────────────
/* 4-A  Pull assets + real download URL (public_url OR url) */
async function fetchPdfAssets(itemId, boardId){
  // first call → list of asset IDs
  const q = `query{ items(ids:[${itemId}]){ assets{id name file_extension} }}`;
  const { data } = await axios.post('https://api.monday.com/v2',{query:q},
                   { headers:{Authorization:`Bearer ${MONDAY_CONFIG.apiKey}`}});
  const pdfIds = (data.data.items?.[0]?.assets||[])
                 .filter(a => (a.file_extension||'').toLowerCase()==='pdf')
                 .map(a => ({ id:a.id, name:a.name }));

  if (!pdfIds.length) return [];

  // second call → signed URLs
  const ids = pdfIds.map(a=>a.id).join(',');
  const q2 = `query{ assets(ids:[${ids}]){ id public_url url file_extension } }`;
  const { data: d2 } = await axios.post('https://api.monday.com/v2',{query:q2},
                         { headers:{Authorization:`Bearer ${MONDAY_CONFIG.apiKey}`}});
  const urlMap = Object.fromEntries(
    d2.data.assets.map(a => [a.id, a.public_url || a.url])
  );

  return pdfIds
    .map(a => ({ name:a.name, public_url:urlMap[a.id], assetId:a.id }))
    .filter(a => a.public_url);  // drop any that still have no URL
}

/* 4-B  Send to Instabase (batch → run → results) */
async function runInstabase(files){
  const batch = await axios.post(`${INSTABASE_CONFIG.baseUrl}/api/v2/batches`,
                                 { workspace:'nileshn_sturgeontire.com' },
                                 { headers:INSTABASE_CONFIG.headers });
  const batchId = batch.data.id;

  const originals = [];
  for (const f of files){
    const buf = Buffer.from((await axios.get(f.public_url,{responseType:'arraybuffer'})).data);
    originals.push({ name:f.name, buffer:buf });

    await axios.put(`${INSTABASE_CONFIG.baseUrl}/api/v2/batches/${batchId}/files/${encodeURIComponent(f.name)}`,
                    buf,{ headers:{...INSTABASE_CONFIG.headers,'Content-Type':'application/octet-stream'}});
  }

  const run = await axios.post(`${INSTABASE_CONFIG.baseUrl}/api/v2/apps/deployments/${INSTABASE_CONFIG.deploymentId}/runs`,
                               { batch_id:batchId },{ headers:INSTABASE_CONFIG.headers });
  const runId = run.data.id;

  /* poll status (≤5 min) */
  let status='RUNNING', n=0;
  while(['RUNNING','PENDING'].includes(status) && n<60){
    await new Promise(r=>setTimeout(r,5000));
    status = (await axios.get(`${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${runId}`,
                              { headers:INSTABASE_CONFIG.headers })).data.status;
    n++;
  }
  if(status!=='COMPLETE') throw new Error('Instabase run failed: '+status);

  const res = await axios.get(`${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${runId}/results`,
                              { headers:INSTABASE_CONFIG.headers });
  return { files:res.data.files, originalFiles:originals };
}

/* 4-C  Create item on “Extracted Docs” board and attach PDF */
async function createItemsInMonday(docs, originalFiles){
  // board metadata
  const meta = await axios.post('https://api.monday.com/v2',
    { query:`query{ boards(ids:[${MONDAY_CONFIG.extractedDocsBoardId}]){ columns{id title type} }}`},
    { headers:{Authorization:`Bearer ${MONDAY_CONFIG.apiKey}`}} );
  const cols = meta.data.data.boards[0].columns;
  const numCol = cols.find(c=>/Document Number/i.test(c.title));
  const fileCol= cols.find(c=>c.type==='file');

  for(const d of docs){
    const vals = { [numCol.id]: d.invoice_number };
    const create = await axios.post('https://api.monday.com/v2',{
      query:`mutation{ create_item(board_id:${MONDAY_CONFIG.extractedDocsBoardId},
                                   item_name:"${d.document_type} ${d.invoice_number}",
                                   column_values:${JSON.stringify(JSON.stringify(vals))}){id}}`
    },{ headers:{Authorization:`Bearer ${MONDAY_CONFIG.apiKey}`}});

    const itemId = create.data.data.create_item.id;
    if (fileCol && originalFiles.length){
      const { buffer,name }=originalFiles[0];
      await uploadPdf(itemId, buffer, name, fileCol.id);
    }
  }
}

/* 4-D  Upload helper */
async function uploadPdf(itemId, buf, name, colId){
  const ops = {
    query:`mutation ($file: File!, $itemId: Int!, $col: String!){
             add_file_to_column(file:$file,item_id:$itemId,column_id:$col){id}}`,
    variables:{ file:null, itemId:Number(itemId), col:colId }
  };
  const form=new FormData();
  form.append('operations',JSON.stringify(ops));
  form.append('map',JSON.stringify({'0':['variables.file']}));
  form.append('0',buf,{filename:name,contentType:'application/pdf'});

  const { data } = await axios.post('https://api.monday.com/v2/file',form,
                     { headers:{...form.getHeaders(), Authorization:`Bearer ${MONDAY_CONFIG.apiKey}`}});
  if(data.errors) throw new Error(JSON.stringify(data.errors));
  console.log('✅ PDF attached →',data.data.add_file_to_column.id);
}

// ────────────────────────────── 5.  MISC  ─────────────────────────────────────
app.get('/health',(req,res)=>res.json({status:'healthy',ts:Date.now()}));

const PORT = process.env.PORT||8080;
app.listen(PORT,'0.0.0.0',()=>console.log('🚀 webhook running on',PORT));
