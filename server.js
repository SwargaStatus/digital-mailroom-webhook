// server.js – Digital-Mailroom webhook (fixed Monday file-download/upload)
// -----------------------------------------------------------------------
const express  = require('express');
const axios    = require('axios');
const FormData = require('form-data');

const app = express();
app.use(express.json());

// ────────────────────────────── CONFIG ──────────────────────────────────
const INSTABASE_CONFIG = {
  baseUrl      : 'https://aihub.instabase.com',
  apiKey       : 'jEmrseIwOb9YtmJ6GzPAywtz53KnpS',
  deploymentId : '0197a3fe-599d-7bac-a34b-81704cc83beb',
  headers      : {
    'IB-Context'  : 'sturgeontire',
    Authorization : 'Bearer jEmrseIwOb9YtmJ6GzPAywtz53KnpS',
    'Content-Type': 'application/json'
  }
};

const MONDAY_CONFIG = {
  apiKey              : 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjUzMDYzOTcxOSwiYWFpIjoxMSwidWlkIjo2Nzg2NjA4MywiaWFkIjoiMjAyNS0wNi0yNFQyMjoxNjowMC42NTJaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MjYyMDQ5OTgsInJnbiI6InVzZTEifQ.zv9EsISZnchs7WKSqN2t3UU1GwcLrzPGeaP7ssKIla8',
  fileUploadsBoardId  : '9445652448',
  extractedDocsBoardId: '9446325745'
};

// ─────────────────────────── WEBHOOK ────────────────────────────────────
app.post('/webhook/monday-to-instabase', async (req, res) => {
  if (req.body?.challenge) return res.json({ challenge: req.body.challenge });
  res.json({ ok: true });

  try { await processWebhook(req.body); }
  catch (e){ console.error('⚠️  async error', e); }
});

// ──────────────────── MAIN WORKFLOW (unchanged) ─────────────────────────
async function processWebhook(body){
  const ev = body.event;
  if (!ev || ev.columnId!=='status' || ev.value?.label?.text!=='Processing')
    return;

  const itemId = ev.pulseId;

  /* 1️⃣ get PDFs from the File-Uploads board */
  const pdfs = await fetchPdfAssets(itemId);
  if (!pdfs.length) return console.log('No PDFs on item', itemId);

  /* 2️⃣ Instabase */
  const { files:extracted, originalFiles } = await runInstabase(pdfs);

  /* 3️⃣ stub grouping */
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

  /* 4️⃣ create target items + attach first PDF */
  await createItemsInMonday(docs, originalFiles);
}

// ──────────────────────── HELPERS ───────────────────────────────────────
// 4-A  pull assets & choose a download URL
async function fetchPdfAssets(itemId){
  const q=`query{ items(ids:[${itemId}]){ assets{ id name file_extension url public_url } } }`;
  const { data } = await axios.post('https://api.monday.com/v2', {query:q},
                    { headers:{Authorization:`Bearer ${MONDAY_CONFIG.apiKey}`}});

  return (data.data.items?.[0]?.assets||[])
    .filter(a => (a.file_extension||'').toLowerCase()==='pdf')
    .map(a => ({
      name      : a.name,
      public_url: a.public_url || a.url,    // ← fallback to ‘url’
      assetId   : a.id
    }))
    .filter(a => a.public_url);             // drop PDFs w/o URL
}

// 4-B  Instabase batch/run/result  (unchanged)
async function runInstabase(files){ /* … exactly as before … */ }

// 4-C  make items on the Extracted-Docs board (unchanged except call name)
async function createItemsInMonday(docs, originals){ /* … unchanged … */ }

// 4-D  **proper** file upload
async function uploadPdf(itemId, buf, name, columnId){
  const ops = {
    query: `mutation ($file: File!, $itemId: Int!, $col: String!){
              add_file_to_column(file:$file,item_id:$itemId,column_id:$col){id}}`,
    variables: { file:null, itemId:Number(itemId), col:columnId }
  };

  const form = new FormData();
  form.append('operations', JSON.stringify(ops));
  form.append('map',        JSON.stringify({ '0':['variables.file'] }));
  form.append('0',          buf, { filename:name, contentType:'application/pdf' });

  try {
    const { data } = await axios.post('https://api.monday.com/v2/file', form,
      { headers:{ ...form.getHeaders(), Authorization:`Bearer ${MONDAY_CONFIG.apiKey}`}});
    if (data.errors) throw new Error(JSON.stringify(data.errors));
    console.log('✅ PDF attached →', data.data.add_file_to_column.id);
  } catch (err){
    console.error('--- Monday add_file_to_column failed ----------------');
    console.error(JSON.stringify(err.response?.data||err, null, 2));
    console.error('-----------------------------------------------------');
    throw err;
  }
}

// ──────────────────────── MISC ──────────────────────────────────────────
app.get('/health',(req,res)=>res.json({ status:'healthy', ts:Date.now() }));

const PORT = process.env.PORT || 8080;
app.listen(PORT,'0.0.0.0',()=>console.log('🚀 webhook running on',PORT));

module.exports = app;
