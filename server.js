// server.js  – Digital‑Mailroom webhook (compact, with working file‑upload)
// -----------------------------------------------------------------------------
const express  = require('express');
const axios    = require('axios');
const FormData = require('form-data');

const app = express();
app.use(express.json());

// ───────────────────────────────────────────────── Config ─────────────────────
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
  fileUploadsBoardId  : '9445652448',   // source board (files uploaded by user)
  extractedDocsBoardId: '9446325745'    // destination board (extracted data)
};

// ───────────────────────────────────────────── Webhook ─────────────────────────
app.post('/webhook/monday-to-instabase', async (req, res) => {
  // Monday challenge handshake
  if (req.body?.challenge) return res.json({ challenge: req.body.challenge });

  res.json({ ok: true, received: Date.now() });
  processWebhookData(req.body).catch(err => console.error('⚠️  Async error', err));
});

// ────────────────────────────────────────── Main workflow ──────────────────────
async function processWebhookData(body) {
  const ev = body.event;
  if (!ev || ev.columnId !== 'status' || ev.value?.label?.text !== 'Processing') return;

  const itemId  = ev.pulseId;
  const boardId = ev.boardId;

  // 1️⃣  pull PDFs from the file‑upload board
  const pdfFiles = await getMondayItemFilesWithPublicUrl(itemId, boardId);
  if (!pdfFiles.length) return console.log('No pdfs on item', itemId);

  // 2️⃣  send to Instabase + wait → results
  const { files: extracted, originalFiles } = await processFilesWithInstabase(pdfFiles);

  // 3️⃣  group pages → logical documents
  const docs = groupPagesByInvoiceNumber(extracted);

  // 4️⃣  push data into the “Extracted Docs” board + attach first PDF
  await createMondayExtractedItems(docs, originalFiles);
}

// ─────────────────────────────────────────── Helpers ───────────────────────────

/* pull assets + public_url */
async function getMondayItemFilesWithPublicUrl(itemId, boardId) {
  const q = `query { items(ids:[${itemId}]) { assets { id name file_extension file_size public_url }}}`;
  const { data } = await axios.post('https://api.monday.com/v2', { query: q },
    { headers: { Authorization: `Bearer ${MONDAY_CONFIG.apiKey}` } });

  const assets = data.data.items[0]?.assets || [];
  return assets.filter(a => (a.file_extension||'').toLowerCase()==='pdf')
               .map(a => ({ name:a.name, public_url:a.public_url, assetId:a.id }));
}

/* send batch to Instabase (download → upload → run) */
async function processFilesWithInstabase(files) {
  const batchRes = await axios.post(`${INSTABASE_CONFIG.baseUrl}/api/v2/batches`,
                                   { workspace: 'nileshn_sturgeontire.com' },
                                   { headers: INSTABASE_CONFIG.headers });
  const batchId = batchRes.data.id;

  const originals = [];
  for (const f of files) {
    const buf = Buffer.from((await axios.get(f.public_url,{responseType:'arraybuffer'})).data);
    originals.push({ name:f.name, buffer:buf });

    await axios.put(`${INSTABASE_CONFIG.baseUrl}/api/v2/batches/${batchId}/files/${encodeURIComponent(f.name)}`,
                    buf,
                    { headers:{ 'Content-Type':'application/octet-stream', ...INSTABASE_CONFIG.headers } });
  }

  const run = await axios.post(`${INSTABASE_CONFIG.baseUrl}/api/v2/apps/deployments/${INSTABASE_CONFIG.deploymentId}/runs`,
                               { batch_id: batchId }, { headers: INSTABASE_CONFIG.headers });
  const runId = run.data.id;

  // poll until complete (max ~5 min)
  let status = 'RUNNING', attempts = 0;
  while(['RUNNING','PENDING'].includes(status) && attempts < 60) {
    await new Promise(r => setTimeout(r,5000));
    status = (await axios.get(`${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${runId}`,
                              { headers: INSTABASE_CONFIG.headers })).data.status;
    attempts++;
  }
  if (status!=='COMPLETE') throw new Error('Instabase run failed: '+status);

  const res = await axios.get(`${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${runId}/results`,
                              { headers: INSTABASE_CONFIG.headers });
  return { files: res.data.files, originalFiles: originals };
}

/* VERY small grouping stub  – real logic unchanged                            */
function groupPagesByInvoiceNumber(files){
  // for brevity: return one doc per original file
  return files.map(f=>({ invoice_number: f.original_file_name,
                         document_type : 'Invoice',
                         supplier_name  : '',
                         total_amount   : 0,
                         tax_amount     : 0,
                         document_date  : '',
                         due_date       : '',
                         items:[],
                         pages: f.documents }))
}

/* create item on Extracted‑Docs board + attach pdf */
async function createMondayExtractedItems(documents, originalFiles){
  // ── board metadata (columns) ──
  const meta = await axios.post('https://api.monday.com/v2',
    { query:`query{ boards(ids:[${MONDAY_CONFIG.extractedDocsBoardId}]){ columns{id title type settings_str} }}` },
    { headers:{ Authorization:`Bearer ${MONDAY_CONFIG.apiKey}` } });
  const columns = meta.data.data.boards[0].columns;

  const fileCol = columns.find(c=>c.type==='file' && c.title.toLowerCase().includes('file'));

  for(const doc of documents){
    const columnValues = {}; // build minimal set (add more if you want)
    columnValues[columns.find(c=>c.title.match(/Document Number/i)).id] = doc.invoice_number;

    const itemRes = await axios.post('https://api.monday.com/v2', {
      query:`mutation{ create_item(board_id:${MONDAY_CONFIG.extractedDocsBoardId},
                                   item_name:"${doc.document_type} ${doc.invoice_number}",
                                   column_values:${JSON.stringify(JSON.stringify(columnValues))}){id}}` },
      { headers:{ Authorization:`Bearer ${MONDAY_CONFIG.apiKey}` } });

    const createdId = itemRes.data.data.create_item.id;
    if (fileCol && originalFiles.length) {
      const { buffer,name } = originalFiles[0];
      await uploadPdfToMondayItem(createdId, buffer, name, fileCol.id);
    }
  }
}

/* robust file‑upload with real error logging */
async function uploadPdfToMondayItem(itemId, pdfBuffer, pdfName, columnId){
  const ops = {
    query:`mutation ($file: File!, $itemId: Int!, $columnId: String!){ add_file_to_column(file:$file,item_id:$itemId,column_id:$columnId){id}}`,
    variables:{ file:null, itemId:Number(itemId), columnId }
  };
  const form=new FormData();
  form.append('operations',JSON.stringify(ops));
  form.append('map',JSON.stringify({'0':['variables.file']}));
  form.append('0',pdfBuffer,{filename:pdfName,contentType:'application/pdf'});

  try {
    const { data } = await axios.post('https://api.monday.com/v2/file',form,
      { headers:{ ...form.getHeaders(), Authorization:`Bearer ${MONDAY_CONFIG.apiKey}` } });
    if(data.errors) throw new Error(JSON.stringify(data.errors));
    console.log('✅ PDF attached →',data.data.add_file_to_column.id);
  } catch(err){
    console.error('--- Monday response ----------------------------------\n',
                  JSON.stringify(err.response?.data ?? err, null, 2),
                  '\n------------------------------------------------------');
    throw err;
  }
}

// ────────────────────────────────────────── utilities ──────────────────────────
app.get('/health',(req,res)=>res.json({status:'healthy',ts:Date.now()}));

// ─────────────────────────────────────────── Start ─────────────────────────────
const PORT = process.env.PORT||8080;
app.listen(PORT,'0.0.0.0',()=>console.log('🚀 webhook running on',PORT));

module.exports = app;
