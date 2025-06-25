// Digital‑Mailroom Webhook – CLEANED VERSION
// ───────────────────────────────────────────
// 1) One clear copy of every helper
// 2) No duplicate blocks / undefined variables
// 3) Works with: Instabase + Monday.com
// ------------------------------------------------------------

const express = require('express');
const axios   = require('axios');
const FormData = require('form-data');

const app = express();
app.use(express.json());

/* ───────────────────────── CONFIG ───────────────────────── */
const INSTABASE_CONFIG = {
  baseUrl:       'https://aihub.instabase.com',
  apiKey:        'jEmrseIwOb9YtmJ6GzPAywtz53KnpS',
  deploymentId:  '0197a3fe-599d-7bac-a34b-81704cc83beb',
  headers: {
    'IB-Context':   'sturgeontire',
    'Authorization': 'Bearer jEmrseIwOb9YtmJ6GzPAywtz53KnpS',
    'Content-Type':  'application/json'
  }
};

const MONDAY_CONFIG = {
  apiKey:               'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjUzMDYzOTcxOSwiYWFpIjoxMSwidWlkIjo2Nzg2NjA4MywiaWFkIjoiMjAyNS0wNi0yNFQyMjoxNjowMC42NTJaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MjYyMDQ5OTgsInJnbiI6InVzZTEifQ.zv9EsISZnchs7WKSqN2t3UU1GwcLrzPGeaP7ssKIla8',
  fileUploadsBoardId:   '9445652448',
  extractedDocsBoardId: '9446325745'
};

/* ─────────────────────── UTILITIES ─────────────────────── */
const iso = (d) => {
  if (!d) return '';
  try { return new Date(d).toISOString().slice(0,10); }
  catch { return String(d).slice(0,10); }
};

/* ─────────────────────── WEBHOOK ENTRY ─────────────────── */
app.post('/webhook/monday-to-instabase', async (req, res) => {
  try {
    if (req.body.challenge) {
      // verification ping from Monday
      return res.json({ challenge: req.body.challenge });
    }

    res.json({ success:true, message:'processing', ts:Date.now() });
    processWebhookData(req.body).catch(console.error);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

/* ────────────────────── MAIN WORKFLOW ──────────────────── */
async function processWebhookData(body) {
  const ev = body.event;
  if (!ev || ev.columnId !== 'status' || ev.value?.label?.text !== 'Processing') return;

  const pdfs  = await getMondayItemFilesWithPublicUrl(ev.pulseId, ev.boardId);
  if (!pdfs.length) return;

  const result   = await processFilesWithInstabase(pdfs);
  const grouped  = groupPagesByInvoiceNumber(result.files);
  await createMondayExtractedItems(grouped, result.originalFiles);
}

/* ───────────────────── GET FILES FROM MONDAY ───────────── */
async function getMondayItemFilesWithPublicUrl(itemId, boardId){
  const query = `query { items(ids:[${itemId}]) { assets { id name file_extension file_size public_url } } }`;
  const { data } = await axios.post('https://api.monday.com/v2',{ query },{
    headers:{ Authorization:`Bearer ${MONDAY_CONFIG.apiKey}` }
  });
  const assets = data.data.items?.[0]?.assets || [];
  return assets.filter(a => (a.file_extension||'').toLowerCase()==='pdf' || a.name.toLowerCase().endsWith('.pdf'))
               .map(a => ({ name:a.name, public_url:a.public_url, assetId:a.id }));
}

/* ─────────────── UPLOAD TO INSTABASE & RUN APP ─────────── */
async function processFilesWithInstabase(files){
  // 1) create batch
  const { data:batch } = await axios.post(`${INSTABASE_CONFIG.baseUrl}/api/v2/batches`,
    { workspace:"nileshn_sturgeontire.com" },{ headers:INSTABASE_CONFIG.headers });

  const originals = [];
  for (const f of files){
    const buf = (await axios.get(f.public_url,{responseType:'arraybuffer'})).data;
    originals.push({ name:f.name, buffer:Buffer.from(buf) });
    await axios.put(`${INSTABASE_CONFIG.baseUrl}/api/v2/batches/${batch.id}/files/${f.name}`, buf, {
      headers:{...INSTABASE_CONFIG.headers, 'Content-Type':'application/octet-stream'} });
  }

  // 2) start run
  const { data:run } = await axios.post(
    `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/deployments/${INSTABASE_CONFIG.deploymentId}/runs`,
    { batch_id: batch.id }, { headers:INSTABASE_CONFIG.headers });

  // 3) poll until complete (max 5 min)
  for (let i=0;i<60;i++){
    await new Promise(r=>setTimeout(r,5000));
    const { data:status } = await axios.get(`${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${run.id}`,
      { headers:INSTABASE_CONFIG.headers });
    if (status.status==='COMPLETE') break;
    if (['ERROR','FAILED'].includes(status.status)) throw new Error('Instabase failed');
  }

  // 4) fetch results
  const { data:results } = await axios.get(`${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${run.id}/results`,
    { headers:INSTABASE_CONFIG.headers });

  return { files:results.files, originalFiles:originals };
}

/* ──────────── GROUP PAGES BY INVOICE NUMBER ───────────── */
function groupPagesByInvoiceNumber(files){
  const map = {};
  for (const f of files){
    for (const doc of f.documents){
      const inv = doc.fields['0']?.value || 'unknown';
      if (inv==='unknown') continue;
      if (!map[inv]) map[inv] = {
        invoice_number:inv, document_type:doc.fields['2']?.value||'Invoice',
        supplier_name:doc.fields['3']?.value||'', total_amount:0, tax_amount:0,
        document_date:doc.fields['5']?.value||'', items:[], pages:[],
      };
      map[inv].pages.push({ file:f.original_file_name, page_type:doc.fields['1']?.value||'' });
      // main‑page numeric totals
      if (doc.fields['1']?.value==='main'){
        map[inv].total_amount = parseFloat(String(doc.fields['8']?.value||'0').replace(/[^0-9.\-]/g,''))||0;
        map[inv].tax_amount   = parseFloat(String(doc.fields['9']?.value||'0').replace(/[^0-9.\-]/g,''))||0;
      }
    }
  }
  return Object.values(map);
}

/* ───────────── CREATE ITEMS + ATTACH PDF ON MONDAY ─────── */
async function createMondayExtractedItems(docs, originals){
  // fetch once the destination‐board columns
  const { data } = await axios.post('https://api.monday.com/v2',{
    query:`query{ boards(ids:[${MONDAY_CONFIG.extractedDocsBoardId}]){ columns{ id title type settings_str } } }`},
    { headers:{ Authorization:`Bearer ${MONDAY_CONFIG.apiKey}` }});
  const cols = data.data.boards[0].columns;
  const fileCol = cols.find(c=>['document file','file'].some(t=>c.title.toLowerCase().includes(t)));

  for (const d of docs){
    const colVals = buildColumnValues(d, cols);
    const create = await axios.post('https://api.monday.com/v2',{
      query:`mutation{ create_item(board_id:${MONDAY_CONFIG.extractedDocsBoardId}, item_name:"${d.document_type.toUpperCase()} ${d.invoice_number}", column_values:${JSON.stringify(JSON.stringify(colVals))}){ id } }`},
      { headers:{ Authorization:`Bearer ${MONDAY_CONFIG.apiKey}` }});
    const itemId = create.data.data.create_item.id;

    // attach first PDF
    if (fileCol && originals.length){
      const { buffer,name } = originals[0];
      await uploadPdfToMondayItem(itemId, buffer, name, fileCol.id);
    }
  }
}

function buildColumnValues(doc, cols){
  const out = {};
  for (const c of cols){
    const t=c.title.toLowerCase();
    if (t.includes('supplier')) out[c.id]=doc.supplier_name;
    else if (t.includes('document number')) out[c.id]=doc.invoice_number;
    else if (t.includes('document type')) out[c.id]=doc.document_type;
    else if (t.includes('document date') && !t.includes('due')) out[c.id]=iso(doc.document_date);
    else if (t==='due date') out[c.id]='';
    else if (t.includes('total amount')) out[c.id]=doc.total_amount;
    else if (t.includes('tax amount'))   out[c.id]=doc.tax_amount;
    else if (t.includes('extraction status')) out[c.id]={ index:1 };
  }
  return out;
}

async function uploadPdfToMondayItem(itemId, buf, filename, colId){
  const ops = {
    query:`mutation ($file:File!,$item:Int!,$col:String!){ add_file_to_column(file:$file,item_id:$item,column_id:$col){ id } }`,
    variables:{ file:null, item:itemId, col:colId }
  };
  const map = { '0':[ 'variables.file' ] };
  const form = new FormData();
  form.append('operations',JSON.stringify(ops));
  form.append('map',JSON.stringify(map));
  form.append('0',buf,{ filename, contentType:'application/pdf' });

  await axios.post('https://api.monday.com/v2/file',form,{ headers:{ ...form.getHeaders(), Authorization:`Bearer ${MONDAY_CONFIG.apiKey}` }});
  console.log('PDF attached to Monday item', itemId);
}

/* ──────────────── HEALTH / TEST ROUTES ─────────────────── */
app.get('/health',(req,res)=>res.json({ ok:true, ts:Date.now() }));

/* ─────────────────────── STARTUP ───────────────────────── */
const PORT = process.env.PORT||3000;
app.listen(PORT,'0.0.0.0',()=>console.log('🚀 webhook running on',PORT));
