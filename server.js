// server.js  – Digital-Mailroom webhook (2025-06-25 “clarity” build)
// ───────────────────────────────────────────────────────────────────
const express  = require('express');
const axios    = require('axios');
const FormData = require('form-data');
const app = express();
app.use(express.json());

// ─── 1. CONFIG –– KEEP YOUR TOKENS SAFE IN REAL LIFE ──────────────
const INSTABASE = {
  base : 'https://aihub.instabase.com',
  key  : 'jEmrseIwOb9YtmJ6GzPAywtz53KnpS',
  dep  : '0197a3fe-599d-7bac-a34b-81704cc83beb',
  hdr  : {
    'IB-Context':'sturgeontire',
    Authorization:'Bearer jEmrseIwOb9YtmJ6GzPAywtz53KnpS',
    'Content-Type':'application/json'
  }
};
const MONDAY = {
  key  : 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjUzMDYzOTcxOSwiYWFpIjoxMSwidWlkIjo2Nzg2NjA4MywiaWFkIjoiMjAyNS0wNi0yNFQyMjoxNjowMC42NTJaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MjYyMDQ5OTgsInJnbiI6InVzZTEifQ.zv9EsISZnchs7WKSqN2t3UU1GwcLrzPGeaP7ssKIla8',
  src  : '9445652448', // File Uploads
  dst  : '9446325745'  // Extracted Documents
};
const AUTH_HDR = { Authorization:`Bearer ${MONDAY.key}` };

// tiny helper
const gql = async (q, vars={}) => {
  const r = await axios.post('https://api.monday.com/v2', {query:q,variables:vars},{headers:AUTH_HDR});
  if (r.data.errors) throw new Error(JSON.stringify(r.data.errors,null,2));
  return r.data.data;
};

// ─── 2. WEBHOOK ────────────────────────────────────────────────────
app.post('/webhook/monday-to-instabase', (req,res)=>{
  if (req.body?.challenge) return res.json({challenge:req.body.challenge});
  res.json({ok:true});
  processEvent(req.body).catch(e=>console.error('💥',e));
});

// ─── 3. EVENT → PIPELINE ───────────────────────────────────────────
async function processEvent(body){
  const ev = body.event;
  if(!ev || ev.columnId!=='status' || ev.value?.label?.text!=='Processing') return;

  const itemId = ev.pulseId;
  console.log('▶️  Start item', itemId);

  // 3-A  pull PDFs
  const pdfs = await collectPdfAssets(itemId);
  if(!pdfs.length) { console.log('🚫  No valid PDFs – abort'); return; }

  // 3-B  Instabase
  const {files, originals} = await instabaseRun(pdfs);

  // 3-C  trivial doc object (one per file)
  const docs = files.map(f=>({
    invoice_number : f.original_file_name,
    document_type  : 'Invoice',
    supplier_name  : '',
    total_amount   : 0,  tax_amount:0,
    document_date  : '', due_date : '',
    items:[], pages:f.documents
  }));

  // 3-D  push to monday
  await pushToMonday(docs, originals);
  console.log('✅  Finished item', itemId);
}

// ─── 4. HELPERS ────────────────────────────────────────────────────
// 4-A  harvest every file on the item
async function collectPdfAssets(itemId){
  const pdfs=[];

  // assets attached to item
  const i = await gql(`query{ items(ids:[${itemId}]){ assets{id name url public_url file_extension}
                              column_values{id type value}
                              updates{ assets{id name url public_url file_extension}}}}`);
  const item = i.items[0];

  const dumpAssets = (arr,label)=>{
    console.log(`   • ${arr.length} assets in ${label}`);
    arr.forEach(a=>console.log(`     · ${a.id}  ${a.name}`));
  };

  dumpAssets(item.assets,'item.assets');

  // file columns
  let colAssets=[];
  for(const c of item.column_values.filter(c=>c.type==='file' && c.value)){
    try{
      const val = JSON.parse(c.value);
      (val.assets||val.files||[]).forEach(a=>colAssets.push(a));
    }catch{/*ignore*/}
  }
  dumpAssets(colAssets,'file columns');

  // updates
  const upAssets = item.updates.flatMap(u=>u.assets||[]);
  dumpAssets(upAssets,'updates');

  const all = [...item.assets,...colAssets,...upAssets];

  // second query to get signed URLs where missing
  const missing = all.filter(a=>!a.public_url && !a.url).map(a=>a.id);
  if(missing.length){
    const m = await gql(`query{ assets(ids:[${missing.join(',')}]){id url public_url file_extension}}`);
    const byId = Object.fromEntries(m.assets.map(a=>[a.id,a]));
    all.forEach(a=>{ if(!a.url && byId[a.id]) Object.assign(a,byId[a.id]); });
  }

  // keep only PDFs
  all.filter(a=>{
    const name=a.name.toLowerCase();
    const ext =(a.file_extension||'').toLowerCase();
    const ok = name.endsWith('.pdf') || ext==='pdf';
    if(ok) pdfs.push({name:a.name, public_url:a.public_url||a.url, assetId:a.id});
    else   console.log('      (skip non-PDF)',a.name);
  });

  console.log(`   → ${pdfs.length} PDF(s) ready`);
  return pdfs;
}

// 4-B  Instabase batch ▸ run ▸ results
async function instabaseRun(files){
  console.log('☁️  Instabase - create batch');
  const batch = await axios.post(`${INSTABASE.base}/api/v2/batches`,
                                 {workspace:'nileshn_sturgeontire.com'},
                                 {headers:INSTABASE.hdr});
  const batchId = batch.data.id;

  const originals=[];
  for(const f of files){
    const buf = Buffer.from((await axios.get(f.public_url,{responseType:'arraybuffer'})).data);
    originals.push({name:f.name, buffer:buf});
    await axios.put(`${INSTABASE.base}/api/v2/batches/${batchId}/files/${encodeURIComponent(f.name)}`,
                    buf,{headers:{...INSTABASE.hdr,'Content-Type':'application/octet-stream'}});
    console.log('   ↑ uploaded',f.name);
  }

  const run = await axios.post(`${INSTABASE.base}/api/v2/apps/deployments/${INSTABASE.dep}/runs`,
                               {batch_id:batchId},{headers:INSTABASE.hdr});
  const runId = run.data.id;
  console.log('   ▶ run',runId);

  let status='RUNNING', t=0;
  while(['RUNNING','PENDING'].includes(status) && t<60){
    await new Promise(r=>setTimeout(r,5000));
    status = (await axios.get(`${INSTABASE.base}/api/v2/apps/runs/${runId}`,
                              {headers:INSTABASE.hdr})).data.status;
    t++; process.stdout.write(`      … ${status}\r`);
  }
  if(status!=='COMPLETE') throw new Error('Instabase failed: '+status);
  console.log('\n   ✔ finished');

  const res = await axios.get(`${INSTABASE.base}/api/v2/apps/runs/${runId}/results`,
                              {headers:INSTABASE.hdr});
  return {files:res.data.files, originals};
}

// 4-C  push one row per doc, attach first PDF
async function pushToMonday(docs, originals){
  // fetch column metadata once
  const meta = await gql(`query{ boards(ids:[${MONDAY.dst}]){ columns{id title type} }}`);
  const cols = meta.boards[0].columns;
  const numCol = cols.find(c=>/document number/i.test(c.title));
  const fileCol= cols.find(c=>c.type==='file');

  for(const d of docs){
    const colVals = { [numCol.id] : d.invoice_number };
    const created = await gql(
      `mutation($v:JSON!){ create_item(board_id:${MONDAY.dst},
                   item_name:"${d.document_type} ${d.invoice_number}",
                   column_values:$v){ id }}`, { v: JSON.stringify(colVals) }
    );
    const id = created.create_item.id;
    console.log('   ➕ row',id);

    if(fileCol && originals.length){
      await uploadFile(id, originals[0], fileCol.id);
    }
  }
}

// 4-D  robust /v2/file uploader
async function uploadFile(itemId, file, colId){
  const ops = {
    query:`mutation ($file:File!,$item_id:Int!,$column_id:String!){
             add_file_to_column(file:$file,item_id:$item_id,column_id:$column_id){id}}`,
    variables:{ file:null, item_id:itemId, column_id:colId }
  };
  const form = new FormData();
  form.append('operations',JSON.stringify(ops));
  form.append('map',JSON.stringify({'0':['variables.file']}));
  form.append('0',file.buffer,{filename:file.name,contentType:'application/pdf'});

  try{
    const r = await axios.post('https://api.monday.com/v2/file',form,
               {headers:{...form.getHeaders(), ...AUTH_HDR}});
    console.log('     📎 attached', r.data.data.add_file_to_column.id);
  }catch(e){
    console.error('🟥 monday 400', JSON.stringify(e.response?.data||e, null, 2));
    throw e;
  }
}

// ─── 5. HOUSEKEEPING ───────────────────────────────────────────────
app.get('/health',(q,s)=>s.json({status:'ok',ts:Date.now()}));
const PORT = process.env.PORT||8080;
app.listen(PORT,'0.0.0.0',()=>console.log('🚀 webhook on',PORT));
module.exports = app;
