// Digital‑Mailroom Webhook — FIXED VERSION with PDF upload working
// -----------------------------------------------------------------------------
// This is the corrected server that fixes the PDF upload issue
// -----------------------------------------------------------------------------

const express   = require('express');
const axios     = require('axios');
const FormData  = require('form-data');

const app = express();
app.use(express.json());

// ─────────────────────────────────────────────────────────────────────────────
// 1️⃣  CONFIG  –  **unchanged / hard‑coded**
// ----------------------------------------------------------------------------
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

// Helper — toggle very verbose logs by `DEBUG=1 node server.js`
const dbg = (...args) => process.env.DEBUG && console.log('[dbg]', ...args);

// ─────────────────────────────────────────────────────────────────────────────
// 2️⃣  WEBHOOK ENDPOINT
// ----------------------------------------------------------------------------
app.post('/webhook/monday-to-instabase', async (req, res) => {
  try {
    console.log('=== WEBHOOK RECEIVED ===');
    dbg('Full body', req.body);

    // Monday "challenge" handshake
    if (req.body.challenge) return res.json({ challenge: req.body.challenge });

    res.json({ success: true, received: new Date().toISOString() });

    // fire‑and‑forget ⇢ background
    processWebhookData(req.body);
  } catch (err) {
    console.error('Webhook error', err);
    res.status(500).json({ error: err.message });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// 3️⃣  MAIN BACKGROUND FLOW
// ----------------------------------------------------------------------------
async function processWebhookData(body) {
  const ev = body.event;
  if (!ev) return;
  if (ev.columnId !== 'status' || ev.value?.label?.text !== 'Processing') return;

  const itemId = ev.pulseId;
  try {
    const pdfFiles = await getMondayItemFilesWithPublicUrl(itemId, ev.boardId);
    if (!pdfFiles.length) return;

    const { files: extracted, originalFiles } = await processFilesWithInstabase(pdfFiles);
    const groups = groupPagesByInvoiceNumber(extracted);
    await createMondayExtractedItems(groups, originalFiles);
  } catch (err) {
    console.error('Background processing failed', err);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 4️⃣  MONDAY HELPERS
// ----------------------------------------------------------------------------
async function getMondayItemFilesWithPublicUrl(itemId, boardId) {
  const query = `query { items(ids:[${itemId}]){ assets{id name file_extension file_size public_url} }}`;
  const { data } = await axios.post('https://api.monday.com/v2', { query }, {
    headers:{ Authorization:`Bearer ${MONDAY_CONFIG.apiKey}` }
  });
  const assets = data.data.items[0].assets;
  return assets.filter(a => (/pdf$/i).test(a.file_extension) || (/\.pdf$/i).test(a.name))
               .map(a => ({ name:a.name, public_url:a.public_url, assetId:a.id }));
}

// 🔧 FIXED: PDF Upload function with correct Monday.com multipart format
async function uploadPdfToMondayItem(itemId, buffer, filename, columnId) {
  try {
    console.log(`Attempting to upload PDF: ${filename} to item ${itemId}, column ${columnId}`);
    console.log(`Buffer size: ${buffer.length} bytes`);
    
    // Create the form data using Monday.com's multipart spec
    const form = new FormData();
    
    // Monday.com requires this specific multipart format
    const operations = {
      query: `
        mutation ($file: File!, $item_id: ID!, $column_id: String!) {
          add_file_to_column(item_id: $item_id, column_id: $column_id, file: $file) {
            id
          }
        }
      `,
      variables: {
        file: null,
        item_id: String(itemId),
        column_id: columnId
      }
    };
    
    // The map tells Monday.com which file corresponds to which variable
    const map = {
      "0": ["variables.file"]
    };
    
    // Append in the correct order that Monday.com expects
    form.append('operations', JSON.stringify(operations));
    form.append('map', JSON.stringify(map));
    form.append('0', buffer, {
      filename: filename,
      contentType: 'application/pdf'
    });

    console.log('Sending file upload request to Monday.com...');
    
    const response = await axios.post('https://api.monday.com/v2/file', form, {
      headers: {
        ...form.getHeaders(),
        'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`
      },
      timeout: 60000,
      maxBodyLength: Infinity,
      maxContentLength: Infinity
    });

    console.log('Monday.com file upload response:', JSON.stringify(response.data, null, 2));

    if (response.data.errors) {
      console.error('PDF Upload errors:', JSON.stringify(response.data.errors, null, 2));
      throw new Error(`PDF upload failed: ${JSON.stringify(response.data.errors)}`);
    } else {
      console.log('✅ PDF uploaded successfully!', response.data.data.add_file_to_column.id);
      return response.data.data.add_file_to_column.id;
    }
  } catch (error) {
    console.error('Error uploading PDF:', error.message);
    if (error.response) {
      console.error('Response status:', error.response.status);
      console.error('Response data:', JSON.stringify(error.response.data, null, 2));
    }
    throw error;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 5️⃣  INSTABASE PROCESSING (FULL VERSION)
// ----------------------------------------------------------------------------
async function processFilesWithInstabase(files) {
  try {
    console.log('=== STARTING INSTABASE PROCESSING ===');
    
    // Create batch
    const batchRes = await axios.post(`${INSTABASE_CONFIG.baseUrl}/api/v2/batches`,
                                     { workspace:'nileshn_sturgeontire.com' },
                                     { headers: INSTABASE_CONFIG.headers });
    const batchId = batchRes.data.id;
    console.log('✅ Created Instabase batch:', batchId);

    const originalFiles = [];
    
    // Upload files to batch
    for (const f of files) {
      console.log('Processing file:', f.name);
      const { data } = await axios.get(f.public_url, { responseType:'arraybuffer' });
      const buffer = Buffer.from(data);
      
      originalFiles.push({ name: f.name, buffer: buffer });
      
      await axios.put(`${INSTABASE_CONFIG.baseUrl}/api/v2/batches/${batchId}/files/${f.name}`,
                      data,
                      { headers:{ ...INSTABASE_CONFIG.headers, 'Content-Type':'application/octet-stream' } });
      console.log(`✅ Uploaded ${f.name} to Instabase`);
    }

    // Start processing run
    console.log('Starting Instabase processing...');
    const runResponse = await axios.post(
      `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/deployments/${INSTABASE_CONFIG.deploymentId}/runs`,
      { batch_id: batchId },
      { headers: INSTABASE_CONFIG.headers }
    );
    
    const runId = runResponse.data.id;
    console.log(`✅ Started processing run: ${runId}`);

    // Poll for completion
    let status = 'RUNNING';
    let attempts = 0;
    const maxAttempts = 60; // 5 minutes

    while (status === 'RUNNING' || status === 'PENDING') {
      if (attempts >= maxAttempts) {
        throw new Error('Processing timeout after 5 minutes');
      }

      await new Promise(resolve => setTimeout(resolve, 5000));

      const statusResponse = await axios.get(
        `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${runId}`,
        { headers: INSTABASE_CONFIG.headers }
      );

      status = statusResponse.data.status;
      attempts++;
      console.log(`Run status: ${status} (attempt ${attempts})`);

      if (status === 'ERROR' || status === 'FAILED') {
        throw new Error(`Instabase processing failed with status: ${status}`);
      }
    }

    if (status !== 'COMPLETE') {
      throw new Error(`Processing ended with unexpected status: ${status}`);
    }

    console.log('✅ Instabase processing completed successfully');

    // Get results
    const resultsResponse = await axios.get(
      `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${runId}/results`,
      { headers: INSTABASE_CONFIG.headers }
    );

    console.log('✅ Extraction results received');
    
    return { 
      files: resultsResponse.data.files, 
      originalFiles: originalFiles 
    };
    
  } catch (error) {
    console.error('Instabase processing error:', error);
    throw error;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 6️⃣  GROUPING (FULL VERSION)
// ----------------------------------------------------------------------------
function groupPagesByInvoiceNumber(extractedFiles) {
  console.log('=== GROUPING DEBUG ===');
  console.log('Input files:', extractedFiles?.length || 0);
  
  const documentGroups = {};
  
  if (!extractedFiles || extractedFiles.length === 0) {
    console.log('No extracted files to process');
    return [];
  }
  
  extractedFiles.forEach((file, fileIndex) => {
    console.log(`Processing file ${fileIndex}: ${file.original_file_name}`);
    
    if (!file.documents || file.documents.length === 0) {
      console.log('  No documents found in file');
      return;
    }
    
    file.documents.forEach((doc, docIndex) => {
      console.log(`  Processing document ${docIndex}`);
      const fields = doc.fields || {};
      console.log(`  Available fields:`, Object.keys(fields));
      
      // Extract field values based on Instabase field mapping
      const invoiceNumber = fields['0']?.value || 'unknown';
      const pageType = fields['1']?.value || 'unknown';
      const documentType = fields['2']?.value || 'invoice';
      const supplier = fields['3']?.value || '';
      const terms = fields['4']?.value || '';
      const documentDate = fields['5']?.value || '';
      const dueDateData = fields['6']?.value || '';
      const itemsData = fields['7']?.value || [];
      const totalAmount = fields['8']?.value || 0;
      const taxAmount = fields['9']?.value || 0;
      
      console.log(`  Extracted data:`);
      console.log(`    Invoice Number: "${invoiceNumber}"`);
      console.log(`    Page Type: "${pageType}"`);
      console.log(`    Document Type: "${documentType}"`);
      console.log(`    Supplier: "${supplier}"`);
      console.log(`    Total: "${totalAmount}"`);
      
      if (!invoiceNumber || invoiceNumber === 'none' || invoiceNumber === 'unknown') {
        console.log(`  Skipping document - no valid invoice number`);
        return;
      }
      
      // Create new group if it doesn't exist
      if (!documentGroups[invoiceNumber]) {
        documentGroups[invoiceNumber] = {
          invoice_number: invoiceNumber,
          document_type: documentType,
          supplier_name: supplier,
          total_amount: 0,
          tax_amount: 0,
          document_date: documentDate,
          due_date: '',
          due_date_2: '',
          due_date_3: '',
          terms: terms,
          items: [],
          pages: [],
          confidence: 0
        };
        console.log(`  Created new group for invoice: ${invoiceNumber}`);
      }
      
      const group = documentGroups[invoiceNumber];
      group.pages.push({
        page_type: pageType,
        file_name: file.original_file_name,
        fields: fields
      });
      
      // Update group data with main page info
      if (pageType === 'main' || !group.supplier_name) {
        if (supplier) {
          group.supplier_name = supplier;
        }
        
        if (totalAmount) {
          const totalStr = String(totalAmount).replace(/[^0-9.-]/g, '');
          group.total_amount = parseFloat(totalStr) || 0;
        }
        
        if (taxAmount) {
          const taxStr = String(taxAmount).replace(/[^0-9.-]/g, '');
          group.tax_amount = parseFloat(taxStr) || 0;
        }
        
        if (documentDate) {
          group.document_date = documentDate;
        }
        
        if (terms) {
          group.terms = terms;
        }
      }
      
      // Process line items
      if (itemsData && Array.isArray(itemsData) && itemsData.length > 0 && pageType === 'main') {
        console.log(`  Found items data:`, itemsData.length, 'items');
        
        const processedItems = itemsData.map(item => ({
          item_number: item['Item Number'] || item.item_number || '',
          description: item.description || item.desc || '',
          quantity: parseFloat(item.Quantity || item.quantity || '0') || 0,
          unit_cost: parseFloat(item['Unit Cost'] || item.unit_cost || item.price || '0') || 0,
          amount: parseFloat(item.amount || item.total || '0') || 0
        }));
        
        group.items = processedItems;
        console.log(`  Processed ${group.items.length} items`);
      }
    });
  });
  
  console.log(`=== GROUPING RESULT ===`);
  console.log(`Found ${Object.keys(documentGroups).length} document groups`);
  
  return Object.values(documentGroups);
}

// ─────────────────────────────────────────────────────────────────────────────
// 7️⃣  CREATE MONDAY ITEMS + SUB‑ITEMS
// ----------------------------------------------------------------------------
async function createMondayExtractedItems(documents, originalFiles){
  try {
    console.log('=== GETTING BOARD STRUCTURE ===');
    const { data } = await axios.post('https://api.monday.com/v2', {
      query:`query{boards(ids:[${MONDAY_CONFIG.extractedDocsBoardId}]){columns{id title type settings_str}}}`
    }, { headers:{ Authorization:`Bearer ${MONDAY_CONFIG.apiKey}` } });
    
    const columns = data.data.boards[0].columns;
    console.log('Available columns:');
    columns.forEach(col => {
      console.log(`  ${col.title}: ID = "${col.id}", Type = ${col.type}`);
    });

    const fmt = d => d ? new Date(d).toISOString().slice(0,10) : '';
    const fileCol = columns.find(c => c.type === 'file');

    for (const doc of documents){
      console.log(`Creating Monday.com item for ${doc.document_type} ${doc.invoice_number}...`);
      
      const cv = buildColumnValues(columns, doc, fmt);
      console.log('Mapped column values:', cv);
      
      const create = await axios.post('https://api.monday.com/v2', { query:
        `mutation{create_item(board_id:${MONDAY_CONFIG.extractedDocsBoardId},item_name:"${doc.document_type.toUpperCase()} ${doc.invoice_number}",column_values:${JSON.stringify(JSON.stringify(cv))}){id}}`
      }, { headers:{ Authorization:`Bearer ${MONDAY_CONFIG.apiKey}` } });

      if (create.data.errors) {
        console.error('Item creation errors:', create.data.errors);
        continue;
      }

      const itemId = create.data.data.create_item.id;
      console.log(`✅ Created Monday.com item for ${doc.document_type} ${doc.invoice_number} (ID: ${itemId})`);
      
      // Upload PDF file
      if (fileCol && originalFiles.length) {
        const { buffer, name } = originalFiles[0];
        try {
          await uploadPdfToMondayItem(itemId, buffer, name, fileCol.id);
        } catch (uploadError) {
          console.error('PDF upload failed, but continuing:', uploadError.message);
        }
      }
      
      // Create subitems for line items
      if (doc.items && doc.items.length > 0) {
        console.log(`Creating ${doc.items.length} subitems for line items...`);
        await createSubitemsForLineItems(itemId, doc.items);
      }
    }
  } catch (error) {
    console.error('Error creating Monday.com items:', error);
    throw error;
  }
}

async function createSubitemsForLineItems(parentItemId, items) {
  try {
    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      
      const itemNumber = String(item.item_number || '').replace(/"/g, '\\"');
      const description = String(item.description || itemNumber || `Item ${i + 1}`).replace(/"/g, '\\"');
      const quantity = item.quantity || 0;
      const unitCost = item.unit_cost || 0;
      const amount = item.amount || (quantity * unitCost);
      
      const subitemName = `${itemNumber}: ${description.substring(0, 40)}${description.length > 40 ? '...' : ''}`;
      
      console.log(`Creating subitem: ${subitemName} (Qty: ${quantity}, Cost: ${unitCost})`);
      
      const subitemMutation = `
        mutation {
          create_subitem(
            parent_item_id: ${parentItemId}
            item_name: "${subitemName}"
          ) {
            id
            name
          }
        }
      `;
      
      const subitemResponse = await axios.post('https://api.monday.com/v2', {
        query: subitemMutation
      }, {
        headers: {
          'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
          'Content-Type': 'application/json'
        }
      });
      
      if (subitemResponse.data.errors) {
        console.error('Subitem creation errors:', subitemResponse.data.errors);
      } else {
        console.log(`✅ Created subitem: ${subitemName}`);
      }
    }
  } catch (error) {
    console.error('Error creating subitems:', error);
  }
}

function buildColumnValues(columns, doc, fmt){
  const out = {};
  for (const c of columns){
    const t = c.title.toLowerCase();
    if (t.includes('supplier'))           out[c.id] = doc.supplier_name || '';
    else if (t.includes('document number')) out[c.id] = doc.invoice_number || '';
    else if (t.includes('document type')) {
      if (c.type === 'dropdown') {
        try {
          const settings = JSON.parse(c.settings_str || '{}');
          if (settings.labels && settings.labels.length > 0) {
            const matchingLabel = settings.labels.find(label => 
              label.name?.toLowerCase() === (doc.document_type || '').toLowerCase()
            );
            out[c.id] = matchingLabel ? matchingLabel.name : settings.labels[0].name;
          }
        } catch (e) {
          out[c.id] = doc.document_type || '';
        }
      } else {
        out[c.id] = doc.document_type || '';
      }
    }
    else if (t.includes('document date')) out[c.id] = fmt(doc.document_date);
    else if (t.includes('due date') && !t.includes('2') && !t.includes('3')) out[c.id] = fmt(doc.due_date);
    else if (t.includes('due date 2')) out[c.id] = fmt(doc.due_date_2);
    else if (t.includes('due date 3')) out[c.id] = fmt(doc.due_date_3);
    else if (t.includes('total amount'))  out[c.id] = doc.total_amount || 0;
    else if (t.includes('tax amount'))    out[c.id] = doc.tax_amount || 0;
    else if (t.includes('status'))        out[c.id] = c.type==='status'?{index:1}:'Extracted';
  }
  return out;
}

// ─────────────────────────────────────────────────────────────────────────────
// 8️⃣  HEALTH + TEST ROUTES
// ----------------------------------------------------------------------------
app.get('/health', (req,res)=>res.json({ ok:true, t:Date.now() }));
app.post('/test/process-item/:id', async(req,res)=>{
  try {
    const files = await getMondayItemFilesWithPublicUrl(req.params.id, MONDAY_CONFIG.fileUploadsBoardId);
    res.json({ files: files.map(f=>f.name) });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// 9️⃣  START SERVER
// ----------------------------------------------------------------------------
const PORT = process.env.PORT || 3000;
app.listen(PORT,'0.0.0.0',()=>console.log(`🚀 webhook listening on ${PORT}`));

// -----------------------------------------------------------------------------
//  End of file – PDF upload is now FIXED!
// -----------------------------------------------------------------------------
