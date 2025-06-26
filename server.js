async function createMondayExtractedItems(documents, sourceItemId, originalFiles, requestId) {
  try {
    log('info', 'STAGE: Getting board structure', { requestId });
    
    const boardQuery = `
      query {
        boards(ids: [${MONDAY_CONFIG.extractedDocsBoardId}]) {
          id
          name
          columns {
            id
            title
            type
            settings_str
          }
        }
      }
    `;
    
    const boardResponse = await axios.post('https://api.monday.com/v2', {
      query: boardQuery
    }, {
      headers: {
        'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
        'Content-Type': 'application/json'
      }
    });
    
    const columns = boardResponse.data.data?.boards?.[0]?.columns || [];
    
    // 🔧 FIX 6: Validate subitems column exists
    const subitemsColumn = columns.find(col => col.type === 'subtasks');
    log('info', 'BOARD_VALIDATION', {
      requestId,
      totalColumns: columns.length,
      hasSubitemsColumn: !!subitemsColumn,
      subitemsColumnId: subitemsColumn?.id,
      subitemsColumnTitle: subitemsColumn?.title
    });
    
    if (!subitemsColumn) {
      log('error', 'MISSING_SUBITEMS_COLUMN', {
        requestId,
        error: 'Board missing subitems column - cannot create subitems',
        availableColumns: columns.map(c => ({ id: c.id, title: c.title, type: c.type }))
      });
    }
    
    for (const doc of documents) {
      log('info', 'CREATING_MONDAY_ITEM', {
        requestId,
        documentType: doc.document_type,
        invoiceNumber: doc.invoice_number,
        itemCount: doc.items?.length || 0
      });
      
      const escapedSupplier = (doc.supplier_name || '').replace(/"/g, '\\"');
      const escapedInvoiceNumber = (doc.invoice_number || '').replace(/"/g, '\\"');
      const escapedDocumentType = (doc.document_type || '').replace(/"/g, '\\"');
      
      const formatDate = (dateStr) => {
        if (!dateStr) return '';
        try {
          const date = new Date(dateStr);
          return date.toISOString().split('T')[0];
        } catch (e) {
          return String(dateStr).slice(0, 10);
        }
      };
      
      const columnValues = {};
      
      // Map extracted data to Monday.com columns
      columns.forEach(col => {
        const title = col.title.toLowerCase();
        const id = col.id;
        const type = col.type;
        
        if (title.includes('supplier')) {
          columnValues[id] = escapedSupplier;
        } else if (title.includes('document number') || (title.includes('number') && !title.includes('total'))) {
          columnValues[id] = escapedInvoiceNumber;
        } else if (title.includes('document type') || (title.includes('type') && !title.includes('document'))) {
          if (type === 'dropdown') {
            let settings = {};
            try {
              settings = JSON.parse(col.settings_str || '{}');
            } catch (e) {
              log('warn', 'Could not parse dropdown settings', { requestId, columnId: id });
            }
            
            if (settings.labels && settings.labels.length > 0) {
              const matchingLabel = settings.labels.find(label => 
                label.name?.toLowerCase() === escapedDocumentType.toLowerCase()
              );
              
              if (matchingLabel) {
                columnValues[id] = matchingLabel.name;
              } else {
                columnValues[id] = settings.labels[0].name;
              }
            }
          } else {
            columnValues[id] = escapedDocumentType;
          }
        } else if (title.includes('document date') || (title.includes('date') && !title.includes('due'))) {
          columnValues[id] = formatDate(doc.document_date);
        } else if (title === 'due date' || title.includes('due date') && !title.includes('2') && !title.includes('3')) {
          columnValues[id] = formatDate(doc.due_date);
        } else if (title.includes('due date 2')) {
          columnValues[id] = formatDate(doc.due_date_2);
        } else if (title.includes('due date 3')) {
          columnValues[id] = formatDate(doc.due_date_3);
        } else if (title.includes('amount') && !title.includes('total') && !title.includes('tax')) {
          columnValues[id] = doc.total_amount || 0;
        } else if (title.includes('total amount')) {
          columnValues[id] = doc.total_amount || 0;
        } else if (title.includes('tax amount')) {
          columnValues[id] = doc.tax_amount || 0;
        } else if (title.includes('extraction status')) {
          if (type === 'status') {
            columnValues[id] = { "index": 1 };
          } else {
            columnValues[id] = "Extracted";
          }
        } else if (title.includes('status') && !title.includes('extraction')) {
          if (type === 'status') {
            columnValues[id] = { "index": 1 };
          } else {
            columnValues[id] = "Extracted";
          }
        }
      });
      
      // Create the item
      const mutation = `
        mutation {
          create_item(
            board_id: ${MONDAY_CONFIG.extractedDocsBoardId}
            item_name: "${escapedDocumentType.toUpperCase()} ${escapedInvoiceNumber}"
            column_values: ${JSON.stringify(JSON.stringify(columnValues))}
          ) {
            id
            name
          }
        }
      `;
      
      const response = await axios.post('https://api.monday.com/v2', {
        query: mutation
      }, {
        headers: {
          'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
          'Content-Type': 'application/json'
        }
      });
      
      if (response.data.errors) {
        log('error', 'ITEM_CREATION_FAILED', {
          requestId,
          errors: response.data.errors
        });
        continue;
      }
      
      const createdItemId = response.data.data.create_item.id;
      log('info', 'ITEM_CREATED_SUCCESS', {
        requestId,
        itemId: createdItemId,
        documentType: doc.document_type,
        invoiceNumber: doc.invoice_number
      });
      
      // Upload PDF file
      if (originalFiles && originalFiles.length > 0) {
        const fileColumn = columns.find(col => col.type === 'file');
        if (fileColumn) {
          try {
            await uploadPdfToMondayItem(createdItemId, originalFiles[0].buffer, originalFiles[0].name, fileColumn.id, requestId);
          } catch (uploadError) {
            log('error', 'PDF_UPLOAD_FAILED', {
              requestId,
              itemId: createdItemId,
              error: uploadError.message
            });
          }
        }
      }
      
      // 🔧 FIX 7: Enhanced subitem creation with proper validation and testing
      log('info', 'SUBITEM_PROCESSING_START', {
        requestId,
        itemId: createdItemId,
        hasItems: !!(doc.items && doc.items.length > 0),
        itemCount: doc.items?.length || 0,
        hasSubitemsColumn: !!subitemsColumn
      });
      
      // Always test subitem creation first
      if (subitemsColumn) {
        try {
          log('info', 'TEST_SUBITEM_CREATION', { requestId, itemId: createdItemId });
          
          const testSubitemMutation = `
            mutation {
              create_subitem(
                parent_item_id: ${createdItemId}
                item_name: "🧪 TEST - Line Items Processing"
              ) {
                id
                name
                board { id }
              }
            }
          `;
          
          const testResponse = await axios.post('https://api.monday.com/v2', {
            query: testSubitemMutation
          }, {
            headers: {
              'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
              'Content-Type': 'application/json',
              'API-Version': '2024-04'
            }
          });
          
          if (testResponse.data.errors) {
            log('error', 'TEST_SUBITEM_FAILED', {
              requestId,
              itemId: createdItemId,
              errors: testResponse.data.errors
            });
          } else {
            const testSubitemId = testResponse.data.data.create_subitem.id;
            log('info', 'TEST_SUBITEM_SUCCESS', {
              requestId,
              itemId: createdItemId,
              testSubitemId: testSubitemId
            });
            
            // Now create actual line item subitems
            if (doc.items && doc.items.length > 0) {
              log('info', 'CREATING_LINE_ITEM_SUBITEMS', {
                requestId,
                itemId: createdItemId,
                lineItemCount: doc.items.length
              });
              
              await createSubitemsForLineItems(createdItemId, doc.items, columns, requestId);
            } else {
              log('warn', 'NO_LINE_ITEMS_TO_CREATE', {
                requestId,
                itemId: createdItemId,
                documentHasItems: !!(doc.items),
                itemsLength: doc.items?.length || 0
              });
            }
          }
        } catch (testError) {
          log('error', 'TEST_SUBITEM_EXCEPTION', {
            requestId,
            itemId: createdItemId,
            error: testError.message,
            stack: testError.stack
          });
        }
      } else {
        log('error', 'CANNOT_CREATE_SUBITEMS', {
          requestId,
          itemId: createdItemId,
          reason: 'No subitems column found on board'
        });
      }
    }
  } catch (error) {
    log('error', 'CREATE_MONDAY_ITEMS_FAILED', {
      requestId,
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
}
        function groupPagesByInvoiceNumber(extractedFiles, requestId) {
  log('info', 'STAGE: Grouping debug started', { 
    requestId, 
    inputFiles: extractedFiles?.length || 0 
  });
  
  const documentGroups = {};
  
  if (!extractedFiles || extractedFiles.length === 0) {
    log('warn', 'No extracted files to process', { requestId });
    return [];
  }
  
  extractedFiles.forEach((file, fileIndex) => {
    log('info', 'Processing file', { 
      requestId, 
      fileIndex, 
      fileName: file.original_file_name 
    });
    
    if (!file.documents || file.documents.length === 0) {
      log('warn', 'No documents found in file', { requestId, fileIndex });
      return;
    }
    
    file.documents.forEach((doc, docIndex) => {
      const fields = doc.fields || {};
      
      // Extract field values
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
      
      log('info', 'Document processed', {
        requestId,
        docIndex,
        invoiceNumber,
        pageType,
        documentType,
        supplier,
        totalAmount
      });
      
      // 🔧 FIX 4: Enhanced Field 7 validation and debugging
      log('info', 'FIELD_7_CHECK', {
        requestId,
        docIndex,
        field7Exists: fields['7'] !== undefined,
        field7HasValue: !!fields['7']?.value,
        field7Type: typeof fields['7']?.value,
        field7IsArray: Array.isArray(fields['7']?.value),
        field7Length: Array.isArray(fields['7']?.value) ? fields['7'].value.length : 'N/A'
      });
      
      // Check all fields for arrays (potential line items)
      Object.keys(fields).forEach(key => {
        const val = fields[key]?.value;
        if (Array.isArray(val) && val.length > 0) {
          log('info', 'ARRAY_FOUND', {
            requestId,
            fieldKey: key,
            arrayLength: val.length,
            firstItem: val[0]
          });
        }
      });
      
      if (!invoiceNumber || invoiceNumber === 'none' || invoiceNumber === 'unknown') {
        log('warn', 'Skipping document - no valid invoice number', { requestId, docIndex });
        return;
      }
      
      // Create or update document group
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
        log('info', 'Created new document group', { requestId, invoiceNumber });
      }
      
      const group = documentGroups[invoiceNumber];
      group.pages.push({
        page_type: pageType,
        file_name: file.original_file_name,
        fields: fields
      });
      
      // Update group data with main page info
      if (pageType === 'main' || !group.supplier_name) {
        if (supplier) group.supplier_name = supplier;
        if (totalAmount) {
          const totalStr = String(totalAmount).replace(/[^0-9.-]/g, '');
          group.total_amount = parseFloat(totalStr) || 0;
        }
        if (taxAmount) {
          const taxStr = String(taxAmount).replace(/[^0-9.-]/g, '');
          group.tax_amount = parseFloat(taxStr) || 0;
        }
        if (documentDate) group.document_date = documentDate;
        if (terms) group.terms = terms;
      }
      
      // 🔧 FIX 5: Enhanced line items processing with validation
      log('info', 'LINE_ITEMS_PROCESSING_START', { 
        requestId, 
        pageType,
        hasItemsData: !!itemsData,
        itemsDataType: typeof itemsData,
        itemsDataLength: Array.isArray(itemsData) ? itemsData.length : 'N/A'
      });
      
      if (itemsData && Array.isArray(itemsData) && itemsData.length > 0) {
        log('info', 'ITEMS_TABLE_FOUND', { 
          requestId, 
          pageType,
          tableRowCount: itemsData.length,
          firstRowSample: itemsData[0]
        });
        
        const processedItems = [];
        
        itemsData.forEach((row, rowIndex) => {
          let itemNumber = '';
          let unitCost = 0;
          let quantity = 0;
          
          if (Array.isArray(row)) {
            // Array format: [itemNumber, unitCost, quantity]
            itemNumber = String(row[0] || '').trim();
            unitCost = parseFloat(row[1]) || 0;
            quantity = parseFloat(row[2]) || 0;
          } else if (typeof row === 'object' && row !== null) {
            // Object format with properties
            itemNumber = String(row['Item Number'] || row.item_number || row.itemNumber || row.number || '').trim();
            unitCost = parseFloat(row['Unit Cost'] || row.unit_cost || row.unitCost || row.cost || row.price || 0);
            quantity = parseFloat(row.Quantity || row.quantity || row.qty || 0);
          }
          
          // Only add if we have meaningful data
          if (itemNumber && (unitCost > 0 || quantity > 0)) {
            const processedItem = {
              item_number: itemNumber,
              description: '', // Not in this table format
              quantity: quantity,
              unit_cost: unitCost,
              amount: quantity * unitCost
            };
            
            processedItems.push(processedItem);
            log('info', 'ITEM_PROCESSED', {
              requestId,
              rowIndex,
              itemNumber,
              quantity,
              unitCost,
              amount: processedItem.amount
            });
          }
        });
        
        // Add items to the group (prioritize main page)
        if (processedItems.length > 0 && (pageType === 'main' || group.items.length === 0)) {
          group.items = processedItems;
          log('info', 'ITEMS_ADDED_TO_GROUP', { 
            requestId, 
            invoiceNumber,
            itemCount: processedItems.length 
          });
        }
      } else {
        log('info', 'NO_ITEMS_TABLE_FOUND', { requestId, pageType });
      }
    });
  });
  
  const resultGroups = Object.values(documentGroups);
  log('info', 'GROUPING_RESULT', {
    requestId,
    groupCount: resultGroups.length,
    groupsWithItems: resultGroups.filter(g => g.items.length > 0).length
  });
  
  resultGroups.forEach(group => {
    log('info', 'GROUP_SUMMARY', {
      requestId,
      invoiceNumber: group.invoice_number,
      pageCount: group.pages.length,
      itemCount: group.items.length,
      hasItems: group.items.length > 0
    });
  });
  
  return resultGroups;
}// Digital‑Mailroom Webhook — FIXED VERSION with PDF upload working
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
    
    // Create the form data using Monday.com's exact specification
    const form = new FormData();
    
    // Monday.com requires this exact format based on their documentation
    const query = `mutation($file: File!, $item_id: ID!, $column_id: String!) {
      add_file_to_column(item_id: $item_id, column_id: $column_id, file: $file) {
        id
      }
    }`;
    
    const variables = {
      item_id: String(itemId),
      column_id: columnId
    };
    
    const map = {
      "file": "variables.file"
    };
    
    // Append in the exact order Monday.com expects
    form.append('query', query);
    form.append('variables', JSON.stringify(variables));
    form.append('map', JSON.stringify(map));
    form.append('file', buffer, {
      filename: filename,
      contentType: 'application/pdf'
    });

    console.log('Sending file upload request to Monday.com...');
    console.log('Query:', query);
    console.log('Variables:', variables);
    console.log('Map:', map);
    
    const response = await axios.post('https://api.monday.com/v2/file', form, {
      headers: {
        ...form.getHeaders(),
        'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
        'API-Version': '2024-04'
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
    console.log(`Creating ${items.length} subitems for parent item ${parentItemId}...`);
    
    // First, let's get the subitem board structure to find column IDs
    const subitemBoardQuery = `
      query {
        boards(ids: [${MONDAY_CONFIG.extractedDocsBoardId}]) {
          id
          name
          columns {
            id
            title
            type
          }
        }
      }
    `;
    
    const subitemBoardResponse = await axios.post('https://api.monday.com/v2', {
      query: subitemBoardQuery
    }, {
      headers: {
        'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
        'Content-Type': 'application/json'
      }
    });
    
    const allColumns = subitemBoardResponse.data.data?.boards?.[0]?.columns || [];
    console.log('=== ALL BOARD COLUMNS (including subitem columns) ===');
    allColumns.forEach(col => {
      console.log(`  ${col.title}: ID = "${col.id}", Type = ${col.type}`);
    });
    
    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      
      // Clean and prepare data
      const itemNumber = String(item.item_number || '').replace(/"/g, '\\"').substring(0, 50);
      const quantity = parseFloat(item.quantity) || 0;
      const unitCost = parseFloat(item.unit_cost) || 0;
      
      // Create meaningful subitem name
      const subitemName = itemNumber || `Line Item ${i + 1}`;
      
      console.log(`Creating subitem ${i + 1}: ${subitemName}`);
      console.log(`  Item Number: "${itemNumber}"`);
      console.log(`  Quantity: ${quantity}`);
      console.log(`  Unit Cost: ${unitCost}`);
      
      // Map to your subitem columns (we'll update these IDs once we see them in logs)
      const subitemColumnValues = {};
      
      // Find the actual column IDs from your board
      allColumns.forEach(col => {
        const title = col.title.toLowerCase();
        
        if (title.includes('item num') || title === 'item number') {
          subitemColumnValues[col.id] = itemNumber;
          console.log(`  Mapping Item Number to column ${col.id}: "${itemNumber}"`);
        } else if (title.includes('quantity') || title === 'quantity') {
          subitemColumnValues[col.id] = quantity;
          console.log(`  Mapping Quantity to column ${col.id}: ${quantity}`);
        } else if (title.includes('unit cost') || title === 'unit cost') {
          subitemColumnValues[col.id] = unitCost;
          console.log(`  Mapping Unit Cost to column ${col.id}: ${unitCost}`);
        }
      });
      
      console.log(`Final column values for subitem ${i + 1}:`, subitemColumnValues);
      
      // Convert to JSON string format that Monday.com expects
      const columnValuesJson = JSON.stringify(subitemColumnValues);
      
      // Create the subitem mutation
      const subitemMutation = `
        mutation {
          create_subitem(
            parent_item_id: ${parentItemId}
            item_name: "${subitemName.replace(/"/g, '\\"')}"
            column_values: ${JSON.stringify(columnValuesJson)}
          ) {
            id
            name
            board {
              id
            }
          }
        }
      `;
      
      console.log(`Executing subitem mutation for item ${i + 1}...`);
      console.log(`Query:`, subitemMutation);
      
      const subitemResponse = await axios.post('https://api.monday.com/v2', {
        query: subitemMutation
      }, {
        headers: {
          'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
          'Content-Type': 'application/json',
          'API-Version': '2024-04'
        },
        timeout: 30000
      });
      
      console.log(`Subitem response for item ${i + 1}:`, JSON.stringify(subitemResponse.data, null, 2));
      
      if (subitemResponse.data.errors) {
        console.error(`Subitem creation errors for item ${i + 1}:`, JSON.stringify(subitemResponse.data.errors, null, 2));
        
        // Try creating without column values if there are errors
        console.log(`Retrying subitem ${i + 1} without column values...`);
        
        const simpleSubitemMutation = `
          mutation {
            create_subitem(
              parent_item_id: ${parentItemId}
              item_name: "${subitemName.replace(/"/g, '\\"')}"
            ) {
              id
              name
            }
          }
        `;
        
        const retryResponse = await axios.post('https://api.monday.com/v2', {
          query: simpleSubitemMutation
        }, {
          headers: {
            'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
            'Content-Type': 'application/json',
            'API-Version': '2024-04'
          }
        });
        
        if (retryResponse.data.errors) {
          console.error(`Retry also failed for item ${i + 1}:`, JSON.stringify(retryResponse.data.errors, null, 2));
        } else {
          console.log(`✅ Created simple subitem ${i + 1}: ${subitemName} (ID: ${retryResponse.data.data.create_subitem.id})`);
        }
      } else {
        console.log(`✅ Created subitem ${i + 1}: ${subitemName} (ID: ${subitemResponse.data.data.create_subitem.id})`);
      }
      
      // Small delay between subitem creations to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 500));
    }
    
    console.log(`✅ Completed creating subitems for parent item ${parentItemId}`);
    
  } catch (error) {
    console.error('Error creating subitems:', error.message);
    if (error.response) {
      console.error('Response status:', error.response.status);
      console.error('Response data:', JSON.stringify(error.response.data, null, 2));
    }
    // Don't throw error - continue with other processing
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
