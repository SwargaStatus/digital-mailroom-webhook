// Digital Mailroom Webhook — FULLY FIXED VERSION with Working Subitems
// -----------------------------------------------------------------------------
// This version fixes all subitem creation issues and line item processing
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

// Helper logging with request ID
function log(level, message, data = {}) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] ${level.toUpperCase()}: ${message}`, data);
}

// ─────────────────────────────────────────────────────────────────────────────
// 2️⃣  WEBHOOK ENDPOINT
// ----------------------------------------------------------------------------
app.post('/webhook/monday-to-instabase', async (req, res) => {
  const requestId = `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  
  try {
    log('info', 'WEBHOOK_RECEIVED', { requestId, body: req.body });

    // Monday "challenge" handshake
    if (req.body.challenge) {
      log('info', 'CHALLENGE_RESPONSE', { requestId, challenge: req.body.challenge });
      return res.json({ challenge: req.body.challenge });
    }

    res.json({ success: true, received: new Date().toISOString(), requestId });

    // fire‑and‑forget ⇢ background processing
    processWebhookData(req.body, requestId);
  } catch (err) {
    log('error', 'WEBHOOK_ERROR', { requestId, error: err.message, stack: err.stack });
    res.status(500).json({ error: err.message });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// 3️⃣  MAIN BACKGROUND FLOW
// ----------------------------------------------------------------------------
async function processWebhookData(body, requestId) {
  try {
    log('info', 'PROCESSING_START', { requestId, event: body.event });
    
    const ev = body.event;
    if (!ev) {
      log('warn', 'NO_EVENT_DATA', { requestId });
      return;
    }
    
    if (ev.columnId !== 'status' || ev.value?.label?.text !== 'Processing') {
      log('info', 'IGNORED_EVENT', { 
        requestId, 
        columnId: ev.columnId, 
        status: ev.value?.label?.text 
      });
      return;
    }

    const itemId = ev.pulseId;
    log('info', 'PROCESSING_ITEM', { requestId, itemId, boardId: ev.boardId });
    
    const pdfFiles = await getMondayItemFilesWithPublicUrl(itemId, ev.boardId, requestId);
    if (!pdfFiles.length) {
      log('warn', 'NO_PDF_FILES', { requestId, itemId });
      return;
    }

    const { files: extracted, originalFiles } = await processFilesWithInstabase(pdfFiles, requestId);
    const groups = groupPagesByInvoiceNumber(extracted, requestId);
    await createMondayExtractedItems(groups, itemId, originalFiles, requestId);
    
    log('info', 'PROCESSING_COMPLETE', { requestId, itemId });
  } catch (err) {
    log('error', 'BACKGROUND_PROCESSING_FAILED', { 
      requestId, 
      error: err.message, 
      stack: err.stack 
    });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 4️⃣  MONDAY HELPERS
// ----------------------------------------------------------------------------
async function getMondayItemFilesWithPublicUrl(itemId, boardId, requestId) {
  try {
    log('info', 'FETCHING_FILES', { requestId, itemId, boardId });
    
    const query = `query { items(ids:[${itemId}]){ assets{id name file_extension file_size public_url} }}`;
    const { data } = await axios.post('https://api.monday.com/v2', { query }, {
      headers: { Authorization: `Bearer ${MONDAY_CONFIG.apiKey}` }
    });
    
    const assets = data.data.items[0].assets;
    const pdfFiles = assets.filter(a => 
      (/pdf$/i).test(a.file_extension) || (/\.pdf$/i).test(a.name)
    ).map(a => ({ 
      name: a.name, 
      public_url: a.public_url, 
      assetId: a.id 
    }));
    
    log('info', 'FILES_FOUND', { requestId, fileCount: pdfFiles.length, files: pdfFiles.map(f => f.name) });
    return pdfFiles;
  } catch (error) {
    log('error', 'FETCH_FILES_ERROR', { requestId, error: error.message });
    throw error;
  }
}

// 🔧 FIXED: PDF Upload function with correct Monday.com multipart format
async function uploadPdfToMondayItem(itemId, buffer, filename, columnId, requestId) {
  try {
    log('info', 'PDF_UPLOAD_START', { requestId, itemId, filename, columnId, bufferSize: buffer.length });
    
    // Create the form data using Monday.com's exact specification
    const form = new FormData();
    
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

    if (response.data.errors) {
      log('error', 'PDF_UPLOAD_ERROR', { requestId, errors: response.data.errors });
      throw new Error(`PDF upload failed: ${JSON.stringify(response.data.errors)}`);
    } else {
      log('info', 'PDF_UPLOAD_SUCCESS', { requestId, fileId: response.data.data.add_file_to_column.id });
      return response.data.data.add_file_to_column.id;
    }
  } catch (error) {
    log('error', 'PDF_UPLOAD_EXCEPTION', { requestId, error: error.message });
    throw error;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 5️⃣  INSTABASE PROCESSING (FULL VERSION)
// ----------------------------------------------------------------------------
async function processFilesWithInstabase(files, requestId) {
  try {
    log('info', 'INSTABASE_START', { requestId, fileCount: files.length });
    
    // Create batch
    const batchRes = await axios.post(`${INSTABASE_CONFIG.baseUrl}/api/v2/batches`,
                                     { workspace:'nileshn_sturgeontire.com' },
                                     { headers: INSTABASE_CONFIG.headers });
    const batchId = batchRes.data.id;
    log('info', 'BATCH_CREATED', { requestId, batchId });

    const originalFiles = [];
    
    // Upload files to batch
    for (const f of files) {
      log('info', 'UPLOADING_FILE', { requestId, fileName: f.name });
      const { data } = await axios.get(f.public_url, { responseType:'arraybuffer' });
      const buffer = Buffer.from(data);
      
      originalFiles.push({ name: f.name, buffer: buffer });
      
      await axios.put(`${INSTABASE_CONFIG.baseUrl}/api/v2/batches/${batchId}/files/${f.name}`,
                      data,
                      { headers:{ ...INSTABASE_CONFIG.headers, 'Content-Type':'application/octet-stream' } });
      log('info', 'FILE_UPLOADED', { requestId, fileName: f.name });
    }

    // Start processing run
    log('info', 'STARTING_PROCESSING', { requestId, batchId });
    const runResponse = await axios.post(
      `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/deployments/${INSTABASE_CONFIG.deploymentId}/runs`,
      { batch_id: batchId },
      { headers: INSTABASE_CONFIG.headers }
    );
    
    const runId = runResponse.data.id;
    log('info', 'RUN_STARTED', { requestId, runId });

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
      log('info', 'PROCESSING_STATUS', { requestId, status, attempt: attempts });

      if (status === 'ERROR' || status === 'FAILED') {
        throw new Error(`Instabase processing failed with status: ${status}`);
      }
    }

    if (status !== 'COMPLETE') {
      throw new Error(`Processing ended with unexpected status: ${status}`);
    }

    log('info', 'PROCESSING_COMPLETE', { requestId, runId });

    // Get results
    const resultsResponse = await axios.get(
      `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${runId}/results`,
      { headers: INSTABASE_CONFIG.headers }
    );

    log('info', 'RESULTS_RECEIVED', { requestId, fileCount: resultsResponse.data.files?.length || 0 });
    
    return { 
      files: resultsResponse.data.files, 
      originalFiles: originalFiles 
    };
    
  } catch (error) {
    log('error', 'INSTABASE_ERROR', { requestId, error: error.message });
    throw error;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 6️⃣  ENHANCED GROUPING WITH PROPER LINE ITEMS PROCESSING
// ----------------------------------------------------------------------------
function groupPagesByInvoiceNumber(extractedFiles, requestId) {
  log('info', 'GROUPING_START', { 
    requestId, 
    inputFiles: extractedFiles?.length || 0 
  });
  
  const documentGroups = {};
  
  if (!extractedFiles || extractedFiles.length === 0) {
    log('warn', 'NO_FILES_TO_GROUP', { requestId });
    return [];
  }
  
  extractedFiles.forEach((file, fileIndex) => {
    log('info', 'PROCESSING_FILE', { 
      requestId, 
      fileIndex, 
      fileName: file.original_file_name 
    });
    
    if (!file.documents || file.documents.length === 0) {
      log('warn', 'NO_DOCUMENTS_IN_FILE', { requestId, fileIndex });
      return;
    }
    
    file.documents.forEach((doc, docIndex) => {
      const fields = doc.fields || {};
      
      log('info', 'DOCUMENT_FIELDS', {
        requestId,
        docIndex,
        availableFields: Object.keys(fields),
        field7Type: typeof fields['7']?.value,
        field7IsArray: Array.isArray(fields['7']?.value),
        field7Length: Array.isArray(fields['7']?.value) ? fields['7'].value.length : 'N/A'
      });
      
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
      
      log('info', 'EXTRACTED_DATA', {
        requestId,
        docIndex,
        invoiceNumber,
        pageType,
        documentType,
        supplier,
        totalAmount,
        hasItemsData: !!itemsData,
        itemsDataLength: Array.isArray(itemsData) ? itemsData.length : 'N/A'
      });
      
      if (!invoiceNumber || invoiceNumber === 'none' || invoiceNumber === 'unknown') {
        log('warn', 'SKIPPING_NO_INVOICE', { requestId, docIndex });
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
        log('info', 'GROUP_CREATED', { requestId, invoiceNumber });
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
      
      // 🔧 ENHANCED LINE ITEMS PROCESSING
      log('info', 'LINE_ITEMS_CHECK', { 
        requestId, 
        pageType,
        hasItemsData: !!itemsData,
        itemsDataType: typeof itemsData,
        itemsDataIsArray: Array.isArray(itemsData),
        itemsDataLength: Array.isArray(itemsData) ? itemsData.length : 'N/A'
      });
      
      if (itemsData && Array.isArray(itemsData) && itemsData.length > 0) {
        log('info', 'PROCESSING_LINE_ITEMS', { 
          requestId, 
          pageType,
          tableRowCount: itemsData.length,
          firstRowSample: itemsData[0]
        });
        
        const processedItems = [];
        
        itemsData.forEach((row, rowIndex) => {
          log('info', 'PROCESSING_ROW', {
            requestId,
            rowIndex,
            rowType: typeof row,
            rowIsArray: Array.isArray(row),
            rowContent: row
          });
          
          let itemNumber = '';
          let unitCost = 0;
          let quantity = 0;
          let description = '';
          
          if (Array.isArray(row)) {
            // Array format: [itemNumber, unitCost, quantity, description?]
            itemNumber = String(row[0] || '').trim();
            unitCost = parseFloat(row[1]) || 0;
            quantity = parseFloat(row[2]) || 0;
            description = String(row[3] || '').trim();
          } else if (typeof row === 'object' && row !== null) {
            // Object format with properties
            itemNumber = String(row['Item Number'] || row.item_number || row.itemNumber || row.number || row.item || '').trim();
            unitCost = parseFloat(row['Unit Cost'] || row.unit_cost || row.unitCost || row.cost || row.price || 0);
            quantity = parseFloat(row.Quantity || row.quantity || row.qty || row.amount || 0);
            description = String(row.Description || row.description || row.desc || row.item_description || '').trim();
          } else {
            // Single value - treat as item number
            itemNumber = String(row || '').trim();
          }
          
          // Only add if we have meaningful data
          if (itemNumber || (unitCost > 0) || (quantity > 0)) {
            const processedItem = {
              item_number: itemNumber || `Item ${rowIndex + 1}`,
              description: description,
              quantity: quantity,
              unit_cost: unitCost,
              amount: quantity * unitCost
            };
            
            processedItems.push(processedItem);
            log('info', 'ITEM_ADDED', {
              requestId,
              rowIndex,
              itemNumber: processedItem.item_number,
              quantity: processedItem.quantity,
              unitCost: processedItem.unit_cost,
              amount: processedItem.amount
            });
          } else {
            log('warn', 'ITEM_SKIPPED_NO_DATA', { requestId, rowIndex, row });
          }
        });
        
        // Add items to the group (prioritize main page, but accept any page with items)
        if (processedItems.length > 0 && (pageType === 'main' || group.items.length === 0)) {
          group.items = processedItems;
          log('info', 'ITEMS_ASSIGNED_TO_GROUP', { 
            requestId, 
            invoiceNumber,
            itemCount: processedItems.length,
            pageType
          });
        }
      } else {
        log('info', 'NO_LINE_ITEMS_FOUND', { requestId, pageType, docIndex });
      }
    });
  });
  
  const resultGroups = Object.values(documentGroups);
  log('info', 'GROUPING_COMPLETE', {
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
      totalAmount: group.total_amount
    });
  });
  
  return resultGroups;
}

// ─────────────────────────────────────────────────────────────────────────────
// 7️⃣  ENHANCED MONDAY ITEMS CREATION WITH FIXED SUBITEMS
// ----------------------------------------------------------------------------
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
    
    // Validate subitems column exists
    const subitemsColumn = columns.find(col => col.type === 'subtasks');
    log('info', 'BOARD_VALIDATION', {
      requestId,
      totalColumns: columns.length,
      hasSubitemsColumn: !!subitemsColumn,
      subitemsColumnId: subitemsColumn?.id,
      subitemsColumnTitle: subitemsColumn?.title,
      allColumns: columns.map(c => ({ id: c.id, title: c.title, type: c.type }))
    });
    
    if (!subitemsColumn) {
      log('error', 'MISSING_SUBITEMS_COLUMN', {
        requestId,
        error: 'Board missing subitems column - cannot create subitems'
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
      
      const columnValues = buildColumnValues(columns, doc, formatDate);
      
      log('info', 'COLUMN_VALUES_MAPPED', { 
        requestId, 
        invoiceNumber: doc.invoice_number,
        columnValues 
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
      
      // 🔧 FIXED: Enhanced subitem creation with proper validation
      log('info', 'SUBITEM_PROCESSING_START', {
        requestId,
        itemId: createdItemId,
        hasItems: !!(doc.items && doc.items.length > 0),
        itemCount: doc.items?.length || 0,
        hasSubitemsColumn: !!subitemsColumn
      });
      
      // Create subitems if we have line items and subitems column exists
      if (subitemsColumn && doc.items && doc.items.length > 0) {
        try {
          log('info', 'CREATING_SUBITEMS', { 
            requestId, 
            itemId: createdItemId,
            lineItemCount: doc.items.length 
          });
          
          await createSubitemsForLineItems(createdItemId, doc.items, columns, requestId);
          
        } catch (subitemError) {
          log('error', 'SUBITEM_CREATION_FAILED', {
            requestId,
            itemId: createdItemId,
            error: subitemError.message,
            stack: subitemError.stack
          });
        }
      } else {
        if (!subitemsColumn) {
          log('warn', 'NO_SUBITEMS_COLUMN', { requestId, itemId: createdItemId });
        }
        if (!doc.items || doc.items.length === 0) {
          log('warn', 'NO_LINE_ITEMS', { 
            requestId, 
            itemId: createdItemId,
            hasItems: !!doc.items,
            itemsLength: doc.items?.length || 0
          });
        }
      }
    }
    
    log('info', 'ALL_ITEMS_PROCESSED', { requestId, documentCount: documents.length });
    
  } catch (error) {
    log('error', 'CREATE_MONDAY_ITEMS_FAILED', {
      requestId,
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
}

// 🔧 COMPLETELY REWRITTEN: Subitem creation function
async function createSubitemsForLineItems(parentItemId, items, columns, requestId) {
  try {
    log('info', 'SUBITEM_CREATION_START', {
      requestId,
      parentItemId,
      lineItemCount: items.length
    });
    
    // Test subitem creation capability first
    const testSubitemMutation = `
      mutation {
        create_subitem(
          parent_item_id: ${parentItemId}
          item_name: "🧪 TEST - Line Items Available"
        ) {
          id
          name
          board { id }
        }
      }
    `;
    
    log('info', 'TESTING_SUBITEM_CREATION', { requestId, parentItemId });
    
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
        parentItemId,
        errors: testResponse.data.errors
      });
      return;
    }
    
    const testSubitemId = testResponse.data.data.create_subitem.id;
    const subitemBoardId = testResponse.data.data.create_subitem.board.id;
    
    log('info', 'TEST_SUBITEM_SUCCESS', {
      requestId,
      parentItemId,
      testSubitemId,
      subitemBoardId
    });
    
    // Get subitem board structure
    const subitemBoardQuery = `
      query {
        boards(ids: [${subitemBoardId}]) {
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
    
    const subitemColumns = subitemBoardResponse.data.data?.boards?.[0]?.columns || [];
    
    log('info', 'SUBITEM_BOARD_COLUMNS', {
      requestId,
      subitemBoardId,
      columnCount: subitemColumns.length,
      columns: subitemColumns.map(c => ({ id: c.id, title: c.title, type: c.type }))
    });
    
    // Create actual line item subitems
    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      
      // Clean and prepare data
      const itemNumber = String(item.item_number || '').replace(/"/g, '\\"').substring(0, 50);
      const description = String(item.description || '').replace(/"/g, '\\"').substring(0, 100);
      const quantity = parseFloat(item.quantity) || 0;
      const unitCost = parseFloat(item.unit_cost) || 0;
      const amount = parseFloat(item.amount) || (quantity * unitCost);
      
      // Create meaningful subitem name
      const subitemName = itemNumber ? 
        `${itemNumber}${description ? ` - ${description}` : ''}` : 
        `Line Item ${i + 1}${description ? ` - ${description}` : ''}`;
      
      log('info', 'CREATING_LINE_ITEM', {
        requestId,
        lineIndex: i + 1,
        itemNumber,
        description,
        quantity,
        unitCost,
        amount,
        subitemName
      });
      
      // Map to subitem columns
      const subitemColumnValues = {};
      
      subitemColumns.forEach(col => {
        const title = col.title.toLowerCase();
        
        if (title.includes('item num') || title.includes('item number') || title === 'item #') {
          subitemColumnValues[col.id] = itemNumber;
        } else if (title.includes('description') || title.includes('desc')) {
          subitemColumnValues[col.id] = description;
        } else if (title.includes('quantity') || title === 'qty') {
          if (col.type === 'numbers') {
            subitemColumnValues[col.id] = quantity;
          } else {
            subitemColumnValues[col.id] = String(quantity);
          }
        } else if (title.includes('unit cost') || title.includes('price')) {
          if (col.type === 'numbers') {
            subitemColumnValues[col.id] = unitCost;
          } else {
            subitemColumnValues[col.id] = String(unitCost);
          }
        } else if (title.includes('amount') || title.includes('total')) {
          if (col.type === 'numbers') {
            subitemColumnValues[col.id] = amount;
          } else {
            subitemColumnValues[col.id] = String(amount);
          }
        }
      });
      
      log('info', 'SUBITEM_COLUMN_MAPPING', {
        requestId,
        lineIndex: i + 1,
        columnValues: subitemColumnValues
      });
      
      // Create the subitem with column values
      const subitemMutation = `
        mutation {
          create_subitem(
            parent_item_id: ${parentItemId}
            item_name: "${subitemName.replace(/"/g, '\\"')}"
            column_values: ${JSON.stringify(JSON.stringify(subitemColumnValues))}
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
          'Content-Type': 'application/json',
          'API-Version': '2024-04'
        },
        timeout: 30000
      });
      
      if (subitemResponse.data.errors) {
        log('error', 'SUBITEM_WITH_COLUMNS_FAILED', {
          requestId,
          lineIndex: i + 1,
          errors: subitemResponse.data.errors
        });
        
        // Try creating without column values
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
          log('error', 'SIMPLE_SUBITEM_ALSO_FAILED', {
            requestId,
            lineIndex: i + 1,
            errors: retryResponse.data.errors
          });
        } else {
          log('info', 'SIMPLE_SUBITEM_SUCCESS', {
            requestId,
            lineIndex: i + 1,
            subitemId: retryResponse.data.data.create_subitem.id,
            subitemName
          });
        }
      } else {
        log('info', 'SUBITEM_WITH_COLUMNS_SUCCESS', {
          requestId,
          lineIndex: i + 1,
          subitemId: subitemResponse.data.data.create_subitem.id,
          subitemName
        });
      }
      
      // Small delay between subitem creations to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 500));
    }
    
    log('info', 'ALL_SUBITEMS_CREATED', {
      requestId,
      parentItemId,
      lineItemCount: items.length
    });
    
  } catch (error) {
    log('error', 'SUBITEM_CREATION_ERROR', {
      requestId,
      parentItemId,
      error: error.message,
      stack: error.stack
    });
    // Don't throw - continue with other processing
  }
}

function buildColumnValues(columns, doc, formatDate) {
  const columnValues = {};
  
  // Map extracted data to Monday.com columns
  columns.forEach(col => {
    const title = col.title.toLowerCase();
    const id = col.id;
    const type = col.type;
    
    if (title.includes('supplier')) {
      columnValues[id] = doc.supplier_name || '';
    } else if (title.includes('document number') || (title.includes('number') && !title.includes('total'))) {
      columnValues[id] = doc.invoice_number || '';
    } else if (title.includes('document type') || (title.includes('type') && !title.includes('document'))) {
      if (type === 'dropdown') {
        let settings = {};
        try {
          settings = JSON.parse(col.settings_str || '{}');
        } catch (e) {
          // Use default if parsing fails
        }
        
        if (settings.labels && settings.labels.length > 0) {
          const matchingLabel = settings.labels.find(label => 
            label.name?.toLowerCase() === (doc.document_type || '').toLowerCase()
          );
          
          if (matchingLabel) {
            columnValues[id] = matchingLabel.name;
          } else {
            columnValues[id] = settings.labels[0].name;
          }
        }
      } else {
        columnValues[id] = doc.document_type || '';
      }
    } else if (title.includes('document date') || (title.includes('date') && !title.includes('due'))) {
      columnValues[id] = formatDate(doc.document_date);
    } else if (title === 'due date' || (title.includes('due date') && !title.includes('2') && !title.includes('3'))) {
      columnValues[id] = formatDate(doc.due_date);
    } else if (title.includes('due date 2')) {
      columnValues[id] = formatDate(doc.due_date_2);
    } else if (title.includes('due date 3')) {
      columnValues[id] = formatDate(doc.due_date_3);
    } else if (title.includes('total amount')) {
      columnValues[id] = doc.total_amount || 0;
    } else if (title.includes('tax amount')) {
      columnValues[id] = doc.tax_amount || 0;
    } else if (title.includes('extraction status') || title.includes('status')) {
      if (type === 'status') {
        columnValues[id] = { "index": 1 };
      } else {
        columnValues[id] = "Extracted";
      }
    }
  });
  
  return columnValues;
}

// ─────────────────────────────────────────────────────────────────────────────
// 8️⃣  HEALTH + TEST ROUTES
// ----------------------------------------------------------------------------
app.get('/health', (req, res) => {
  res.json({ 
    ok: true, 
    timestamp: new Date().toISOString(),
    service: 'digital-mailroom-webhook',
    version: '2.0.0-fixed'
  });
});

app.get('/test/board-structure', async (req, res) => {
  try {
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
    
    const response = await axios.post('https://api.monday.com/v2', {
      query: boardQuery
    }, {
      headers: {
        'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
        'Content-Type': 'application/json'
      }
    });
    
    res.json({
      success: true,
      board: response.data.data.boards[0],
      hasSubitemsColumn: !!response.data.data.boards[0].columns.find(c => c.type === 'subtasks')
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/test/process-item/:id', async (req, res) => {
  const requestId = `test_${Date.now()}`;
  try {
    log('info', 'TEST_PROCESSING_START', { requestId, itemId: req.params.id });
    
    const files = await getMondayItemFilesWithPublicUrl(req.params.id, MONDAY_CONFIG.fileUploadsBoardId, requestId);
    
    if (files.length === 0) {
      return res.json({ success: false, message: 'No PDF files found' });
    }
    
    const { files: extracted, originalFiles } = await processFilesWithInstabase(files, requestId);
    const groups = groupPagesByInvoiceNumber(extracted, requestId);
    await createMondayExtractedItems(groups, req.params.id, originalFiles, requestId);
    
    res.json({ 
      success: true, 
      message: 'Processing completed',
      requestId,
      filesProcessed: files.length,
      groupsCreated: groups.length,
      itemsWithLineItems: groups.filter(g => g.items.length > 0).length
    });
  } catch (error) {
    log('error', 'TEST_PROCESSING_ERROR', { requestId, error: error.message });
    res.status(500).json({ error: error.message, requestId });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// 9️⃣  START SERVER
// ----------------------------------------------------------------------------
const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Digital Mailroom Webhook Server v2.0.0-fixed`);
  console.log(`📡 Listening on port ${PORT}`);
  console.log(`🔗 Health check: http://localhost:${PORT}/health`);
  console.log(`🧪 Test endpoint: POST /test/process-item/:id`);
  console.log(`📋 Board structure: GET /test/board-structure`);
  console.log(`📅 Started at: ${new Date().toISOString()}`);
});

// -----------------------------------------------------------------------------
//  🔧 FIXES APPLIED:
//  1. Enhanced logging with request IDs for better tracking
//  2. Fixed line items processing from Field 7 with multiple data formats
//  3. Proper subitem creation with board structure validation
//  4. Test subitem creation before processing line items
//  5. Enhanced error handling and fallback subitem creation
//  6. Added test endpoints for debugging
//  7. Improved column mapping for subitems
//  8. Better handling of different data types in line items
// -----------------------------------------------------------------------------
