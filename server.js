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

    const { files: extracted, originalFiles, runId } = await processFilesWithInstabase(pdfFiles, requestId);
    const groups = groupPagesByInvoiceNumber(extracted, requestId);
    
    // 🔧 DEBUG: Log the groups after processing to verify items are found
    log('info', 'GROUPS_AFTER_PROCESSING', {
      requestId,
      groupCount: groups.length,
      groups: groups.map(g => ({
        invoiceNumber: g.invoice_number,
        itemCount: g.items?.length || 0,
        items: g.items || []
      }))
    });
    
    await createMondayExtractedItems(groups, itemId, originalFiles, requestId, runId);
    
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
      originalFiles: originalFiles,
      runId: runId  // Return the run ID so we can link it
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
      
      // 🔧 ENHANCED FIELD 7 DEBUGGING - Show the actual structure
      if (fields['7']) {
        log('info', 'FIELD_7_RAW_STRUCTURE', {
          requestId,
          docIndex,
          field7Raw: JSON.stringify(fields['7'], null, 2)
        });
        
        log('info', 'FIELD_7_DETAILED', {
          requestId,
          docIndex,
          field7Exists: true,
          field7Value: fields['7']?.value,
          field7ValueType: typeof fields['7']?.value,
          field7ValueStringified: JSON.stringify(fields['7']?.value)
        });
      } else {
        log('info', 'FIELD_7_MISSING', { requestId, docIndex });
      }
      
      // Check ALL fields for potential due date data
      Object.keys(fields).forEach(fieldKey => {
        const fieldValue = fields[fieldKey]?.value;
        
        // Log arrays
        if (Array.isArray(fieldValue) && fieldValue.length > 0) {
          log('info', 'ARRAY_FIELD_FOUND', {
            requestId,
            fieldKey,
            arrayLength: fieldValue.length,
            firstElement: fieldValue[0],
            fullArray: fieldValue
          });
        }
        
        // Log potential date fields
        if (fieldValue && typeof fieldValue === 'string' && 
            (fieldValue.includes('2025') || fieldValue.includes('2024') || fieldValue.includes('2026'))) {
          log('info', 'POTENTIAL_DATE_FIELD', {
            requestId,
            fieldKey,
            value: fieldValue
          });
        }
      });
      
      // 🔧 DEBUG: Show raw Field 6 structure for due dates
      if (fields['6']) {
        log('info', 'FIELD_6_RAW_STRUCTURE', {
          requestId,
          docIndex,
          field6Raw: JSON.stringify(fields['6'], null, 2)
        });
      }
      
      // Extract field values
      const invoiceNumber = fields['0']?.value || 'unknown';
      const pageType = fields['1']?.value || 'unknown';
      const documentType = fields['2']?.value || 'invoice';
      const supplier = fields['3']?.value || '';
      const terms = fields['4']?.value || '';
      const documentDate = fields['5']?.value || '';
      
      // 🔧 NEW: Extract Reference Number from Field 10
      const referenceNumber = fields['10']?.value || '';
      
      log('info', 'REFERENCE_NUMBER_EXTRACTED', {
        requestId,
        docIndex,
        referenceNumber,
        field10Raw: fields['10']
      });
      
      // 🔧 FIXED: Extract due dates from Field 6 (can be table format with header row)
      const raw6 = fields['6'];
      let dueDates = { due_date: '', due_date_2: '', due_date_3: '' };
      
      if (raw6) {
        const v = raw6?.value;
        
        if (typeof v === 'string') {
          try {
            const parsed = JSON.parse(v.trim());
            if (Array.isArray(parsed)) {
              // Handle array of due dates - skip header row (index 0)
              dueDates.due_date = parsed[1] || '';     // Row 1 = first due date
              dueDates.due_date_2 = parsed[2] || '';   // Row 2 = second due date  
              dueDates.due_date_3 = parsed[3] || '';   // Row 3 = third due date
              log('info', 'DUE_DATES_PARSED_FROM_JSON_ARRAY_SKIP_HEADER', { 
                requestId, 
                rawArray: parsed,
                header: parsed[0],
                mappedDates: dueDates
              });
            } else {
              // Single date as string
              dueDates.due_date = String(parsed);
              log('info', 'SINGLE_DUE_DATE_PARSED', { requestId, dueDate: dueDates.due_date });
            }
          } catch (e) {
            // Not JSON, treat as single date string
            dueDates.due_date = String(v);
            log('info', 'DUE_DATE_AS_STRING', { requestId, dueDate: dueDates.due_date });
          }
        } else if (Array.isArray(v)) {
          // Direct array of due dates - skip header row (index 0)
          dueDates.due_date = v[1] || '';     // Row 1 = first due date
          dueDates.due_date_2 = v[2] || '';   // Row 2 = second due date
          dueDates.due_date_3 = v[3] || '';   // Row 3 = third due date
          log('info', 'DUE_DATES_DIRECT_ARRAY_SKIP_HEADER', { 
            requestId, 
            rawArray: v,
            header: v[0],
            mappedDates: dueDates
          });
        } else if (v?.tables && Array.isArray(v.tables[0]?.rows)) {
          // Table format - extract dates from rows, skip header row (index 0)
          const rows = v.tables[0].rows;
          dueDates.due_date = rows[1] || '';     // Row 1 = first due date
          dueDates.due_date_2 = rows[2] || '';   // Row 2 = second due date
          dueDates.due_date_3 = rows[3] || '';   // Row 3 = third due date
          log('info', 'DUE_DATES_FROM_TABLE_SKIP_HEADER', { 
            requestId, 
            tableRows: rows,
            header: rows[0],
            mappedDates: dueDates
          });
        } else if (v?.rows) {
          // Direct rows format - skip header row (index 0)
          dueDates.due_date = v.rows[1] || '';     // Row 1 = first due date
          dueDates.due_date_2 = v.rows[2] || '';   // Row 2 = second due date
          dueDates.due_date_3 = v.rows[3] || '';   // Row 3 = third due date
          log('info', 'DUE_DATES_FROM_ROWS_SKIP_HEADER', { 
            requestId, 
            rows: v.rows,
            header: v.rows[0],
            mappedDates: dueDates
          });
        } else {
          // Single value
          dueDates.due_date = String(v);
          log('info', 'DUE_DATE_SINGLE_VALUE', { requestId, dueDate: dueDates.due_date });
        }
      } else {
        log('info', 'NO_DUE_DATE_FIELD', { requestId });
      }
      
      // 🔧 FIXED: Extract line items from the correct nested structure
      const raw7 = fields['7'];
      let itemsData = [];
      
      if (raw7) {
        const v = raw7?.value;
        
        if (typeof v === 'string') {
          try {
            itemsData = JSON.parse(v.trim());
            log('info', 'ITEMS_PARSED_FROM_JSON_STRING', { requestId, itemCount: Array.isArray(itemsData) ? itemsData.length : 'N/A' });
          } catch (e) {
            log('error', 'JSON_PARSE_FAILED', { requestId, error: e.message, raw: v });
          }
        } else if (Array.isArray(v)) {
          itemsData = v;
          log('info', 'ITEMS_FOUND_DIRECT_ARRAY', { requestId, itemCount: itemsData.length });
        } else if (v?.tables && Array.isArray(v.tables[0]?.rows)) {
          itemsData = v.tables[0].rows;
          log('info', 'ITEMS_FOUND_IN_TABLES', { requestId, itemCount: itemsData.length });
        } else if (v?.rows) {
          itemsData = v.rows;
          log('info', 'ITEMS_FOUND_IN_ROWS', { requestId, itemCount: itemsData.length });
        } else if (raw7.tables?.[0]?.rows) {
          itemsData = raw7.tables[0].rows;
          log('info', 'ITEMS_FOUND_ROOT_TABLES', { requestId, itemCount: itemsData.length });
        } else if (raw7.rows) {
          itemsData = raw7.rows;
          log('info', 'ITEMS_FOUND_ROOT_ROWS', { requestId, itemCount: itemsData.length });
        } else {
          log('info', 'NO_RECOGNIZABLE_TABLE_STRUCTURE', { 
            requestId, 
            field7Keys: Object.keys(raw7),
            field7ValueKeys: raw7.value ? Object.keys(raw7.value) : 'No value object'
          });
        }
      }
      
      log('info', 'RESOLVED_ITEMS_DATA', { 
        requestId, 
        itemsLength: Array.isArray(itemsData) ? itemsData.length : 'N/A',
        itemsType: typeof itemsData
      });
      
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
        itemsDataLength: Array.isArray(itemsData) ? itemsData.length : 'N/A',
        dueDatesExtracted: dueDates,
        field6Raw: fields['6']?.value
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
          reference_number: referenceNumber,
          total_amount: 0,
          tax_amount: 0,
          document_date: documentDate,
          due_date: dueDates.due_date,
          due_date_2: dueDates.due_date_2,
          due_date_3: dueDates.due_date_3,
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
        if (referenceNumber) group.reference_number = referenceNumber;
        if (totalAmount) {
          const totalStr = String(totalAmount).replace(/[^0-9.-]/g, '');
          group.total_amount = parseFloat(totalStr) || 0;
        }
        if (taxAmount) {
          const taxStr = String(taxAmount).replace(/[^0-9.-]/g, '');
          group.tax_amount = parseFloat(taxStr) || 0;
        }
        if (documentDate) group.document_date = documentDate;
        if (dueDates.due_date) group.due_date = dueDates.due_date;
        if (dueDates.due_date_2) group.due_date_2 = dueDates.due_date_2;
        if (dueDates.due_date_3) group.due_date_3 = dueDates.due_date_3;
        if (terms) group.terms = terms;
      }
      
      // 🔧 ENHANCED LINE ITEMS PROCESSING
      log('info', 'LINE_ITEMS_CHECK', { 
        requestId, 
        pageType,
        hasItemsData: !!itemsData,
        itemsDataType: typeof itemsData,
        itemsDataIsArray: Array.isArray(itemsData),
        itemsDataLength: Array.isArray(itemsData) ? itemsData.length : 'N/A',
        itemsDataContent: itemsData
      });
      
      // Try to find line items in ANY field that contains arrays
      let foundLineItems = [];
      let sourceField = null;
      
      // First try Field 7 (our expected field)
      if (itemsData && Array.isArray(itemsData) && itemsData.length > 0) {
        foundLineItems = itemsData;
        sourceField = '7';
        log('info', 'LINE_ITEMS_FROM_FIELD_7', { requestId, itemCount: itemsData.length });
      } else {
        // Search all fields for arrays that might contain line items
        Object.keys(fields).forEach(fieldKey => {
          const fieldValue = fields[fieldKey]?.value;
          if (Array.isArray(fieldValue) && fieldValue.length > 0 && !foundLineItems.length) {
            // Check if this looks like line items data
            const firstItem = fieldValue[0];
            if (typeof firstItem === 'object' || Array.isArray(firstItem)) {
              foundLineItems = fieldValue;
              sourceField = fieldKey;
              log('info', 'LINE_ITEMS_FOUND_IN_ALTERNATE_FIELD', {
                requestId,
                fieldKey,
                itemCount: fieldValue.length,
                sampleItem: firstItem
              });
            }
          }
        });
      }
      
      if (foundLineItems.length > 0) {
        log('info', 'PROCESSING_LINE_ITEMS', { 
          requestId, 
          pageType,
          sourceField,
          tableRowCount: foundLineItems.length,
          firstRowSample: foundLineItems[0],
          allItems: foundLineItems
        });
        
        const processedItems = [];
        
        foundLineItems.forEach((row, rowIndex) => {
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
            pageType,
            sourceField
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
async function createMondayExtractedItems(documents, sourceItemId, originalFiles, requestId, instabaseRunId) {
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
        if (!dateStr || dateStr === 'Due Date' || dateStr === 'undefined' || dateStr === 'null') {
          return '';
        }
        try {
          // Handle various date formats
          let date;
          if (typeof dateStr === 'string') {
            // Clean the date string
            const cleanDate = dateStr.trim();
            if (cleanDate.match(/^\d{4}-\d{2}-\d{2}$/)) {
              // Already in YYYY-MM-DD format
              return cleanDate;
            }
            date = new Date(cleanDate);
          } else {
            date = new Date(dateStr);
          }
          
          if (isNaN(date.getTime())) {
            log('warn', 'INVALID_DATE_FORMAT', { 
              requestId, 
              originalDate: dateStr,
              reason: 'Date parsing failed'
            });
            return '';
          }
          
          return date.toISOString().split('T')[0];
        } catch (e) {
          log('warn', 'DATE_FORMAT_ERROR', { 
            requestId, 
            originalDate: dateStr,
            error: e.message
          });
          return '';
        }
      };
      
      const columnValues = buildColumnValues(columns, doc, formatDate);
      
      log('info', 'COLUMN_VALUES_MAPPED', { 
        requestId, 
        invoiceNumber: doc.invoice_number,
        columnValues,
        docDueDates: {
          due_date: doc.due_date,
          due_date_2: doc.due_date_2,
          due_date_3: doc.due_date_3
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
      
      // 🔧 NEW: Update the Source Request ID column with Instabase Run ID
      if (instabaseRunId) {
        try {
          const updateMutation = `
            mutation {
              change_column_value(
                item_id: ${createdItemId}
                board_id: ${MONDAY_CONFIG.extractedDocsBoardId}
                column_id: "name"
                value: "${escapedDocumentType.toUpperCase()} ${escapedInvoiceNumber} (${instabaseRunId})"
              ) {
                id
              }
            }
          `;
          
          await axios.post('https://api.monday.com/v2', {
            query: updateMutation
          }, {
            headers: {
              'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
              'Content-Type': 'application/json'
            }
          });
          
          log('info', 'INSTABASE_RUN_ID_LINKED', {
            requestId,
            itemId: createdItemId,
            instabaseRunId: instabaseRunId
          });
        } catch (linkError) {
          log('error', 'FAILED_TO_LINK_RUN_ID', {
            requestId,
            itemId: createdItemId,
            error: linkError.message
          });
        }
      }
      
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

// 🔧 FIXED: Subitem creation function
async function createSubitemsForLineItems(parentItemId, items, columns, requestId) {
  try {
    log('info', 'SUBITEM_CREATION_START', { requestId, parentItemId, lineItemCount: items.length });

    // 1️⃣ Get the linked board ID from your Subtasks column
    const subitemsColumn = columns.find(c => c.type === 'subtasks');
    if (!subitemsColumn) {
      log('error', 'NO_SUBTASKS_COLUMN_FOUND', { requestId });
      return;
    }

    let settings = {};
    try {
      settings = JSON.parse(subitemsColumn.settings_str || '{}');
    } catch (e) {
      log('error', 'SETTINGS_PARSE_ERROR', { requestId, error: e.message });
      return;
    }

    const subitemBoardId = Array.isArray(settings.boardIds)
      ? settings.boardIds[0]
      : settings.linked_board_id;
    
    if (!subitemBoardId) {
      log('error', 'MISSING_SUBITEM_BOARD_ID', { requestId, settings });
      return;
    }

    log('info', 'SUBITEM_BOARD_IDENTIFIED', { requestId, subitemBoardId });

    // 2️⃣ Fetch that board's columns
    const colsQuery = `
      query {
        boards(ids: [${subitemBoardId}]) {
          columns { id title type }
        }
      }
    `;
    
    const colsResponse = await axios.post('https://api.monday.com/v2', { query: colsQuery }, {
      headers: { Authorization: `Bearer ${MONDAY_CONFIG.apiKey}` }
    });
    
    const subitemColumns = colsResponse.data.data.boards[0].columns;
    
    log('info', 'SUBITEM_COLUMNS_FETCHED', {
      requestId,
      subitemBoardId,
      columnCount: subitemColumns.length,
      columns: subitemColumns.map(c => ({ 
        id: c.id, 
        title: c.title, 
        type: c.type,
        titleLowercase: c.title.trim().toLowerCase()
      }))
    });

    // 3️⃣ Loop each line item and map into those columns
    for (let i = 0; i < items.length; i++) {
      const { item_number, quantity, unit_cost, description } = items[i];
      const columnValues = {};
      
      subitemColumns.forEach(col => {
        const title = col.title.trim().toLowerCase();
        if (title === 'item number' || title.includes('item num')) {
          columnValues[col.id] = item_number || '';
        } else if (title === 'quantity' || title === 'qty') {
          if (col.type === 'numbers') {
            columnValues[col.id] = quantity || 0;
          } else {
            columnValues[col.id] = String(quantity || 0);
          }
        } else if (title === 'unit cost' || title.includes('unit cost')) {
          if (col.type === 'numbers') {
            columnValues[col.id] = unit_cost || 0;
          } else {
            columnValues[col.id] = String(unit_cost || 0);
          }
        } else if (title === 'description' || title.includes('desc')) {
          columnValues[col.id] = description || '';
        }
      });

      log('info', 'SUBITEM_COLUMN_MAPPING', {
        requestId,
        lineIndex: i + 1,
        itemNumber: item_number,
        columnValues
      });

      // 4️⃣ Create the subitem
      const subitemName = item_number || `Line Item ${i + 1}`;
      const escapedName = subitemName.replace(/"/g, '\\"');
      const columnValuesJson = JSON.stringify(columnValues);
      
      const mutation = `
        mutation {
          create_subitem(
            parent_item_id: ${parentItemId}
            item_name: "${escapedName}"
            column_values: ${JSON.stringify(columnValuesJson)}
          ) { 
            id 
            name
          }
        }
      `;
      
      log('info', 'CREATING_SUBITEM', {
        requestId,
        lineIndex: i + 1,
        subitemName: escapedName,
        mutation
      });
      
      const response = await axios.post('https://api.monday.com/v2', { query: mutation }, {
        headers: {
          Authorization: `Bearer ${MONDAY_CONFIG.apiKey}`,
          'Content-Type': 'application/json',
          'API-Version': '2024-04'
        }
      });
      
      if (response.data.errors) {
        log('error', 'SUBITEM_MUTATION_ERRORS', { 
          requestId, 
          lineIndex: i + 1, 
          errors: response.data.errors 
        });
        
        // Try creating without column values if there are errors
        const simpleQuery = `
          mutation {
            create_subitem(
              parent_item_id: ${parentItemId}
              item_name: "${escapedName}"
            ) { 
              id 
              name
            }
          }
        `;
        
        const retryResponse = await axios.post('https://api.monday.com/v2', { query: simpleQuery }, {
          headers: {
            Authorization: `Bearer ${MONDAY_CONFIG.apiKey}`,
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
            subitemName: escapedName
          });
        }
      } else {
        log('info', 'SUBITEM_CREATED', { 
          requestId, 
          lineIndex: i + 1, 
          subitemId: response.data.data.create_subitem.id,
          subitemName: escapedName
        });
      }
      
      // Small pause to avoid rate limits
      await new Promise(resolve => setTimeout(resolve, 250));
    }

    log('info', 'ALL_SUBITEMS_CREATED', { requestId, parentItemId, lineItemCount: items.length });
    
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
    
    // 🔧 DEBUG: Log each column mapping attempt
    if (title.includes('reference')) {
      console.log(`[DEBUG] Found reference column: ${col.title} (${col.id}) - value: ${doc.reference_number}`);
    }
    
    if (title.includes('supplier')) {
      columnValues[id] = doc.supplier_name || '';
    } else if (title.includes('reference number') || title === 'reference number' || title.includes('reference')) {
      columnValues[id] = doc.reference_number || '';
      console.log(`[DEBUG] Mapped reference number: ${doc.reference_number} to column ${col.id}`);
    } else if (title.includes('document number') || (title.includes('number') && !title.includes('total') && !title.includes('reference'))) {
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
      // 🔧 FIXED: Only set due date if it's a valid date, not empty
      const formattedDate = formatDate(doc.due_date);
      if (formattedDate) {
        columnValues[id] = formattedDate;
      }
    } else if (title.includes('due date 2')) {
      // 🔧 FIXED: Only set due date 2 if it's a valid date, not empty
      const formattedDate = formatDate(doc.due_date_2);
      if (formattedDate) {
        columnValues[id] = formattedDate;
      }
    } else if (title.includes('due date 3')) {
      // 🔧 FIXED: Only set due date 3 if it's a valid date, not empty
      const formattedDate = formatDate(doc.due_date_3);
      if (formattedDate) {
        columnValues[id] = formattedDate;
      }
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

app.get('/test/subitem-board-structure', async (req, res) => {
  try {
    // Get main board structure
    const mainBoardQuery = `
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
    
    const mainBoardResponse = await axios.post('https://api.monday.com/v2', {
      query: mainBoardQuery
    }, {
      headers: {
        'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
        'Content-Type': 'application/json'
      }
    });
    
    const mainColumns = mainBoardResponse.data.data.boards[0].columns;
    const subitemsColumn = mainColumns.find(col => col.type === 'subtasks');
    
    if (!subitemsColumn) {
      return res.json({ 
        success: false, 
        error: 'No subitems column found',
        mainBoardColumns: mainColumns
      });
    }
    
    // Parse subitems column settings to get linked board ID
    let settings = {};
    try {
      settings = JSON.parse(subitemsColumn.settings_str || '{}');
    } catch (e) {
      return res.json({
        success: false,
        error: 'Could not parse subitems column settings',
        rawSettings: subitemsColumn.settings_str
      });
    }
    
    const subitemBoardId = Array.isArray(settings.boardIds)
      ? settings.boardIds[0]
      : settings.linked_board_id;
    
    if (!subitemBoardId) {
      return res.json({
        success: false,
        error: 'No subitem board ID found in settings',
        settings: settings
      });
    }
    
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
            settings_str
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
    
    const subitemColumns = subitemBoardResponse.data.data.boards[0].columns;
    
    res.json({
      success: true,
      mainBoard: {
        id: MONDAY_CONFIG.extractedDocsBoardId,
        name: mainBoardResponse.data.data.boards[0].name,
        subitemsColumn: {
          id: subitemsColumn.id,
          title: subitemsColumn.title,
          settings: settings
        }
      },
      subitemBoard: {
        id: subitemBoardId,
        name: subitemBoardResponse.data.data.boards[0].name,
        columns: subitemColumns.map(col => ({
          id: col.id,
          title: col.title,
          type: col.type,
          titleLowercase: col.title.trim().toLowerCase()
        }))
      }
    });
    
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      error: error.message,
      stack: error.stack 
    });
  }
});

app.post('/test/debug-extraction/:id', async (req, res) => {
  const requestId = `debug_${Date.now()}`;
  try {
    log('info', 'DEBUG_EXTRACTION_START', { requestId, itemId: req.params.id });
    
    // Get files
    const files = await getMondayItemFilesWithPublicUrl(req.params.id, MONDAY_CONFIG.fileUploadsBoardId, requestId);
    
    if (files.length === 0) {
      return res.json({ success: false, message: 'No PDF files found' });
    }
    
    // Process with Instabase
    const { files: extracted, originalFiles } = await processFilesWithInstabase(files, requestId);
    
    // Log the RAW extraction results
    log('info', 'RAW_INSTABASE_RESULTS', {
      requestId,
      extractedFiles: extracted,
      fileCount: extracted?.length || 0
    });
    
    if (extracted && extracted.length > 0) {
      extracted.forEach((file, fileIndex) => {
        log('info', 'FILE_DETAILS', {
          requestId,
          fileIndex,
          fileName: file.original_file_name,
          documentCount: file.documents?.length || 0
        });
        
        if (file.documents) {
          file.documents.forEach((doc, docIndex) => {
            log('info', 'DOCUMENT_DETAILS', {
              requestId,
              fileIndex,
              docIndex,
              fields: doc.fields,
              fieldKeys: Object.keys(doc.fields || {}),
              field7: doc.fields?.['7'],
              allFieldsWithValues: Object.keys(doc.fields || {}).map(key => ({
                fieldKey: key,
                value: doc.fields[key]?.value,
                type: typeof doc.fields[key]?.value,
                isArray: Array.isArray(doc.fields[key]?.value)
              }))
            });
          });
        }
      });
    }
    
    // Also run grouping to see what happens
    const groups = groupPagesByInvoiceNumber(extracted, requestId);
    
    res.json({ 
      success: true, 
      message: 'Debug extraction completed - check logs for detailed results',
      requestId,
      filesProcessed: files.length,
      extractedFiles: extracted?.length || 0,
      groupsCreated: groups.length,
      rawExtractionPreview: {
        firstFile: extracted?.[0]?.original_file_name,
        firstDocument: extracted?.[0]?.documents?.[0]?.fields ? Object.keys(extracted[0].documents[0].fields) : 'No fields',
        field7Content: extracted?.[0]?.documents?.[0]?.fields?.['7']?.value
      }
    });
  } catch (error) {
    log('error', 'DEBUG_EXTRACTION_ERROR', { requestId, error: error.message });
    res.status(500).json({ error: error.message, requestId });
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
    
    const { files: extracted, originalFiles, runId } = await processFilesWithInstabase(files, requestId);
    const groups = groupPagesByInvoiceNumber(extracted, requestId);
    await createMondayExtractedItems(groups, req.params.id, originalFiles, requestId, runId);
    
    res.json({ 
      success: true, 
      message: 'Processing completed',
      requestId,
      instabaseRunId: runId,
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
  console.log(`🔬 Debug endpoint: POST /test/debug-extraction/:id`);
  console.log(`📋 Board structure: GET /test/board-structure`);
  console.log(`📅 Started at: ${new Date().toISOString()}`);
});

// -----------------------------------------------------------------------------
//  🔧 FIXES APPLIED:
//  1. Fixed all syntax errors and missing closing braces
//  2. Enhanced Field 7 extraction with JSON string parsing
//  3. Proper subitem creation with board structure validation
//  4. Enhanced error handling and fallback subitem creation
//  5. Added comprehensive debug endpoints
//  6. Improved column mapping for subitems
//  7. Better handling of different data types in line items
//  8. Complete logging system for debugging
// -----------------------------------------------------------------------------
