// Digital Mailroom Webhook — FINAL VERSION with Updated Field Mapping + Line Number Support
// -----------------------------------------------------------------------------
// This version works with the updated Instabase field mapping (page type field removed)
// and includes Line Number extraction for subitems
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
        isMultiPage: g.isMultiPageReconstruction || false,
        pageCount: g.originalPageCount || 1
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
// 6️⃣  ENHANCED GROUPING WITH INTELLIGENT MULTI-PAGE DOCUMENT MERGING
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
  
  // 🔧 NEW: First pass - collect all pages by filename
  const pagesByFilename = {};
  
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
    
    // Group pages by filename
    if (!pagesByFilename[file.original_file_name]) {
      pagesByFilename[file.original_file_name] = [];
    }
    
    file.documents.forEach((doc, docIndex) => {
      const fields = doc.fields || {};
      
      pagesByFilename[file.original_file_name].push({
        fileIndex,
        docIndex,
        fields,
        fileName: file.original_file_name
      });
    });
  });
  
  log('info', 'PAGES_GROUPED_BY_FILENAME', {
    requestId,
    filenames: Object.keys(pagesByFilename),
    pagesByFile: Object.keys(pagesByFilename).map(filename => ({
      filename,
      pageCount: pagesByFilename[filename].length
    }))
  });
  
  // 🔧 NEW: Second pass - intelligent document reconstruction
  Object.keys(pagesByFilename).forEach(filename => {
    const pages = pagesByFilename[filename];
    
    log('info', 'RECONSTRUCTING_DOCUMENT', {
      requestId,
      filename,
      totalPages: pages.length
    });
    
    // Find the page with the invoice number (usually page 1, but not always)
    let masterPage = null;
    let invoiceNumber = null;
    
    // Strategy 1: Look for explicit invoice number
    for (const page of pages) {
      const candidateInvoiceNumber = page.fields['0']?.value;
      if (candidateInvoiceNumber && 
          candidateInvoiceNumber !== 'unknown' && 
          candidateInvoiceNumber !== 'none' && 
          candidateInvoiceNumber !== '') {
        masterPage = page;
        invoiceNumber = candidateInvoiceNumber;
        log('info', 'FOUND_INVOICE_NUMBER', {
          requestId,
          filename,
          invoiceNumber,
          pageIndex: pages.indexOf(page)
        });
        break;
      }
    }
    
    // Strategy 2: If no invoice number found, use filename as identifier
    if (!invoiceNumber) {
      // Extract potential invoice number from filename
      const filenameMatch = filename.match(/(\d{8,})/); // Look for 8+ digit numbers
      if (filenameMatch) {
        invoiceNumber = filenameMatch[1];
        masterPage = pages[0]; // Use first page as master
        log('info', 'EXTRACTED_INVOICE_FROM_FILENAME', {
          requestId,
          filename,
          extractedInvoiceNumber: invoiceNumber
        });
      } else {
        // Fallback: use filename without extension
        invoiceNumber = filename.replace(/\.[^/.]+$/, "");
        masterPage = pages[0];
        log('info', 'USING_FILENAME_AS_INVOICE', {
          requestId,
          filename,
          invoiceNumber
        });
      }
    }
    
    if (!masterPage) {
      log('warn', 'NO_MASTER_PAGE_FOUND', { requestId, filename });
      return;
    }
    
    // 🔧 NEW: Merge all pages for this document
    const mergedDocument = reconstructMultiPageDocument(pages, masterPage, requestId, filename);
    
    if (mergedDocument) {
      documentGroups[invoiceNumber] = mergedDocument;
      log('info', 'DOCUMENT_RECONSTRUCTED', {
        requestId,
        invoiceNumber,
        filename,
        totalPages: pages.length,
        lineItemCount: mergedDocument.items?.length || 0,
        totalAmount: mergedDocument.total_amount
      });
    }
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

// 🔧 UPDATED: Function to intelligently reconstruct multi-page documents - UPDATED FIELD MAPPING
function reconstructMultiPageDocument(pages, masterPage, requestId, filename) {
  log('info', 'RECONSTRUCTING_MULTI_PAGE_DOC', {
    requestId,
    filename,
    pageCount: pages.length,
    masterPageIndex: pages.indexOf(masterPage)
  });
  
  const masterFields = masterPage.fields;
  
  // 🔧 UPDATED: Extract basic info from master page with NEW field mapping (page type removed)
  const invoiceNumber = masterFields['0']?.value || filename.replace(/\.[^/.]+$/, "");
  const documentType = masterFields['1']?.value || 'invoice';  // Moved from field 2 to 1
  const supplier = masterFields['2']?.value || '';             // Moved from field 3 to 2
  const documentDate = masterFields['3']?.value || '';         // Moved from field 5 to 3
  const referenceNumber = masterFields['8']?.value || '';      // Reference number now in field 8
  
  log('info', 'BASIC_INFO_EXTRACTED', {
    requestId,
    filename,
    invoiceNumber,
    documentType,
    supplier,
    documentDate,
    referenceNumber
  });
  
  // 🔧 UPDATED: Merge due dates from field 4 (was field 6)
  let mergedDueDates = { due_date: '', due_date_2: '', due_date_3: '' };
  
  pages.forEach((page, pageIndex) => {
    const raw4 = page.fields['4'];  // Due date moved from field 6 to field 4
    if (raw4) {
      const dueDates = extractDueDatesFromField(raw4, requestId, pageIndex);
      
      // Merge due dates (keep first non-empty value found)
      if (dueDates.due_date && !mergedDueDates.due_date) {
        mergedDueDates.due_date = dueDates.due_date;
      }
      if (dueDates.due_date_2 && !mergedDueDates.due_date_2) {
        mergedDueDates.due_date_2 = dueDates.due_date_2;
      }
      if (dueDates.due_date_3 && !mergedDueDates.due_date_3) {
        mergedDueDates.due_date_3 = dueDates.due_date_3;
      }
    }
  });
  
  log('info', 'DUE_DATES_MERGED', {
    requestId,
    filename,
    mergedDueDates
  });
  
  // 🔧 UPDATED: Merge line items from field 5 (was field 7)
  const allLineItems = [];
  
  pages.forEach((page, pageIndex) => {
    log('info', 'PROCESSING_PAGE_FOR_ITEMS', {
      requestId,
      filename,
      pageIndex,
      availableFields: Object.keys(page.fields)
    });
    
    const pageItems = extractLineItemsFromPage(page, requestId, pageIndex);
    if (pageItems.length > 0) {
      allLineItems.push(...pageItems);
      log('info', 'ITEMS_FOUND_ON_PAGE', {
        requestId,
        filename,
        pageIndex,
        itemCount: pageItems.length,
        items: pageItems.map(item => ({
          line_number: item.line_number,
          item_number: item.item_number,
          description: item.description ? item.description.substring(0, 50) : ''
        }))
      });
    }
  });
  
  // 🔧 UPDATED: Find totals from field 6 and tax from field 7 (was fields 8 and 9)
  let totalAmount = 0;
  let taxAmount = 0;
  
  // Search all pages for totals (prioritize later pages)
  for (let i = pages.length - 1; i >= 0; i--) {
    const page = pages[i];
    const pageTotalAmount = page.fields['6']?.value;  // Total moved from field 8 to field 6
    const pageTaxAmount = page.fields['7']?.value;    // Tax moved from field 9 to field 7
    
    if (pageTotalAmount && !totalAmount) {
      const totalStr = String(pageTotalAmount).replace(/[^0-9.-]/g, '');
      totalAmount = parseFloat(totalStr) || 0;
      log('info', 'TOTAL_FOUND_ON_PAGE', {
        requestId,
        filename,
        pageIndex: i,
        totalAmount,
        rawValue: pageTotalAmount
      });
    }
    
    if (pageTaxAmount && !taxAmount) {
      const taxStr = String(pageTaxAmount).replace(/[^0-9.-]/g, '');
      taxAmount = parseFloat(taxStr) || 0;
      log('info', 'TAX_FOUND_ON_PAGE', {
        requestId,
        filename,
        pageIndex: i,
        taxAmount,
        rawValue: pageTaxAmount
      });
    }
    
    // If we found both, we can stop
    if (totalAmount && taxAmount) break;
  }
  
  const reconstructedDocument = {
    invoice_number: invoiceNumber,
    document_type: documentType,
    supplier_name: supplier,
    reference_number: referenceNumber,
    total_amount: totalAmount,
    tax_amount: taxAmount,
    document_date: documentDate,
    due_date: mergedDueDates.due_date,
    due_date_2: mergedDueDates.due_date_2,
    due_date_3: mergedDueDates.due_date_3,
    terms: '',  // Terms removed
    items: allLineItems,
    pages: pages.map((page, index) => ({
      page_type: `page_${index + 1}`,  // No longer extracting from field since removed
      file_name: filename,
      fields: page.fields
    })),
    confidence: 0,
    isMultiPageReconstruction: true,
    originalPageCount: pages.length,
    reconstructionStrategy: 'filename_grouping_with_content_merging'
  };
  
  log('info', 'DOCUMENT_RECONSTRUCTION_COMPLETE', {
    requestId,
    filename,
    invoiceNumber,
    totalAmount,
    taxAmount,
    lineItemCount: allLineItems.length,
    pageCount: pages.length,
    dueDatesFound: Object.values(mergedDueDates).filter(d => d).length
  });
  
  return reconstructedDocument;
}

// 🔧 UPDATED: Helper function to extract due dates from field 4 (was field 6)
function extractDueDatesFromField(raw4, requestId, pageIndex) {
  let dueDates = { due_date: '', due_date_2: '', due_date_3: '' };
  
  if (!raw4) return dueDates;
  
  const v = raw4?.value;
  
  if (typeof v === 'string') {
    dueDates.due_date = v.trim();
  } else if (Array.isArray(v)) {
    dueDates.due_date = v[0] || '';
    dueDates.due_date_2 = v[1] || '';
    dueDates.due_date_3 = v[2] || '';
  } else {
    dueDates.due_date = String(v);
  }
  
  return dueDates;
}

// 🔧 UPDATED: Helper function to extract line items - UPDATED FIELD MAPPING
function extractLineItemsFromPage(page, requestId, pageIndex) {
  const fields = page.fields || {};
  const raw5 = fields['5'];  // 🔧 UPDATED: Line items moved from field 7 to field 5
  const raw8 = fields['8'];  // 🔧 UPDATED: PO Number moved from field 11 to field 8 (Reference Number)
  let itemsData = [];
  let poNumber = '';
  
  // Extract PO number from reference field (field 8)
  if (raw8) {
    const v = raw8?.value;
    if (typeof v === 'string' && v.startsWith('SP')) {
      poNumber = v.trim();
    }
  }
  
  // 🔧 UPDATED: Extract line items from field 5 (was field 7)
  if (raw5) {
    const v = raw5?.value;
    
    if (typeof v === 'string') {
      try {
        itemsData = JSON.parse(v.trim());
      } catch (e) {
        log('error', 'JSON_PARSE_FAILED', { requestId, pageIndex, error: e.message });
      }
    } else if (Array.isArray(v)) {
      itemsData = v;
    }
  }
  
  // Try to find line items in ANY field that contains arrays (fallback)
  if (!Array.isArray(itemsData) || itemsData.length === 0) {
    Object.keys(fields).forEach(fieldKey => {
      const fieldValue = fields[fieldKey]?.value;
      if (Array.isArray(fieldValue) && fieldValue.length > 0 && itemsData.length === 0) {
        const firstItem = fieldValue[0];
        if (typeof firstItem === 'object' || Array.isArray(firstItem)) {
          itemsData = fieldValue;
          log('info', 'LINE_ITEMS_FOUND_IN_ALTERNATE_FIELD', {
            requestId,
            pageIndex,
            fieldKey,
            itemCount: fieldValue.length
          });
        }
      }
    });
  }
  
  if (!Array.isArray(itemsData) || itemsData.length === 0) {
    return [];
  }
  
  const processedItems = [];
  
  itemsData.forEach((row, rowIndex) => {
    let lineNumber = '';
    let itemNumber = '';
    let unitCost = 0;
    let quantity = 0;
    let description = '';
    let itemPoNumber = '';
    
    if (Array.isArray(row)) {
      lineNumber = String(row[0] || '').trim();
      itemNumber = String(row[1] || '').trim();
      unitCost = parseFloat(row[2]) || 0;
      quantity = parseFloat(row[3]) || 0;
      description = String(row[4] || '').trim();
      itemPoNumber = String(row[5] || '').trim();
    } else if (typeof row === 'object' && row !== null) {
      lineNumber = String(row['Line Number'] || row.line_number || row.lineNumber || row.line || '').trim();
      itemNumber = String(row['Item Number'] || row.item_number || row.itemNumber || row.number || row.item || row.Item || '').trim();
      unitCost = parseFloat(row['Unit Cost'] || row.unit_cost || row.unitCost || row.cost || row.price || 0);
      quantity = parseFloat(row.Quantity || row.quantity || row.qty || row.amount || 0);
      description = String(row.Description || row.description || row.desc || row.item_description || '').trim();
      itemPoNumber = String(row['PO Number'] || row.po_number || row.poNumber || row.po || '').trim();
      
      // Handle null PO Numbers
      if (itemPoNumber === 'null' || !itemPoNumber) {
        itemPoNumber = '';
      }
    } else {
      lineNumber = String(row || '').trim();
    }
    
    // Use item-specific PO if available, otherwise use page-level PO
    const finalPoNumber = itemPoNumber || poNumber;
    
    if (lineNumber || itemNumber || (unitCost > 0) || (quantity > 0)) {
      processedItems.push({
        line_number: lineNumber || `Item ${rowIndex + 1}`,
        item_number: itemNumber || `Item ${rowIndex + 1}`,
        description: description,
        quantity: quantity,
        unit_cost: unitCost,
        amount: quantity * unitCost,
        source_page: pageIndex + 1,
        po_number: finalPoNumber
      });
    }
  });
  
  return processedItems;
}

// ─────────────────────────────────────────────────────────────────────────────
// 7️⃣  ENHANCED MONDAY ITEMS CREATION WITH FIXED SOURCE ID & SUBITEM INDEXING
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
    
    // Find the actual "Source Request ID" column
    const sourceRequestIdColumn = columns.find(col => 
      col.title.toLowerCase() === 'id'
    );
    
    // Validate subitems column exists
    const subitemsColumn = columns.find(col => col.type === 'subtasks');
    
    for (const doc of documents) {
      log('info', 'CREATING_MONDAY_ITEM', {
        requestId,
        documentType: doc.document_type,
        invoiceNumber: doc.invoice_number,
        itemCount: doc.items?.length || 0
      });
      
      const formatDate = (dateStr) => {
        if (!dateStr || dateStr === 'Due Date' || dateStr === 'undefined' || dateStr === 'null') {
          return '';
        }
        try {
          let date;
          if (typeof dateStr === 'string') {
            const cleanDate = dateStr.trim();
            if (cleanDate.match(/^\d{4}-\d{2}-\d{2}$/)) {
              return cleanDate;
            }
            date = new Date(cleanDate);
          } else {
            date = new Date(dateStr);
          }
          
          if (isNaN(date.getTime())) {
            return '';
          }
          
          return date.toISOString().split('T')[0];
        } catch (e) {
          return '';
        }
      };
      
      const columnValues = buildColumnValues(columns, doc, formatDate, instabaseRunId);
      
      // Create the item
      const mutation = `
        mutation {
          create_item(
            board_id: ${MONDAY_CONFIG.extractedDocsBoardId}
            item_name: "${instabaseRunId || 'RUN_PENDING'}"
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
      
      // Create subitems if we have line items and subitems column exists
      if (subitemsColumn && doc.items && doc.items.length > 0) {
        try {
          await createSubitemsForLineItems(createdItemId, doc.items, columns, requestId);
        } catch (subitemError) {
          log('error', 'SUBITEM_CREATION_FAILED', {
            requestId,
            itemId: createdItemId,
            error: subitemError.message
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

// 🔧 UPDATED: Subitem creation function with Line Number support
async function createSubitemsForLineItems(parentItemId, items, columns, requestId) {
  try {
    log('info', 'SUBITEM_CREATION_START', { requestId, parentItemId, lineItemCount: items.length });

    // Get the linked board ID from your Subtasks column
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

    // Fetch that board's columns
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

    // Loop each line item and map into those columns
    for (let i = 0; i < items.length; i++) {
      const { line_number, item_number, quantity, unit_cost, description, source_page, po_number } = items[i];
      const columnValues = {};
      
      // Use extracted line_number for subitem indexing
      const actualLineNumber = line_number || `${i + 1}`;
      
      subitemColumns.forEach(col => {
        const title = col.title.trim().toLowerCase();
        
        // Map to Line Number column using extracted value
        if (title.includes('line number') || title.includes('line #') || title.includes('subitem') || title === 'index') {
          if (col.type === 'numbers') {
            const numericLineNumber = parseFloat(actualLineNumber);
            columnValues[col.id] = isNaN(numericLineNumber) ? (i + 1) : numericLineNumber;
          } else {
            columnValues[col.id] = actualLineNumber;
          }
        } else if (title === 'item number' || title.includes('item num')) {
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
        } else if (title.includes('source page') || title.includes('page')) {
          columnValues[col.id] = source_page || '';
        } else if (title.includes('po number') || title === 'po' || title.includes('purchase order')) {
          columnValues[col.id] = po_number || '';
        }
      });

      // Use extracted line number in subitem name
      const subitemName = `Line ${actualLineNumber}`;
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
          extractedLineNumber: actualLineNumber, 
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
            extractedLineNumber: actualLineNumber,
            errors: retryResponse.data.errors
          });
        } else {
          log('info', 'SIMPLE_SUBITEM_SUCCESS', {
            requestId,
            extractedLineNumber: actualLineNumber,
            subitemId: retryResponse.data.data.create_subitem.id,
            subitemName: escapedName
          });
        }
      } else {
        log('info', 'SUBITEM_CREATED', { 
          requestId, 
          extractedLineNumber: actualLineNumber, 
          subitemId: response.data.data.create_subitem.id,
          subitemName: escapedName,
          poNumber: po_number
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
  }
}

// buildColumnValues function
function buildColumnValues(columns, doc, formatDate, instabaseRunId = null) {
  const columnValues = {};
  
  columns.forEach(col => {
    const title = col.title.toLowerCase();
    const id = col.id;
    const type = col.type;
    
    if (title.includes('supplier')) {
      columnValues[id] = doc.supplier_name || '';
    } else if (title === 'id') {
      columnValues[id] = instabaseRunId || '';
    } else if (title.includes('reference number') || title === 'reference number' || title.includes('reference')) {
      columnValues[id] = doc.reference_number || '';
    } else if (title.includes('document number') || (title.includes('number') && !title.includes('total') && !title.includes('reference') && !title.includes('source') && !title.includes('id'))) {
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
      const formattedDate = formatDate(doc.due_date);
      if (formattedDate) {
        columnValues[id] = formattedDate;
      }
    } else if (title.includes('due date 2')) {
      const formattedDate = formatDate(doc.due_date_2);
      if (formattedDate) {
        columnValues[id] = formattedDate;
      }
    } else if (title.includes('due date 3')) {
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
    version: '4.1.2-updated-field-mapping'
  });
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
      message: 'Processing completed with updated field mapping and line number indexing',
      requestId,
      instabaseRunId: runId,
      filesProcessed: files.length,
      groupsCreated: groups.length,
      itemsWithLineItems: groups.filter(g => g.items.length > 0).length,
      reconstructionDetails: groups.map(g => ({
        invoiceNumber: g.invoice_number,
        lineItemCount: g.items?.length || 0,
        totalAmount: g.total_amount,
        lineNumbers: g.items?.map(item => item.line_number) || []
      }))
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
  console.log(`🚀 Digital Mailroom Webhook Server v4.1.2-updated-field-mapping`);
  console.log(`📡 Listening on port ${PORT}`);
  console.log(`🔗 Health check: http://localhost:${PORT}/health`);
  console.log(`🧪 Test endpoint: POST /test/process-item/:id`);
  console.log(`📅 Started at: ${new Date().toISOString()}`);
  console.log(`🔧 Features: Updated field mapping, line number indexing, PO number tracking`);
});
