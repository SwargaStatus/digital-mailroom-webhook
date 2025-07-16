
// Digital Mailroom Webhook — FINAL VERSION with Updated Field Mapping + Line Number Support
// -----------------------------------------------------------------------------
// This version works with the updated Instabase field mapping (page type field removed)
// and includes Line Number extraction for subitems
// -----------------------------------------------------------------------------

const express   = require('express');
const axios     = require('axios');
const FormData  = require('form-data');
const PDFDocument = require('pdf-lib').PDFDocument;

const app = express();
app.use(express.json());

// ─────────────────────────────────────────────────────────────────────────────
// 1️⃣  CONFIG  –  **unchanged / hard‑coded**
// ----------------------------------------------------------------------------
const INSTABASE_CONFIG = {
  baseUrl:       'https://aihub.instabase.com',
  apiKey:        'GTovDkdhQjNTbSUQ1Nh201xvZvmM00',
  deploymentId:  '01980ed0-7584-7e30-aad8-2b286de010f4',
  headers: {
    'IB-Context':   'stu',
    'Authorization': 'Bearer GTovDkdhQjNTbSUQ1Nh201xvZvmM00',
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

// 🔧 NEW: PDF PAGE EXTRACTION FUNCTION
// ----------------------------------------------------------------------------
async function createInvoiceSpecificPDF(originalPdfBuffer, invoiceNumber, pageIndices, requestId) {
  try {
    log('info', 'CREATING_INVOICE_SPECIFIC_PDF', {
      requestId,
      invoiceNumber,
      pageIndices,
      originalBufferSize: originalPdfBuffer.length
    });

    // Load the original PDF
    const originalPdf = await PDFDocument.load(originalPdfBuffer);
    const totalPages = originalPdf.getPageCount();
    
    log('info', 'ORIGINAL_PDF_INFO', {
      requestId,
      invoiceNumber,
      totalPagesInOriginal: totalPages,
      requestedPageIndices: pageIndices
    });
    
    // Create a new PDF document
    const newPdf = await PDFDocument.create();
    
    // Validate and copy only the pages for this specific invoice
    const validPageIndices = pageIndices.filter(pageIndex => {
      if (pageIndex >= 0 && pageIndex < totalPages) {
        return true;
      } else {
        log('warn', 'INVALID_PAGE_INDEX', {
          requestId,
          invoiceNumber,
          pageIndex,
          totalPages
        });
        return false;
      }
    });
    
    if (validPageIndices.length === 0) {
      throw new Error(`No valid page indices found for invoice ${invoiceNumber}`);
    }
    
    for (const pageIndex of validPageIndices) {
      const [copiedPage] = await newPdf.copyPages(originalPdf, [pageIndex]);
      newPdf.addPage(copiedPage);
      
      log('info', 'PAGE_COPIED_TO_INVOICE_PDF', {
        requestId,
        invoiceNumber,
        pageIndex,
        totalPages
      });
    }
    
    // Generate the PDF buffer
    const pdfBytes = await newPdf.save();
    
    log('info', 'INVOICE_PDF_CREATED', {
      requestId,
      invoiceNumber,
      requestedPageCount: pageIndices.length,
      validPageCount: validPageIndices.length,
      outputSize: pdfBytes.length
    });
    
    return Buffer.from(pdfBytes);
    
  } catch (error) {
    log('error', 'CREATE_INVOICE_PDF_FAILED', {
      requestId,
      invoiceNumber,
      pageIndices,
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
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
    await updateMondayItemStatusToDone(ev.pulseId, ev.boardId, requestId, runId);
  } catch (err) {
    log('error', 'BACKGROUND_PROCESSING_FAILED', { 
      requestId, 
      error: err.message, 
      stack: err.stack 
    });
  }
}

// Function to update the status column to 'Done' for the original item
async function updateMondayItemStatusToDone(itemId, boardId, requestId, runId = null) {
  const statusColumnId = "status"; // Change if your column ID is different
  const doneLabel = "Completed"; // The label as it appears in Monday.com
  const processingNotesColumnTitle = "Processing Notes"; // Adjust if your column is named differently

  // 1. Fetch the board columns to get the status column settings
  const boardQuery = `
    query {
      boards(ids: [${boardId}]) {
        columns {
          id
          title
          type
          settings_str
        }
      }
    }
  `;

  try {
    const boardResponse = await axios.post('https://api.monday.com/v2', { query: boardQuery }, {
      headers: {
        'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
        'Content-Type': 'application/json'
      }
    });

    const columns = boardResponse.data.data?.boards?.[0]?.columns || [];
    const statusColumn = columns.find(col => col.id === statusColumnId && col.type === 'status');
    if (!statusColumn) {
      log('error', 'STATUS_COLUMN_NOT_FOUND', { requestId, itemId, boardId });
      return;
    }

    // 2. Parse settings_str to get the labels and their indexes
    let doneIndex = null;
    try {
      const settings = JSON.parse(statusColumn.settings_str);
      if (settings.labels) {
        for (const [index, label] of Object.entries(settings.labels)) {
          if (label && label.trim().toLowerCase() === doneLabel.toLowerCase()) {
            doneIndex = parseInt(index, 10);
            break;
          }
        }
      }
    } catch (e) {
      log('error', 'STATUS_COLUMN_SETTINGS_PARSE_ERROR', { requestId, error: e.message });
      return;
    }

    if (doneIndex === null) {
      log('error', 'DONE_LABEL_NOT_FOUND', { requestId, itemId, boardId, labels: statusColumn.settings_str });
      return;
    }

    // Find the Processing Notes text column
    let processingNotesColumn = null;
    if (runId) {
      processingNotesColumn = columns.find(col => col.title.trim().toLowerCase() === processingNotesColumnTitle.toLowerCase() && col.type === 'text');
    }

    // Build the value object for the mutation
    let valueObj = { index: doneIndex };
    let columnValues = { [statusColumnId]: valueObj };
    if (runId && processingNotesColumn) {
      columnValues[processingNotesColumn.id] = runId;
    }

    // 3. Use the index and runId in your mutation
    const mutation = `
      mutation {
        change_multiple_column_values(
          board_id: ${boardId},
          item_id: ${itemId},
          column_values: ${JSON.stringify(JSON.stringify(columnValues))}
        ) {
          id
        }
      }
    `;

    const response = await axios.post('https://api.monday.com/v2', { query: mutation }, {
      headers: {
        'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
        'Content-Type': 'application/json'
      }
    });
    log('info', 'STATUS_AND_NOTES_UPDATED', { requestId, itemId, boardId, runId, response: response.data });
  } catch (error) {
    log('error', 'STATUS_UPDATE_FAILED', { requestId, itemId, boardId, runId, error: error.message });
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
                                     { workspace:'nilesh.narendran_yahoo.com' },
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

    // Start processing run with enhanced error logging
    log('info', 'STARTING_PROCESSING', { requestId, batchId });
    
    // 🔧 NEW: Log the exact URL and payload we're sending
    const runUrl = `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/deployments/${INSTABASE_CONFIG.deploymentId}/runs`;
    const runPayload = { batch_id: batchId };
    
    log('info', 'PROCESSING_REQUEST_DETAILS', {
      requestId,
      url: runUrl,
      payload: runPayload,
      headers: INSTABASE_CONFIG.headers,
      deploymentId: INSTABASE_CONFIG.deploymentId
    });
    
    let runResponse;
    try {
      runResponse = await axios.post(runUrl, runPayload, { headers: INSTABASE_CONFIG.headers });
    } catch (runError) {
      // 🔧 NEW: Enhanced error logging to see exactly what Instabase returns
      log('error', 'PROCESSING_RUN_ERROR_DETAILS', {
        requestId,
        status: runError.response?.status,
        statusText: runError.response?.statusText,
        responseData: runError.response?.data,
        responseHeaders: runError.response?.headers,
        requestUrl: runUrl,
        requestPayload: runPayload,
        requestHeaders: INSTABASE_CONFIG.headers
      });
      
      throw new Error(`Instabase processing run failed: ${runError.response?.status} - ${JSON.stringify(runError.response?.data)}`);
    }
    
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
// 6️⃣  FIXED GROUPING - Recognizes Multiple Invoices in Same PDF
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
  
  // 🔧 FIXED: Track actual PDF page numbers, not document indices
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
      
      // 🔧 CRITICAL: Get the actual page number from Instabase
      const actualPageNumber = doc.page_num || docIndex; // Use page_num if available, fallback to docIndex
      
      // Extract invoice number from each page
      let invoiceNumber = fields['0']?.value;
      
      // Clean up invoice number
      if (invoiceNumber && 
          invoiceNumber !== 'unknown' && 
          invoiceNumber !== 'none' && 
          invoiceNumber !== '') {
        
        const cleanInvoiceNumber = String(invoiceNumber).trim();
        
        if (!documentGroups[cleanInvoiceNumber]) {
          documentGroups[cleanInvoiceNumber] = {
            invoice_number: cleanInvoiceNumber,
            pages: [],
            pageIndices: [], // Track actual PDF page numbers
            fileName: file.original_file_name,
            masterPage: null
          };
        }
        
        const pageData = {
          fileIndex,
          docIndex,
          actualPageNumber, // 🔧 NEW: Store the actual page number
          fields,
          fileName: file.original_file_name,
          invoiceNumber: cleanInvoiceNumber
        };
        
        documentGroups[cleanInvoiceNumber].pages.push(pageData);
        documentGroups[cleanInvoiceNumber].pageIndices.push(actualPageNumber); // 🔧 FIXED: Use actual page number
        
        // Set first page with this invoice number as master
        if (!documentGroups[cleanInvoiceNumber].masterPage) {
          documentGroups[cleanInvoiceNumber].masterPage = pageData;
        }
        
        log('info', 'PAGE_GROUPED_BY_INVOICE', {
          requestId,
          invoiceNumber: cleanInvoiceNumber,
          docIndex: docIndex,
          actualPageNumber: actualPageNumber, // 🔧 NEW: Log both for debugging
          fileName: file.original_file_name
        });
      } else {
        log('warn', 'NO_INVOICE_NUMBER_ON_PAGE', {
          requestId,
          fileIndex,
          docIndex,
          actualPageNumber: doc.page_num || docIndex,
          availableFields: Object.keys(fields)
        });
      }
    });
  });
  
  log('info', 'INVOICES_GROUPED', {
    requestId,
    invoiceNumbers: Object.keys(documentGroups),
    groupsByInvoice: Object.keys(documentGroups).map(invoiceNum => ({
      invoiceNumber: invoiceNum,
      pageCount: documentGroups[invoiceNum].pages.length,
      pageIndices: documentGroups[invoiceNum].pageIndices // 🔧 NEW: Show actual page numbers
    }))
  });
  
  // Create reconstructed documents with correct page indices
  const resultGroups = [];
  
  Object.keys(documentGroups).forEach(invoiceNumber => {
    const group = documentGroups[invoiceNumber];
    
    if (group.masterPage && group.pages.length > 0) {
      log('info', 'RECONSTRUCTING_INVOICE', {
        requestId,
        invoiceNumber,
        pageCount: group.pages.length,
        pageIndices: group.pageIndices
      });
      
      const reconstructedDocument = reconstructSingleInvoiceDocument(
        group.pages, 
        group.masterPage, 
        requestId, 
        group.fileName,
        invoiceNumber
      );
      
      if (reconstructedDocument) {
        // 🔧 FIXED: Add correct page indices to the document
        reconstructedDocument.pageIndices = group.pageIndices;
        reconstructedDocument.debugInfo = {
          originalFileName: group.fileName,
          pageMapping: group.pages.map(p => ({
            invoiceNumber: p.invoiceNumber,
            docIndex: p.docIndex,
            actualPageNumber: p.actualPageNumber
          }))
        };
        resultGroups.push(reconstructedDocument);
      }
    }
  });
  
  return resultGroups;
}

// 🔧 UPDATED: Reconstruct single invoice (not multi-page merger)
function reconstructSingleInvoiceDocument(pages, masterPage, requestId, filename, invoiceNumber) {
  log('info', 'RECONSTRUCTING_SINGLE_INVOICE', {
    requestId,
    invoiceNumber,
    pageCount: pages.length,
    filename
  });
  
  const masterFields = masterPage.fields;
  
  // Extract basic info from master page
  const documentType = masterFields['1']?.value || 'invoice';
  const supplier = masterFields['2']?.value || '';
  const documentDate = masterFields['3']?.value || '';
  const referenceNumber = masterFields['8']?.value || '';
  
  // Extract due dates from field 4
  let mergedDueDates = { due_date: '', due_date_2: '', due_date_3: '' };
  
  pages.forEach((page, pageIndex) => {
    const raw4 = page.fields['4'];
    if (raw4) {
      const dueDates = extractDueDatesFromField(raw4, requestId, pageIndex);
      
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
  
  // Extract line items from field 5 - ONLY from pages with THIS invoice number
  const allLineItems = [];
  
  pages.forEach((page, pageIndex) => {
    // Only process pages that belong to this specific invoice
    if (page.invoiceNumber === invoiceNumber) {
      const pageItems = extractLineItemsFromPage(page, requestId, pageIndex);
      if (pageItems.length > 0) {
        allLineItems.push(...pageItems);
        log('info', 'ITEMS_FOUND_FOR_INVOICE', {
          requestId,
          invoiceNumber,
          pageIndex,
          itemCount: pageItems.length
        });
      }
    }
  });
  
  // Find totals from field 6 and tax from field 7 - ONLY from this invoice's pages
  let totalAmount = 0;
  let taxAmount = 0;
  
  for (const page of pages) {
    if (page.invoiceNumber === invoiceNumber) {
      const pageTotalAmount = page.fields['6']?.value;
      const pageTaxAmount = page.fields['7']?.value;
      
      if (pageTotalAmount && !totalAmount) {
        const totalStr = String(pageTotalAmount).replace(/[^0-9.-]/g, '');
        totalAmount = parseFloat(totalStr) || 0;
      }
      
      if (pageTaxAmount && !taxAmount) {
        const taxStr = String(pageTaxAmount).replace(/[^0-9.-]/g, '');
        taxAmount = parseFloat(taxStr) || 0;
      }
    }
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
    terms: '',
    items: allLineItems,
    pages: pages.filter(p => p.invoiceNumber === invoiceNumber),
    confidence: 0,
    isMultiPageReconstruction: false,
    originalPageCount: pages.filter(p => p.invoiceNumber === invoiceNumber).length,
    reconstructionStrategy: 'invoice_number_based_grouping'
  };
  
  log('info', 'SINGLE_INVOICE_RECONSTRUCTION_COMPLETE', {
    requestId,
    invoiceNumber,
    totalAmount,
    taxAmount,
    lineItemCount: allLineItems.length,
    pageCount: reconstructedDocument.originalPageCount
  });
  
  return reconstructedDocument;
}

// 🔧 UPDATED: Helper function to extract due dates from field 4 (was field 6) - FIXED FOR NESTED ARRAYS
function extractDueDatesFromField(raw4, requestId, pageIndex) {
  let dueDates = { due_date: '', due_date_2: '', due_date_3: '' };
  
  if (!raw4) return dueDates;
  
  const v = raw4?.value;
  
  log('info', 'DUE_DATE_RAW_DATA', {
    requestId,
    pageIndex,
    rawValue: v,
    valueType: typeof v,
    isArray: Array.isArray(v),
    stringified: JSON.stringify(v)
  });
  
  if (typeof v === 'string') {
    try {
      // Try to parse as JSON first
      const parsed = JSON.parse(v.trim());
      if (Array.isArray(parsed)) {
        // Handle nested arrays like [["Due Date"], ["2025-06-16"]]
        if (parsed.length > 0 && Array.isArray(parsed[0])) {
          // Find the actual date values, skip header rows
          const dateValues = parsed.filter(row => 
            Array.isArray(row) && row.length > 0 && 
            !String(row[0]).toLowerCase().includes('due date') &&
            String(row[0]).match(/\d{4}-\d{2}-\d{2}|\d{1,2}\/\d{1,2}\/\d{4}|\d{2}-\d{2}-\d{4}/)
          );
          
          log('info', 'FILTERED_DATE_VALUES', {
            requestId,
            pageIndex,
            originalParsed: parsed,
            dateValues: dateValues
          });
          
          if (dateValues.length > 0) {
            dueDates.due_date = dateValues[0][0] || '';
            dueDates.due_date_2 = dateValues[1] ? dateValues[1][0] : '';
            dueDates.due_date_3 = dateValues[2] ? dateValues[2][0] : '';
          } else {
            // Fallback: look for any date-like value in the array
            for (const row of parsed) {
              if (Array.isArray(row)) {
                for (const cell of row) {
                  const cellStr = String(cell || '');
                  if (cellStr.match(/\d{4}-\d{2}-\d{2}|\d{1,2}\/\d{1,2}\/\d{4}|\d{2}-\d{2}-\d{4}/)) {
                    dueDates.due_date = cellStr;
                    break;
                  }
                }
                if (dueDates.due_date) break;
              }
            }
          }
        } else {
          // Regular array format
          dueDates.due_date = parsed[1] || '';
          dueDates.due_date_2 = parsed[2] || '';
          dueDates.due_date_3 = parsed[3] || '';
        }
      } else {
        dueDates.due_date = String(parsed);
      }
    } catch (e) {
      // If JSON parsing fails, treat as plain string
      if (v.match(/\d{4}-\d{2}-\d{2}|\d{1,2}\/\d{1,2}\/\d{4}|\d{2}-\d{2}-\d{4}/)) {
        dueDates.due_date = v.trim();
      }
    }
  } else if (Array.isArray(v)) {
    // Handle nested arrays like [["Due Date"], ["2025-06-16"]]
    if (v.length > 0 && Array.isArray(v[0])) {
      const dateValues = v.filter(row => 
        Array.isArray(row) && row.length > 0 && 
        !String(row[0]).toLowerCase().includes('due date') &&
        String(row[0]).match(/\d{4}-\d{2}-\d{2}|\d{1,2}\/\d{1,2}\/\d{4}|\d{2}-\d{2}-\d{4}/)
      );
      
      if (dateValues.length > 0) {
        dueDates.due_date = dateValues[0][0] || '';
        dueDates.due_date_2 = dateValues[1] ? dateValues[1][0] : '';
        dueDates.due_date_3 = dateValues[2] ? dateValues[2][0] : '';
      }
    } else {
      // Regular array format - look for actual dates, not headers
      const actualDates = v.filter(item => {
        const str = String(item || '').toLowerCase();
        return !str.includes('due date') && 
               !str.includes('date') && 
               str.match(/\d{4}-\d{2}-\d{2}|\d{1,2}\/\d{1,2}\/\d{4}|\d{2}-\d{2}-\d{4}/);
      });
      
      if (actualDates.length > 0) {
        dueDates.due_date = actualDates[0] || '';
        dueDates.due_date_2 = actualDates[1] || '';
        dueDates.due_date_3 = actualDates[2] || '';
      } else {
        // Fallback to index-based if no dates found
        dueDates.due_date = v[1] || '';
        dueDates.due_date_2 = v[2] || '';
        dueDates.due_date_3 = v[3] || '';
      }
    }
  } else {
    dueDates.due_date = String(v);
  }
  
  log('info', 'DUE_DATES_EXTRACTED', {
    requestId,
    pageIndex,
    extractedDueDates: dueDates
  });
  
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
    const subitemsColumn = columns.find(col => col.type === 'subtasks');
    
    for (const doc of documents) {
      log('info', 'CREATING_MONDAY_ITEM', {
        requestId,
        documentType: doc.document_type,
        invoiceNumber: doc.invoice_number,
        itemCount: doc.items?.length || 0,
        pageIndices: doc.pageIndices
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
      
      // 🔧 FIXED: Upload invoice-specific PDF file
      if (originalFiles && originalFiles.length > 0 && doc.pageIndices && doc.pageIndices.length > 0) {
        const fileColumn = columns.find(col => col.type === 'file');
        if (fileColumn) {
          try {
            // Create a PDF with only the pages for this specific invoice
            const invoiceSpecificPdf = await createInvoiceSpecificPDF(
              originalFiles[0].buffer, 
              doc.invoice_number, 
              doc.pageIndices, 
              requestId
            );
            
            // Generate a specific filename for this invoice
            const invoiceFileName = `Invoice_${doc.invoice_number}.pdf`;
            
            await uploadPdfToMondayItem(
              createdItemId, 
              invoiceSpecificPdf, 
              invoiceFileName, 
              fileColumn.id, 
              requestId
            );
            
            log('info', 'INVOICE_SPECIFIC_PDF_UPLOADED', {
              requestId,
              itemId: createdItemId,
              invoiceNumber: doc.invoice_number,
              fileName: invoiceFileName,
              pageIndices: doc.pageIndices
            });
            
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
