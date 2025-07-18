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
    
    // 🔧 ENHANCED: Better validation and logging
    const validPageIndices = [];
    const invalidPageIndices = [];
    pageIndices.forEach(pageIndex => {
      if (pageIndex >= 0 && pageIndex < totalPages) {
        validPageIndices.push(pageIndex);
      } else {
        invalidPageIndices.push(pageIndex);
      }
    });
    if (invalidPageIndices.length > 0) {
      log('warn', 'INVALID_PAGE_INDICES_FOUND', {
        requestId,
        invoiceNumber,
        invalidIndices: invalidPageIndices,
        totalPages
      });
    }
    if (validPageIndices.length === 0) {
      throw new Error(`No valid page indices found for invoice ${invoiceNumber}. Requested: ${pageIndices}, Total pages: ${totalPages}`);
    }
    
    // 🔧 ENHANCED: Copy pages with detailed logging
    for (let i = 0; i < validPageIndices.length; i++) {
      const pageIndex = validPageIndices[i];
      log('info', 'COPYING_PAGE_TO_INVOICE_PDF', {
        requestId,
        invoiceNumber,
        sourcePageIndex: pageIndex,
        destinationPageIndex: i,
        totalValidPages: validPageIndices.length
      });
      const [copiedPage] = await newPdf.copyPages(originalPdf, [pageIndex]);
      newPdf.addPage(copiedPage);
    }
    
    // Generate the PDF buffer
    const pdfBytes = await newPdf.save();
    
    log('info', 'INVOICE_PDF_CREATED_SUCCESS', {
      requestId,
      invoiceNumber,
      requestedPageCount: pageIndices.length,
      validPageCount: validPageIndices.length,
      invalidPageCount: invalidPageIndices.length,
      outputSize: pdfBytes.length,
      pageMapping: validPageIndices.map((sourceIndex, destIndex) => ({
        sourcePageIndex: sourceIndex,
        destinationPageIndex: destIndex
      }))
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
    
    // 🔧 SIMPLIFIED: No more validation here - just create all items
    // Monday's formula validation will handle retry logic after subitems are created
    if (groups.length > 0) {
      await createMondayExtractedItems(groups, itemId, originalFiles, requestId, runId);
    }

    log('info', 'PROCESSING_COMPLETE', { 
      requestId, 
      itemId, 
      extractedDocuments: groups.length
    });
    
    await updateMondayItemStatusToDone(ev.pulseId, ev.boardId, requestId, runId);
  } catch (err) {
    log('error', 'BACKGROUND_PROCESSING_FAILED', { 
      requestId, 
      error: err.message, 
      stack: err.stack 
    });
  }
}

// 🔧 FIXED: Status update with flexible column detection
async function updateMondayItemStatusToDone(itemId, boardId, requestId, runId = null) {
  const statusColumnId = "status";
  const doneLabel = "Completed";
  // 🔧 ENHANCED: Try multiple possible column names for processing notes
  const possibleNotesColumnNames = [
    "processing notes",
    "notes", 
    "processing note",
    "run id",
    "instabase run id",
    "extraction notes",
    "batch id"
  ];
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
    // 🔧 ENHANCED: Log all available columns for debugging
    log('info', 'AVAILABLE_BOARD_COLUMNS', {
      requestId,
      boardId,
      columns: columns.map(col => ({ id: col.id, title: col.title, type: col.type }))
    });
    const statusColumn = columns.find(col => col.id === statusColumnId && col.type === 'status');
    if (!statusColumn) {
      log('error', 'STATUS_COLUMN_NOT_FOUND', { requestId, itemId, boardId });
      return;
    }
    // Find processing notes column with flexible matching
    let processingNotesColumn = null;
    if (runId) {
      processingNotesColumn = columns.find(col => {
        const title = col.title.toLowerCase().trim();
        return possibleNotesColumnNames.some(name => title.includes(name)) && 
               (col.type === 'text' || col.type === 'long_text');
      });
      if (processingNotesColumn) {
        log('info', 'PROCESSING_NOTES_COLUMN_FOUND', {
          requestId,
          columnId: processingNotesColumn.id,
          columnTitle: processingNotesColumn.title,
          columnType: processingNotesColumn.type
        });
      } else {
        log('warn', 'PROCESSING_NOTES_COLUMN_NOT_FOUND', {
          requestId,
          searchedNames: possibleNotesColumnNames,
          availableTextColumns: columns.filter(col => col.type === 'text' || col.type === 'long_text')
            .map(col => col.title)
        });
      }
    }
    // Parse status column settings
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
      log('error', 'DONE_LABEL_NOT_FOUND', { 
        requestId, 
        itemId, 
        boardId, 
        searchedFor: doneLabel,
        availableLabels: JSON.parse(statusColumn.settings_str).labels 
      });
      return;
    }
    // Build column values
    let columnValues = { 
      [statusColumnId]: { "index": doneIndex }
    };
    if (runId && processingNotesColumn) {
      columnValues[processingNotesColumn.id] = runId;
    }
    log('info', 'UPDATING_MONDAY_COLUMNS', {
      requestId,
      itemId,
      boardId,
      columnValues,
      runId
    });
    // Update columns
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
    if (response.data.errors) {
      log('error', 'STATUS_UPDATE_MUTATION_ERROR', { 
        requestId, 
        itemId, 
        boardId, 
        errors: response.data.errors 
      });
    } else {
      log('info', 'STATUS_AND_NOTES_UPDATED_SUCCESS', { 
        requestId, 
        itemId, 
        boardId, 
        runId,
        updatedColumns: Object.keys(columnValues)
      });
    }
  } catch (error) {
    log('error', 'STATUS_UPDATE_FAILED', { 
      requestId, 
      itemId, 
      boardId, 
      runId, 
      error: error.message 
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
                                     { workspace:'nilesh.narendran_yahoo.com' },
                                     { headers: INSTABASE_CONFIG.headers });
    const batchId = batchRes.data.id;
    log('info', 'BATCH_CREATED', { requestId, batchId });

    const originalFiles = [];
    
    // Upload files to batch
    for (const f of files) {
      log('info', 'UPLOADING_FILE', { requestId, fileName: f.name });
      let buffer;
      let data;
      // 🔧 FIXED: Handle both URL files and direct buffer files
      if (f.public_url) {
        // Regular file from Monday.com
        const response = await axios.get(f.public_url, { responseType:'arraybuffer' });
        data = response.data;
        buffer = Buffer.from(data);
      } else if (f.buffer) {
        // Split file with direct buffer
        buffer = f.buffer;
        data = f.buffer;
      } else {
        log('error', 'NO_FILE_DATA_FOUND', { requestId, fileName: f.name });
        continue;
      }
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

// 🔧 NEW: Enhanced grouping function with proper page tracking
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
  
  // 🔧 FIXED: We'll use Instabase's page_numbers instead of tracking ourselves
  
  extractedFiles.forEach((file, fileIndex) => {
    log('info', 'PROCESSING_FILE', { 
      requestId, 
      fileIndex, 
      fileName: file.original_file_name,
      documentCount: file.documents?.length || 0
    });
    
    if (!file.documents || file.documents.length === 0) {
      log('warn', 'NO_DOCUMENTS_IN_FILE', { requestId, fileIndex });
      return;
    }
    
    file.documents.forEach((doc, docIndex) => {
      const fields = doc.fields || {};
      
      // 🔧 FIXED: Use Instabase's actual page_numbers field
      const instabasePageNumbers = doc.page_numbers || [];
      
      // Convert 1-based Instabase page numbers to 0-based PDF indices
      const pdfPageIndices = instabasePageNumbers.map(pageNum => pageNum - 1);
      
      log('info', 'PROCESSING_DOCUMENT', {
        requestId,
        fileIndex,
        docIndex,
        instabasePageNumbers: instabasePageNumbers,
        convertedPdfIndices: pdfPageIndices,
        fileName: file.original_file_name
      });
      
      // Extract invoice number
      let invoiceNumber = fields['0']?.value;
      if (invoiceNumber && 
          invoiceNumber !== 'unknown' && 
          invoiceNumber !== 'none' && 
          invoiceNumber !== '') {
        
        const cleanInvoiceNumber = String(invoiceNumber).trim();
        
        if (!documentGroups[cleanInvoiceNumber]) {
          documentGroups[cleanInvoiceNumber] = {
            invoice_number: cleanInvoiceNumber,
            pages: [],
            pageIndices: [],
            fileName: file.original_file_name,
            masterPage: null
          };
        }
        
        const pageData = {
          fileIndex,
          docIndex,
          pdfPageIndices: pdfPageIndices, // 🔧 Store all page indices for this document
          fields,
          fileName: file.original_file_name,
          invoiceNumber: cleanInvoiceNumber
        };
        
        documentGroups[cleanInvoiceNumber].pages.push(pageData);
        // 🔧 Add all page indices for this document
        documentGroups[cleanInvoiceNumber].pageIndices.push(...pdfPageIndices);
        
        if (!documentGroups[cleanInvoiceNumber].masterPage) {
          documentGroups[cleanInvoiceNumber].masterPage = pageData;
        }
        
        log('info', 'PAGE_GROUPED_BY_INVOICE', {
          requestId,
          invoiceNumber: cleanInvoiceNumber,
          docIndex: docIndex,
          instabasePageNumbers: instabasePageNumbers,
          pdfPageIndices: pdfPageIndices,
          fileName: file.original_file_name
        });
      } else {
        log('warn', 'NO_INVOICE_NUMBER_ON_PAGE', {
          requestId,
          fileIndex,
          docIndex,
          instabasePageNumbers: instabasePageNumbers,
          pdfPageIndices: pdfPageIndices,
          availableFields: Object.keys(fields)
        });
      }
      // 🔧 Remove the absolutePageIndex increment since we're using Instabase page numbers
      // absolutePageIndex++;
    });
  });
  
  // 🔧 ENHANCED: Detailed logging for debugging
  log('info', 'INVOICES_GROUPED_DETAILED', {
    requestId,
    invoiceNumbers: Object.keys(documentGroups),
    groupsByInvoice: Object.keys(documentGroups).map(invoiceNum => ({
      invoiceNumber: invoiceNum,
      pageCount: documentGroups[invoiceNum].pages.length,
      pageIndices: documentGroups[invoiceNum].pageIndices,
      fileName: documentGroups[invoiceNum].fileName
    }))
  });
  
  // Convert to result format
  const resultGroups = [];
  Object.keys(documentGroups).forEach(invoiceNumber => {
    const group = documentGroups[invoiceNumber];
    if (group.masterPage && group.pages.length > 0) {
      
      const reconstructedDocument = reconstructSingleInvoiceDocument(
        group.pages, 
        group.masterPage, 
        requestId, 
        group.fileName,
        invoiceNumber
      );
      
      if (reconstructedDocument) {
        // 🔧 CRITICAL: Sort and dedupe page indices
        reconstructedDocument.pageIndices = Array.from(new Set(group.pageIndices))
          .filter(i => i >= 0)
          .sort((a, b) => a - b);
        
        reconstructedDocument.fileName = group.fileName;
        reconstructedDocument.debugInfo = {
          originalFileName: group.fileName,
          pageMapping: group.pages.map(p => ({
            invoiceNumber: p.invoiceNumber,
            docIndex: p.docIndex,
            pdfPageIndices: p.pdfPageIndices
          }))
        };
        
        log('info', 'INVOICE_RECONSTRUCTION_COMPLETE', {
          requestId,
          invoiceNumber,
          pageIndices: reconstructedDocument.pageIndices,
          pageCount: reconstructedDocument.pageIndices.length,
          debugInfo: reconstructedDocument.debugInfo
        });
        
        resultGroups.push(reconstructedDocument);
      }
    }
  });
  
  return resultGroups;
}

// Update reconstructSingleInvoiceDocument to only use the filtered pages for line items
function reconstructSingleInvoiceDocument(pages, masterPage, requestId, filename, invoiceNumber) {
  log('info', 'RECONSTRUCTING_SINGLE_INVOICE', {
    requestId,
    invoiceNumber,
    pageCount: pages.length,
    filename
  });
  const masterFields = masterPage.fields;
  const documentType = masterFields['1']?.value || 'invoice';
  const supplier = masterFields['2']?.value || '';
  const documentDate = masterFields['3']?.value || '';
  const referenceNumber = masterFields['8']?.value || '';
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
  // Only extract line items from these pages
  const allLineItems = [];
  pages.forEach((page, pageIndex) => {
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
  });
  let totalAmount = 0;
  let taxAmount = 0;
  for (const page of pages) {
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
    items: allLineItems, // Only line items for this invoice
    pages: pages,
    confidence: 0,
    isMultiPageReconstruction: false,
    originalPageCount: pages.length,
    reconstructionStrategy: 'invoice_number_based_grouping_strict'
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

// 🔧 NEW: Extraction mathematical validation
function validateExtractionAccuracy(doc, requestId) {
  const tolerance = 0.05; // 5% tolerance for rounding differences
  let calculatedLineTotal = 0;
  if (doc.items && doc.items.length > 0) {
    calculatedLineTotal = doc.items.reduce((sum, item) => {
      return sum + (item.quantity * item.unit_cost);
    }, 0);
  }
  const expectedTotal = calculatedLineTotal + (doc.tax_amount || 0);
  const actualTotal = doc.total_amount || 0;
  const difference = Math.abs(expectedTotal - actualTotal);
  const percentageDiff = actualTotal > 0 ? (difference / actualTotal) : 1;
  const validationResult = {
    isValid: percentageDiff <= tolerance || actualTotal === 0, // 🔧 FIXED: Allow zero totals
    calculatedLineTotal: calculatedLineTotal,
    expectedTotal: expectedTotal,
    actualTotal: actualTotal,
    difference: difference,
    percentageDiff: percentageDiff,
    toleranceThreshold: tolerance,
    lineItemCount: doc.items?.length || 0,
    pageCount: doc.originalPageCount || 1,
    requiresRetry: percentageDiff > tolerance && doc.originalPageCount > 3 && actualTotal > 100 // 🔧 FIXED: Only retry if >3 pages AND significant total
  };
  log(validationResult.isValid ? 'info' : 'warn', 'EXTRACTION_VALIDATION', {
    requestId,
    invoiceNumber: doc.invoice_number,
    ...validationResult
  });
  return validationResult;
}

// 🔧 NEW: Extraction failure logging
async function logExtractionFailure(doc, validationResult, originalFiles, requestId, instabaseRunId) {
  const failureDetails = {
    timestamp: new Date().toISOString(),
    requestId: requestId,
    instabaseRunId: instabaseRunId,
    invoiceNumber: doc.invoice_number,
    documentType: doc.document_type,
    supplier: doc.supplier_name,
    actualTotal: doc.total_amount,
    calculatedTotal: validationResult.expectedTotal,
    taxAmount: doc.tax_amount,
    difference: validationResult.difference,
    percentageError: (validationResult.percentageDiff * 100).toFixed(2),
    pageCount: doc.originalPageCount,
    lineItemsExtracted: doc.items?.length || 0,
    fileName: doc.fileName,
    pageIndices: doc.pageIndices,
    failureReason: 'MATHEMATICAL_VALIDATION_FAILED',
    retryEligible: validationResult.requiresRetry
  };
  log('error', 'EXTRACTION_FAILURE_LOGGED', failureDetails);
  return failureDetails;
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
        pageIndices: doc.pageIndices,
        fileName: doc.fileName,
        isRetryAttempt: doc.retryAttempt || false  // 🔧 NEW: Track retry attempts
      });
      
      const formatDate = (dateStr) => {
        if (!dateStr || dateStr === 'Due Date' || dateStr === 'undefined' || dateStr === 'null') {
          return '';
        }
        try {
          let date;
          if (typeof dateStr === 'string') {
            const cleanDate = dateStr.trim();
            if (cleanDate.match(/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/)) {
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
      
      // 🔧 NEW: Add retry indicator to item name if this is a retry
      let itemName = instabaseRunId || 'RUN_PENDING';
      if (doc.retryAttempt) {
        itemName = `RETRY: ${doc.invoice_number} - ${instabaseRunId}`;
      }
      
      // Create the item
      const mutation = `
        mutation {
          create_item(
            board_id: ${MONDAY_CONFIG.extractedDocsBoardId}
            item_name: "${itemName}"
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
        invoiceNumber: doc.invoice_number,
        isRetryAttempt: doc.retryAttempt || false
      });
      
      // Upload PDF (existing code - unchanged)
      if (originalFiles && originalFiles.length > 0 && doc.pageIndices && doc.pageIndices.length > 0) {
        const fileColumn = columns.find(col => col.type === 'file');
        if (fileColumn) {
          try {
            const bufferForThisDoc = originalFiles.find(f => f.name === doc.fileName)?.buffer;
            if (!bufferForThisDoc) {
              log('error', 'PDF_BUFFER_NOT_FOUND_FOR_DOC', {
                requestId,
                fileName: doc.fileName,
                availableFiles: originalFiles.map(f => f.name)
              });
              continue;
            }
            
            let cleanPageIndices = Array.from(new Set(doc.pageIndices))
              .filter(i => typeof i === 'number' && i >= 0)
              .sort((a, b) => a - b);

            if (cleanPageIndices.length > 0) {
              const invoiceSpecificPdf = await createInvoiceSpecificPDF(
                bufferForThisDoc, 
                doc.invoice_number, 
                cleanPageIndices, 
                requestId
              );
              const invoiceFileName = `Invoice_${doc.invoice_number}${doc.retryAttempt ? '_RETRY' : ''}.pdf`;
              await uploadPdfToMondayItem(
                createdItemId, 
                invoiceSpecificPdf, 
                invoiceFileName, 
                fileColumn.id, 
                requestId
              );
            }
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
          
          // 🔧 NEW: After subitems are created, check Monday's validation (only for non-retry items)
          if (!doc.retryAttempt) {
            // Schedule validation check after a longer delay to allow formula to calculate
            setTimeout(async () => {
              await checkMondayValidationAndRetry(createdItemId, doc, originalFiles, requestId, instabaseRunId);
            }, 30000); // Increased to 30 seconds
          }
          
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

async function createSubitemsForLineItems(parentItemId, items, columns, requestId) {
  try {
    log('info', 'SUBITEM_CREATION_START', { requestId, parentItemId, lineItemCount: items.length });
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
    // Fetch subitem board columns
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
    // Process subitems in batches
    let successCount = 0;
    let failureCount = 0;
    const batchSize = 5;
    for (let i = 0; i < items.length; i += batchSize) {
      const batch = items.slice(i, i + batchSize);
      log('info', 'PROCESSING_SUBITEM_BATCH', {
        requestId,
        batchStart: i,
        batchSize: batch.length,
        totalItems: items.length
      });
      for (const item of batch) {
        try {
          const { line_number, item_number, quantity, unit_cost, description, source_page, po_number } = item;
          const columnValues = {};
          const actualLineNumber = line_number || `${items.indexOf(item) + 1}`;
          // Map item data to subitem columns
          subitemColumns.forEach(col => {
            const title = col.title.trim().toLowerCase();
            if (title.includes('line number') || title.includes('line #') || title.includes('subitem') || title === 'index') {
              if (col.type === 'numbers') {
                const numericLineNumber = parseFloat(actualLineNumber);
                columnValues[col.id] = isNaN(numericLineNumber) ? (items.indexOf(item) + 1) : numericLineNumber;
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
          const subitemName = `Line ${actualLineNumber}`;
          const escapedName = subitemName.replace(/"/g, '\"');
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
            },
            timeout: 30000
          });
          if (response.data.errors) {
            log('error', 'SUBITEM_CREATION_ERROR', { 
              requestId, 
              lineNumber: actualLineNumber, 
              errors: response.data.errors 
            });
            // Retry with simple creation on error
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
            try {
              const retryResponse = await axios.post('https://api.monday.com/v2', { query: simpleQuery }, {
                headers: {
                  Authorization: `Bearer ${MONDAY_CONFIG.apiKey}`,
                  'Content-Type': 'application/json',
                  'API-Version': '2024-04'
                },
                timeout: 30000
              });
              if (retryResponse.data.errors) {
                failureCount++;
                log('error', 'SUBITEM_RETRY_FAILED', {
                  requestId,
                  lineNumber: actualLineNumber,
                  errors: retryResponse.data.errors
                });
              } else {
                successCount++;
                log('info', 'SUBITEM_RETRY_SUCCESS', {
                  requestId,
                  lineNumber: actualLineNumber,
                  subitemId: retryResponse.data.data.create_subitem.id
                });
              }
            } catch (retryError) {
              failureCount++;
              log('error', 'SUBITEM_RETRY_EXCEPTION', {
                requestId,
                lineNumber: actualLineNumber,
                error: retryError.message
              });
            }
          } else {
            successCount++;
            log('info', 'SUBITEM_CREATED_SUCCESS', { 
              requestId, 
              lineNumber: actualLineNumber, 
              subitemId: response.data.data.create_subitem.id,
              poNumber: po_number
            });
          }
        } catch (itemError) {
          failureCount++;
          log('error', 'SUBITEM_CREATION_EXCEPTION', {
            requestId,
            itemIndex: items.indexOf(item),
            error: itemError.message
          });
        }
        // Delay between requests to avoid rate limits
        await new Promise(resolve => setTimeout(resolve, 500));
      }
      // Delay between batches
      if (i + batchSize < items.length) {
        log('info', 'BATCH_COMPLETE_WAITING', { requestId, completedItems: i + batchSize, totalItems: items.length });
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }
    log('info', 'ALL_SUBITEMS_PROCESSED', { 
      requestId, 
      parentItemId, 
      totalLineItems: items.length,
      successCount,
      failureCount
    });
  } catch (error) {
    log('error', 'SUBITEM_CREATION_FAILED', {
      requestId,
      parentItemId,
      error: error.message,
      stack: error.stack
    });
  }
}

// 🔧 NEW: Retry handler for failed extractions
async function handleExtractionRetries(failedGroups, sourceItemId, originalFiles, requestId) {
  log('info', 'STARTING_EXTRACTION_RETRIES', {
    requestId,
    failedGroupCount: failedGroups.length
  });
  for (const { doc, failureDetails, validation } of failedGroups) {
    try {
      log('info', 'PROCESSING_RETRY_FOR_INVOICE', {
        requestId,
        invoiceNumber: doc.invoice_number,
        originalPageCount: doc.originalPageCount,
        failureReason: failureDetails.failureReason
      });
      // STEP 1: Create individual page PDFs for retry
      const retryFiles = await createSplitPagePDFs(doc, originalFiles, requestId);
      if (retryFiles.length === 0) {
        log('error', 'NO_RETRY_FILES_CREATED', {
          requestId,
          invoiceNumber: doc.invoice_number
        });
        continue;
      }
      // STEP 2: Process split pages with Instabase
      const { files: retryExtracted, runId: retryRunId } = await processFilesWithInstabase(retryFiles, requestId);
      // STEP 3: Extract line items from retry results
      const retryLineItems = await extractLineItemsFromRetryResults(retryExtracted, requestId);
      // STEP 4: Validate retry results
      const retryDoc = {
        ...doc,
        items: retryLineItems,
        retryAttempt: true,
        originalRunId: failureDetails.instabaseRunId,
        retryRunId: retryRunId
      };
      const retryValidation = validateExtractionAccuracy(retryDoc, requestId);
      if (retryValidation.isValid) {
        log('info', 'RETRY_SUCCESSFUL', {
          requestId,
          invoiceNumber: doc.invoice_number,
          originalLineItems: doc.items?.length || 0,
          retryLineItems: retryLineItems.length,
          originalTotal: validation.calculatedLineTotal,
          retryTotal: retryValidation.calculatedLineTotal
        });
        // STEP 5: Create/Update Monday item with correct data
        await createMondayExtractedItems([retryDoc], sourceItemId, originalFiles, requestId, retryRunId);
      } else {
        log('error', 'RETRY_ALSO_FAILED', {
          requestId,
          invoiceNumber: doc.invoice_number,
          retryValidation
        });
        // Create item with error status for manual review
        await createFailedExtractionItem(doc, failureDetails, retryValidation, sourceItemId, originalFiles, requestId);
      }
    } catch (retryError) {
      log('error', 'RETRY_PROCESSING_ERROR', {
        requestId,
        invoiceNumber: doc.invoice_number,
        error: retryError.message
      });
      // Create item with error status
      await createFailedExtractionItem(doc, failureDetails, validation, sourceItemId, originalFiles, requestId);
    }
  }
}

// 🔧 NEW: Create individual page PDFs for retry
async function createSplitPagePDFs(doc, originalFiles, requestId) {
  try {
    log('info', 'CREATING_SPLIT_PAGE_PDFS', {
      requestId,
      invoiceNumber: doc.invoice_number,
      pageIndices: doc.pageIndices,
      fileName: doc.fileName
    });
    const sourceFile = originalFiles.find(f => f.name === doc.fileName);
    if (!sourceFile) {
      throw new Error(`Source file not found: ${doc.fileName}`);
    }
    const originalPdf = await PDFDocument.load(sourceFile.buffer);
    const splitFiles = [];
    for (let i = 0; i < doc.pageIndices.length; i++) {
      const pageIndex = doc.pageIndices[i];
      const newPdf = await PDFDocument.create();
      const [copiedPage] = await newPdf.copyPages(originalPdf, [pageIndex]);
      newPdf.addPage(copiedPage);
      const pdfBytes = await newPdf.save();
      const fileName = `${doc.invoice_number}_page_${pageIndex + 1}_retry.pdf`;
      splitFiles.push({
        name: fileName,
        buffer: Buffer.from(pdfBytes),
        public_url: null,
        pageIndex: pageIndex,
        invoiceNumber: doc.invoice_number
      });
      log('info', 'SPLIT_PAGE_CREATED', {
        requestId,
        invoiceNumber: doc.invoice_number,
        pageIndex: pageIndex,
        fileName: fileName,
        size: pdfBytes.length
      });
    }
    return splitFiles;
  } catch (error) {
    log('error', 'SPLIT_PDF_CREATION_FAILED', {
      requestId,
      invoiceNumber: doc.invoice_number,
      error: error.message
    });
    return [];
  }
}

// 🔧 NEW: Extract line items from retry processing results
async function extractLineItemsFromRetryResults(retryExtracted, requestId) {
  const allRetryLineItems = [];
  if (!retryExtracted || retryExtracted.length === 0) {
    log('warn', 'NO_RETRY_RESULTS_TO_PROCESS', { requestId });
    return allRetryLineItems;
  }
  retryExtracted.forEach((file, fileIndex) => {
    log('info', 'PROCESSING_RETRY_FILE', {
      requestId,
      fileIndex,
      fileName: file.original_file_name,
      documentCount: file.documents?.length || 0
    });
    if (!file.documents || file.documents.length === 0) {
      return;
    }
    file.documents.forEach((doc, docIndex) => {
      const pageItems = extractLineItemsFromPage({
        fields: doc.fields || {},
        fileName: file.original_file_name,
        invoiceNumber: file.original_file_name.split('_')[0]
      }, requestId, docIndex);
      if (pageItems.length > 0) {
        allRetryLineItems.push(...pageItems);
        log('info', 'RETRY_LINE_ITEMS_FOUND', {
          requestId,
          fileName: file.original_file_name,
          itemCount: pageItems.length
        });
      }
    });
  });
  allRetryLineItems.sort((a, b) => {
    const aNum = parseFloat(a.line_number) || 0;
    const bNum = parseFloat(b.line_number) || 0;
    return aNum - bNum;
  });
  log('info', 'RETRY_LINE_ITEMS_EXTRACTED', {
    requestId,
    totalItems: allRetryLineItems.length
  });
  return allRetryLineItems;
}

// 🔧 NEW: Create Monday item for failed extractions requiring manual review
async function createFailedExtractionItem(doc, failureDetails, validation, sourceItemId, originalFiles, requestId) {
  try {
    log('info', 'CREATING_FAILED_EXTRACTION_ITEM', {
      requestId,
      invoiceNumber: doc.invoice_number
    });
    const boardQuery = `
      query {
        boards(ids: [${MONDAY_CONFIG.extractedDocsBoardId}]) {
          columns { id title type settings_str }
        }
      }
    `;
    const boardResponse = await axios.post('https://api.monday.com/v2', { query: boardQuery }, {
      headers: { 'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}` }
    });
    const columns = boardResponse.data.data?.boards?.[0]?.columns || [];
    const columnValues = buildColumnValues(columns, doc, (dateStr) => {
      if (!dateStr) return '';
      try {
        return new Date(dateStr).toISOString().split('T')[0];
      } catch (e) {
        return '';
      }
    }, failureDetails.instabaseRunId);
    const statusColumn = columns.find(col => col.title.toLowerCase().includes('status'));
    if (statusColumn && statusColumn.type === 'status') {
      try {
        const settings = JSON.parse(statusColumn.settings_str);
        const errorLabel = Object.entries(settings.labels || {}).find(([_, label]) => 
          label && (label.toLowerCase().includes('error') || label.toLowerCase().includes('review'))
        );
        if (errorLabel) {
          columnValues[statusColumn.id] = { "index": parseInt(errorLabel[0]) };
        }
      } catch (e) {}
    }
    const notesColumn = columns.find(col => 
      col.title.toLowerCase().includes('notes') && 
      (col.type === 'text' || col.type === 'long_text')
    );
    if (notesColumn) {
      columnValues[notesColumn.id] = `EXTRACTION FAILED - Requires Manual Review\n` +
        `Original Run ID: ${failureDetails.instabaseRunId}\n` +
        `Error: ${failureDetails.percentageError}% difference between calculated and actual totals\n` +
        `Calculated: $${failureDetails.calculatedTotal.toFixed(2)}\n` +
        `Actual: $${failureDetails.actualTotal.toFixed(2)}\n` +
        `Pages: ${failureDetails.pageCount}\n` +
        `Line Items Found: ${failureDetails.lineItemsExtracted}`;
    }
    const mutation = `
      mutation {
        create_item(
          board_id: ${MONDAY_CONFIG.extractedDocsBoardId}
          item_name: "FAILED: ${doc.invoice_number || 'Unknown'}"
          column_values: ${JSON.stringify(JSON.stringify(columnValues))}
        ) {
          id
          name
        }
      }
    `;
    const response = await axios.post('https://api.monday.com/v2', { query: mutation }, {
      headers: { 'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}` }
    });
    if (response.data.errors) {
      log('error', 'FAILED_ITEM_CREATION_ERROR', {
        requestId,
        errors: response.data.errors
      });
    } else {
      const createdItemId = response.data.data.create_item.id;
      log('info', 'FAILED_EXTRACTION_ITEM_CREATED', {
        requestId,
        itemId: createdItemId,
        invoiceNumber: doc.invoice_number
      });
      const fileColumn = columns.find(col => col.type === 'file');
      if (fileColumn && originalFiles && originalFiles.length > 0) {
        const sourceFile = originalFiles.find(f => f.name === doc.fileName);
        if (sourceFile) {
          await uploadPdfToMondayItem(
            createdItemId,
            sourceFile.buffer,
            `FAILED_${doc.fileName}`,
            fileColumn.id,
            requestId
          );
        }
      }
    }
  } catch (error) {
    log('error', 'FAILED_EXTRACTION_ITEM_CREATION_ERROR', {
      requestId,
      invoiceNumber: doc.invoice_number,
      error: error.message
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

// 🔧 BEST APPROACH: Find item by Document Number (Invoice Number) and validate
async function checkMondayValidationAndRetry(createdItemId, doc, originalFiles, requestId, instabaseRunId) {
  try {
    log('info', 'CHECKING_MONDAY_VALIDATION_DIRECT', {
      requestId,
      createdItemId,
      invoiceNumber: doc.invoice_number,
      instabaseRunId: instabaseRunId
    });
    
    // Wait longer for subitems and formula calculation
    await new Promise(resolve => setTimeout(resolve, 25000)); // Increased to 25 seconds
    
    // 🔧 FIXED: Query the specific item we just created
    const itemQuery = `
      query {
        items(ids: [${createdItemId}]) {
          id
          name
          column_values {
            id
            title
            text
            value
          }
          subitems {
            id
            name
          }
        }
      }
    `;
    
    const itemResponse = await axios.post('https://api.monday.com/v2', {
      query: itemQuery
    }, {
      headers: {
        'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
        'Content-Type': 'application/json'
      }
    });
    
    if (itemResponse.data.errors) {
      log('error', 'ITEM_QUERY_ERROR', {
        requestId,
        errors: itemResponse.data.errors
      });
      return;
    }
    
    const item = itemResponse.data.data?.items?.[0];
    if (!item) {
      log('error', 'ITEM_NOT_FOUND_FOR_VALIDATION', {
        requestId,
        createdItemId,
        invoiceNumber: doc.invoice_number
      });
      return;
    }
    
    log('info', 'ITEM_FOUND_FOR_VALIDATION', {
      requestId,
      itemId: item.id,
      itemName: item.name,
      subitemCount: item.subitems?.length || 0,
      columnCount: item.column_values?.length || 0
    });
    
    // 🔧 FIXED: Better validation column detection
    const validationColumn = item.column_values.find(col => {
      const title = col.title.toLowerCase();
      return title.includes('match') || 
             title.includes('valid') || 
             title.includes('formula') ||
             title.includes('total') ||
             title.includes('check') ||
             title.includes('sum');
    });
    
    if (!validationColumn) {
      log('warn', 'VALIDATION_COLUMN_NOT_FOUND', {
        requestId,
        itemId: item.id,
        availableColumns: item.column_values.map(col => ({
          title: col.title,
          text: col.text,
          value: col.value
        }))
      });
      return;
    }
    
    const validationText = validationColumn.text?.toLowerCase() || '';
    const validationValue = validationColumn.value || '';
    
    log('info', 'VALIDATION_COLUMN_FOUND', {
      requestId,
      itemId: item.id,
      columnTitle: validationColumn.title,
      validationText: validationText,
      validationValue: validationValue
    });
    
    // 🔧 FIXED: Improved validation failure detection
    const validationFailed = validationText.includes('no') || 
                            validationText.includes('false') || 
                            validationText.includes('❌') || 
                            validationText.includes('fail') ||
                            validationText.includes('mismatch') ||
                            validationText.includes('error') ||
                            validationValue === 'false' ||
                            validationValue === '0' ||
                            (!validationText && !validationValue); // Handle empty/null values
    
    if (validationFailed) {
      log('warn', 'VALIDATION_FAILED_TRIGGERING_RETRY', {
        requestId,
        itemId: item.id,
        invoiceNumber: doc.invoice_number,
        validationText: validationText,
        validationValue: validationValue,
        subitemCount: item.subitems?.length || 0
      });
      
      // Create failure details
      const failureDetails = {
        timestamp: new Date().toISOString(),
        requestId: requestId,
        instabaseRunId: instabaseRunId,
        invoiceNumber: doc.invoice_number,
        documentType: doc.document_type,
        supplier: doc.supplier_name,
        mondayValidationResult: validationText,
        pageCount: doc.originalPageCount || 1,
        lineItemsExtracted: doc.items?.length || 0,
        subitemsCreated: item.subitems?.length || 0,
        fileName: doc.fileName,
        pageIndices: doc.pageIndices,
        failureReason: 'MONDAY_FORMULA_VALIDATION_FAILED',
        retryEligible: true,
        mondayItemId: item.id
      };
      
      // Update status to retrying
      await updateMondayItemForRetry(item.id, 'Retrying - Validation Failed', requestId);
      
      // Start retry process
      await handleSingleDocumentRetry(doc, failureDetails, originalFiles, requestId, item.id);
      
    } else {
      log('info', 'VALIDATION_PASSED', {
        requestId,
        itemId: item.id,
        invoiceNumber: doc.invoice_number,
        validationText: validationText,
        validationValue: validationValue,
        subitemCount: item.subitems?.length || 0
      });
    }
    
  } catch (error) {
    log('error', 'VALIDATION_CHECK_FAILED', {
      requestId,
      createdItemId,
      invoiceNumber: doc.invoice_number,
      error: error.message
    });
  }
}

// 🔧 NEW: Update Monday item status for retry
async function updateMondayItemForRetry(itemId, statusText, requestId) {
  try {
    const boardQuery = `
      query {
        boards(ids: [${MONDAY_CONFIG.extractedDocsBoardId}]) {
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
    const statusColumn = columns.find(col => col.type === 'status');
    const notesColumn = columns.find(col => 
      col.title.toLowerCase().includes('notes') && 
      (col.type === 'text' || col.type === 'long_text')
    );
    const columnValues = {};
    if (notesColumn) {
      const timestamp = new Date().toISOString();
      columnValues[notesColumn.id] = `${timestamp}: ${statusText} - Monday validation failed, initiating retry process.`;
    }
    if (statusColumn) {
      try {
        const settings = JSON.parse(statusColumn.settings_str || '{}');
        const retryLabel = Object.entries(settings.labels || {}).find(([_, label]) => 
          label && (
            label.toLowerCase().includes('retry') || 
            label.toLowerCase().includes('processing') ||
            label.toLowerCase().includes('pending')
          )
        );
        if (retryLabel) {
          columnValues[statusColumn.id] = { "index": parseInt(retryLabel[0]) };
        }
      } catch (e) {
        log('warn', 'STATUS_SETTINGS_PARSE_ERROR', { requestId, error: e.message });
      }
    }
    if (Object.keys(columnValues).length > 0) {
      const mutation = `
        mutation {
          change_multiple_column_values(
            board_id: ${MONDAY_CONFIG.extractedDocsBoardId},
            item_id: ${itemId},
            column_values: ${JSON.stringify(JSON.stringify(columnValues))}
          ) {
            id
          }
        }
      `;
      await axios.post('https://api.monday.com/v2', { query: mutation }, {
        headers: {
          'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
          'Content-Type': 'application/json'
        }
      });
      log('info', 'RETRY_STATUS_UPDATED', {
        requestId,
        itemId,
        updatedColumns: Object.keys(columnValues)
      });
    }
  } catch (error) {
    log('error', 'RETRY_STATUS_UPDATE_FAILED', {
      requestId,
      itemId,
      error: error.message
    });
  }
}

// 🔧 NEW: Handle retry for a single document
async function handleSingleDocumentRetry(doc, failureDetails, originalFiles, requestId, originalItemId) {
  try {
    log('info', 'STARTING_SINGLE_DOCUMENT_RETRY', {
      requestId,
      invoiceNumber: doc.invoice_number,
      originalItemId: originalItemId
    });
    const retryFiles = await createSplitPagePDFs(doc, originalFiles, requestId);
    if (retryFiles.length === 0) {
      log('error', 'NO_RETRY_FILES_CREATED_FOR_SINGLE_DOC', {
        requestId,
        invoiceNumber: doc.invoice_number
      });
      await updateMondayItemForRetry(originalItemId, 'Retry Failed - Could not split PDF', requestId);
      return;
    }
    const { files: retryExtracted, runId: retryRunId } = await processFilesWithInstabase(retryFiles, requestId);
    const retryLineItems = await extractLineItemsFromRetryResults(retryExtracted, requestId);
    const retryDoc = {
      ...doc,
      items: retryLineItems,
      retryAttempt: true,
      originalRunId: failureDetails.instabaseRunId,
      retryRunId: retryRunId,
      originalMondayItemId: originalItemId
    };
    log('info', 'RETRY_EXTRACTION_COMPLETE', {
      requestId,
      invoiceNumber: doc.invoice_number,
      originalLineItems: doc.items?.length || 0,
      retryLineItems: retryLineItems.length,
      retryRunId: retryRunId
    });
    await createMondayExtractedItems([retryDoc], originalItemId, originalFiles, requestId, retryRunId);
    await updateMondayItemForRetry(originalItemId, `Retry Completed - New extraction created with Run ID: ${retryRunId}`, requestId);
    log('info', 'SINGLE_DOCUMENT_RETRY_COMPLETE', {
      requestId,
      invoiceNumber: doc.invoice_number,
      originalItemId: originalItemId,
      retryRunId: retryRunId
    });
  } catch (retryError) {
    log('error', 'SINGLE_DOCUMENT_RETRY_FAILED', {
      requestId,
      invoiceNumber: doc.invoice_number,
      originalItemId: originalItemId,
      error: retryError.message
    });
    await updateMondayItemForRetry(originalItemId, `Retry Failed - ${retryError.message}`, requestId);
  }
}
