// 🔧 FIXED: Find the actual "Source Request ID" column instead of "name"
    const sourceRequestIdColumn = columns.find(col => 
      col.title.toLowerCase() === 'id'
    );
    
    log('info', 'SOURCE_REQUEST_ID_COLUMN_SEARCH', {
      requestId,
      foundColumn: !!sourceRequestIdColumn,
      columnId: sourceRequestIdColumn?.id,
      columnTitle: sourceRequestIdColumn?.title,
      allColumns: columns.map(c => ({ id: c.id, title: c.title, type: c.type }))
    });
    
    // Validate subitems column exists
    const subitemsColumn = columns.find(col => col.type === 'subtasks');
    log('info', 'BOARD_VALIDATION', {
      requestId,
      totalColumns: columns.length,
      hasSubitemsColumn: !!subitemsColumn,
      subitemsColumnId: subitemsColumn?.id,
      subitemsColumnTitle: subitemsColumn?.title,
      hasSourceRequestIdColumn: !!sourceRequestIdColumn
    });
    
    if (!subitemsColumn) {
      log('error', 'MISSING_SUBITEMS_COLUMN', {
        requestId,
        error: 'Board missing subitems column - cannot create subitems'
      });
    }
    
    if (!sourceRequestIdColumn) {
      log('warn', 'MISSING_SOURCE_REQUEST_ID_COLUMN', {
        requestId,
        warning: 'No Source Request ID column found - cannot link Instabase run ID'
      });
    }
    
    for (const doc of documents) {
      log('info', 'CREATING_MONDAY_ITEM', {
        requestId,
        documentType: doc.document_type,
        invoiceNumber: doc.invoice_number,
        itemCount: doc.items?.length || 0,
        isMultiPage: doc.isMultiPageReconstruction || false,
        originalPageCount: doc.originalPageCount || 1
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
      
      const columnValues = buildColumnValues(columns, doc, formatDate, instabaseRunId);
      
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
      
      // Create the item - Use Run ID as the primary name/ID
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
        invoiceNumber: doc.invoice_number,
        isMultiPageReconstruction: doc.isMultiPageReconstruction || false
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

// 🔧 FIXED: Subitem creation function with proper indexing
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
      log// Digital Mailroom Webhook — FINAL VERSION with Multi-Page Document Reconstruction
// -----------------------------------------------------------------------------
// This version intelligently merges split PDF pages back into complete documents
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

// 🔧 ENHANCED: Function to intelligently reconstruct multi-page documents
function reconstructMultiPageDocument(pages, masterPage, requestId, filename) {
  log('info', 'RECONSTRUCTING_MULTI_PAGE_DOC', {
    requestId,
    filename,
    pageCount: pages.length,
    masterPageIndex: pages.indexOf(masterPage),
    pageTypes: pages.map(p => p.fields['1']?.value || 'unknown')
  });
  
  const masterFields = masterPage.fields;
  
  // Extract basic info from master page
  const invoiceNumber = masterFields['0']?.value || filename.replace(/\.[^/.]+$/, "");
  const documentType = masterFields['2']?.value || 'invoice';
  const supplier = masterFields['3']?.value || '';
  const terms = masterFields['4']?.value || '';
  const documentDate = masterFields['5']?.value || '';
  const referenceNumber = masterFields['10']?.value || '';
  
  // 🔧 ENHANCED: Smart page classification and data extraction
  const classifiedPages = classifyAndExtractPages(pages, requestId, filename);
  
  // 🔧 ENHANCED: Merge data intelligently from classified pages
  const mergedData = mergeDataFromClassifiedPages(classifiedPages, requestId);
  
  const reconstructedDocument = {
    invoice_number: invoiceNumber,
    document_type: documentType,
    supplier_name: supplier || mergedData.supplier,
    reference_number: referenceNumber || mergedData.referenceNumber,
    total_amount: mergedData.totalAmount,
    tax_amount: mergedData.taxAmount,
    document_date: documentDate || mergedData.documentDate,
    due_date: mergedData.dueDates.due_date,
    due_date_2: mergedData.dueDates.due_date_2,
    due_date_3: mergedData.dueDates.due_date_3,
    terms: terms || mergedData.terms,
    items: mergedData.allLineItems,
    pages: pages.map((page, index) => ({
      page_type: page.fields['1']?.value || `page_${index + 1}`,
      file_name: filename,
      fields: page.fields,
      classification: classifiedPages.find(cp => cp.pageIndex === index)?.classification || 'unknown'
    })),
    confidence: 0,
    isMultiPageReconstruction: true,
    originalPageCount: pages.length,
    reconstructionStrategy: 'ai_enhanced_classification'
  };
  
  log('info', 'DOCUMENT_RECONSTRUCTION_COMPLETE', {
    requestId,
    filename,
    invoiceNumber,
    totalAmount: mergedData.totalAmount,
    taxAmount: mergedData.taxAmount,
    lineItemCount: mergedData.allLineItems.length,
    pageCount: pages.length,
    pageClassifications: classifiedPages.map(cp => cp.classification)
  });
  
  return reconstructedDocument;
}

// 🔧 NEW: Advanced page classification with fallback logic
function classifyAndExtractPages(pages, requestId, filename) {
  const classifiedPages = [];
  
  pages.forEach((page, pageIndex) => {
    const fields = page.fields;
    
    // Get Instabase classification from Page Type field
    const instabasePageType = fields['1']?.value?.toLowerCase() || '';
    
    // 🔧 ENHANCED: Multi-strategy page classification
    let classification = 'unknown';
    let confidence = 0;
    
    // Strategy 1: Use Instabase Page Type if available and reliable
    if (instabasePageType && instabasePageType !== 'unknown' && instabasePageType !== '') {
      classification = instabasePageType;
      confidence = 0.9; // High confidence in AI classification
      
      log('info', 'USING_INSTABASE_CLASSIFICATION', {
        requestId,
        filename,
        pageIndex,
        classification,
        confidence
      });
    } else {
      // Strategy 2: Fallback content-based classification
      const contentAnalysis = analyzePageContent(fields, pageIndex, requestId);
      classification = contentAnalysis.classification;
      confidence = contentAnalysis.confidence;
      
      log('info', 'USING_CONTENT_BASED_CLASSIFICATION', {
        requestId,
        filename,
        pageIndex,
        classification,
        confidence,
        reasoning: contentAnalysis.reasoning
      });
    }
    
    classifiedPages.push({
      pageIndex,
      page,
      classification,
      confidence,
      fields,
      hasInvoiceNumber: !!(fields['0']?.value && fields['0'].value !== 'unknown'),
      hasLineItems: hasLineItemsInPage(fields),
      hasTotals: !!(fields['8']?.value || fields['9']?.value),
      hasDueDates: !!fields['6']?.value
    });
  });
  
  // 🔧 ENHANCED: Post-process classifications for consistency
  return refinePageClassifications(classifiedPages, requestId, filename);
}

// 🔧 NEW: Content-based page analysis for fallback classification
function analyzePageContent(fields, pageIndex, requestId) {
  const hasInvoiceNumber = fields['0']?.value && fields['0'].value !== 'unknown';
  const hasLineItems = hasLineItemsInPage(fields);
  const hasTotals = !!(fields['8']?.value || fields['9']?.value);
  const hasSupplier = !!(fields['3']?.value);
  const hasDocumentDate = !!(fields['5']?.value);
  const hasDueDates = !!(fields['6']?.value);
  
  let classification = 'unknown';
  let confidence = 0.5;
  let reasoning = [];
  
  // Main page indicators
  if (hasInvoiceNumber && hasSupplier && hasDocumentDate) {
    classification = 'main';
    confidence = 0.8;
    reasoning.push('has invoice number, supplier, and document date');
  }
  // Continuation page with line items
  else if (hasLineItems && !hasInvoiceNumber) {
    classification = 'continuation';
    confidence = 0.7;
    reasoning.push('has line items but no invoice number');
  }
  // Summary/final page with totals
  else if (hasTotals) {
    classification = 'summary';
    confidence = 0.8;
    reasoning.push('contains total amounts');
  }
  // Terms page (usually has due dates but no line items or totals)
  else if (hasDueDates && !hasLineItems && !hasTotals) {
    classification = 'terms';
    confidence = 0.6;
    reasoning.push('has due dates but no line items or totals');
  }
  // Fallback based on position
  else {
    if (pageIndex === 0) {
      classification = 'main';
      confidence = 0.4;
      reasoning.push('first page fallback');
    } else {
      classification = 'continuation';
      confidence = 0.3;
      reasoning.push('middle page fallback');
    }
  }
  
  return {
    classification,
    confidence,
    reasoning: reasoning.join(', '),
    indicators: {
      hasInvoiceNumber,
      hasLineItems,
      hasTotals,
      hasSupplier,
      hasDocumentDate,
      hasDueDates
    }
  };
}

// 🔧 NEW: Check if page contains line items
function hasLineItemsInPage(fields) {
  // Check field 7 (primary line items field)
  const field7 = fields['7'];
  if (field7?.value) {
    if (typeof field7.value === 'string') {
      try {
        const parsed = JSON.parse(field7.value);
        return Array.isArray(parsed) && parsed.length > 0;
      } catch (e) {
        return false;
      }
    }
    return Array.isArray(field7.value) && field7.value.length > 0;
  }
  
  // Check any field for arrays that might contain line items
  for (const fieldKey of Object.keys(fields)) {
    const fieldValue = fields[fieldKey]?.value;
    if (Array.isArray(fieldValue) && fieldValue.length > 0) {
      const firstItem = fieldValue[0];
      if (typeof firstItem === 'object' || Array.isArray(firstItem)) {
        return true;
      }
    }
  }
  
  return false;
}

// 🔧 NEW: Refine classifications for consistency
function refinePageClassifications(classifiedPages, requestId, filename) {
  log('info', 'REFINING_PAGE_CLASSIFICATIONS', {
    requestId,
    filename,
    pageCount: classifiedPages.length,
    initialClassifications: classifiedPages.map(cp => cp.classification)
  });
  
  // Ensure we have exactly one main page
  const mainPages = classifiedPages.filter(cp => cp.classification === 'main');
  if (mainPages.length === 0) {
    // Promote the first page with invoice number to main
    const firstWithInvoice = classifiedPages.find(cp => cp.hasInvoiceNumber);
    if (firstWithInvoice) {
      firstWithInvoice.classification = 'main';
      log('info', 'PROMOTED_TO_MAIN', { requestId, pageIndex: firstWithInvoice.pageIndex });
    } else {
      // Fallback: make first page main
      classifiedPages[0].classification = 'main';
      log('info', 'FALLBACK_FIRST_PAGE_TO_MAIN', { requestId });
    }
  } else if (mainPages.length > 1) {
    // Keep only the first main page, demote others to continuation
    for (let i = 1; i < mainPages.length; i++) {
      mainPages[i].classification = 'continuation';
      log('info', 'DEMOTED_DUPLICATE_MAIN', { requestId, pageIndex: mainPages[i].pageIndex });
    }
  }
  
  // Identify summary page (last page with totals)
  const pagesWithTotals = classifiedPages.filter(cp => cp.hasTotals);
  if (pagesWithTotals.length > 0) {
    const lastPageWithTotals = pagesWithTotals[pagesWithTotals.length - 1];
    if (lastPageWithTotals.classification !== 'main') {
      lastPageWithTotals.classification = 'summary';
      log('info', 'IDENTIFIED_SUMMARY_PAGE', { requestId, pageIndex: lastPageWithTotals.pageIndex });
    }
  }
  
  log('info', 'CLASSIFICATION_REFINEMENT_COMPLETE', {
    requestId,
    filename,
    finalClassifications: classifiedPages.map(cp => cp.classification)
  });
  
  return classifiedPages;
}

// 🔧 NEW: Merge data from classified pages intelligently
function mergeDataFromClassifiedPages(classifiedPages, requestId) {
  log('info', 'MERGING_DATA_FROM_CLASSIFIED_PAGES', {
    requestId,
    pageCount: classifiedPages.length,
    classifications: classifiedPages.map(cp => cp.classification)
  });
  
  let totalAmount = 0;
  let taxAmount = 0;
  let supplier = '';
  let referenceNumber = '';
  let documentDate = '';
  let terms = '';
  const allLineItems = [];
  let mergedDueDates = { due_date: '', due_date_2: '', due_date_3: '' };
  
  // Process pages in priority order: main -> summary -> continuation -> terms
  const pageOrder = ['main', 'summary', 'continuation', 'terms'];
  
  pageOrder.forEach(pageType => {
    const pagesOfType = classifiedPages.filter(cp => cp.classification === pageType);
    
    pagesOfType.forEach(classifiedPage => {
      const { page, pageIndex, fields } = classifiedPage;
      
      log('info', 'PROCESSING_CLASSIFIED_PAGE', {
        requestId,
        pageIndex,
        classification: pageType,
        processing: 'data extraction'
      });
      
      // Extract supplier (prioritize main page)
      if (!supplier && fields['3']?.value) {
        supplier = fields['3'].value;
      }
      
      // Extract reference number (prioritize main page)
      if (!referenceNumber && fields['10']?.value) {
        referenceNumber = fields['10'].value;
      }
      
      // Extract document date (prioritize main page)
      if (!documentDate && fields['5']?.value) {
        documentDate = fields['5'].value;
      }
      
      // Extract terms (any page)
      if (!terms && fields['4']?.value) {
        terms = fields['4'].value;
      }
      
      // Extract totals (prioritize summary page, then main)
      if (fields['8']?.value && !totalAmount) {
        const totalStr = String(fields['8'].value).replace(/[^0-9.-]/g, '');
        const amount = parseFloat(totalStr) || 0;
        if (amount > totalAmount) {
          totalAmount = amount;
          log('info', 'TOTAL_UPDATED', {
            requestId,
            pageIndex,
            pageType,
            totalAmount,
            source: 'field_8'
          });
        }
      }
      
      if (fields['9']?.value && !taxAmount) {
        const taxStr = String(fields['9'].value).replace(/[^0-9.-]/g, '');
        const amount = parseFloat(taxStr) || 0;
        if (amount > taxAmount) {
          taxAmount = amount;
          log('info', 'TAX_UPDATED', {
            requestId,
            pageIndex,
            pageType,
            taxAmount,
            source: 'field_9'
          });
        }
      }
      
      // Extract due dates (merge from all pages)
      if (fields['6']) {
        const pageDueDates = extractDueDatesFromField(fields['6'], requestId, pageIndex);
        
        if (pageDueDates.due_date && !mergedDueDates.due_date) {
          mergedDueDates.due_date = pageDueDates.due_date;
        }
        if (pageDueDates.due_date_2 && !mergedDueDates.due_date_2) {
          mergedDueDates.due_date_2 = pageDueDates.due_date_2;
        }
        if (pageDueDates.due_date_3 && !mergedDueDates.due_date_3) {
          mergedDueDates.due_date_3 = pageDueDates.due_date_3;
        }
      }
      
      // Extract line items (from all pages with items)
      if (classifiedPage.hasLineItems) {
        const pageItems = extractLineItemsFromPage({ fields }, requestId, pageIndex);
        if (pageItems.length > 0) {
          allLineItems.push(...pageItems);
          log('info', 'LINE_ITEMS_MERGED', {
            requestId,
            pageIndex,
            pageType,
            newItemCount: pageItems.length,
            totalItemCount: allLineItems.length
          });
        }
      }
    });
  });
  
  log('info', 'DATA_MERGING_COMPLETE', {
    requestId,
    totalAmount,
    taxAmount,
    lineItemCount: allLineItems.length,
    supplier,
    dueDatesCount: Object.values(mergedDueDates).filter(d => d).length
  });
  
  return {
    totalAmount,
    taxAmount,
    supplier,
    referenceNumber,
    documentDate,
    terms,
    allLineItems,
    dueDates: mergedDueDates
  };
}

// 🔧 NEW: Helper function to extract due dates from field 6
function extractDueDatesFromField(raw6, requestId, pageIndex) {
  let dueDates = { due_date: '', due_date_2: '', due_date_3: '' };
  
  if (!raw6) return dueDates;
  
  const v = raw6?.value;
  
  if (typeof v === 'string') {
    try {
      const parsed = JSON.parse(v.trim());
      if (Array.isArray(parsed)) {
        dueDates.due_date = parsed[1] || '';
        dueDates.due_date_2 = parsed[2] || '';
        dueDates.due_date_3 = parsed[3] || '';
      } else {
        dueDates.due_date = String(parsed);
      }
    } catch (e) {
      dueDates.due_date = String(v);
    }
  } else if (Array.isArray(v)) {
    dueDates.due_date = v[1] || '';
    dueDates.due_date_2 = v[2] || '';
    dueDates.due_date_3 = v[3] || '';
  } else if (v?.tables && Array.isArray(v.tables[0]?.rows)) {
    const rows = v.tables[0].rows;
    dueDates.due_date = rows[1] || '';
    dueDates.due_date_2 = rows[2] || '';
    dueDates.due_date_3 = rows[3] || '';
  } else if (v?.rows) {
    dueDates.due_date = v.rows[1] || '';
    dueDates.due_date_2 = v.rows[2] || '';
    dueDates.due_date_3 = v.rows[3] || '';
  } else {
    dueDates.due_date = String(v);
  }
  
  return dueDates;
}

// 🔧 NEW: Helper function to extract line items from a single page
function extractLineItemsFromPage(page, requestId, pageIndex) {
  const fields = page.fields || {};
  const raw7 = fields['7'];
  let itemsData = [];
  
  if (raw7) {
    const v = raw7?.value;
    
    if (typeof v === 'string') {
      try {
        itemsData = JSON.parse(v.trim());
      } catch (e) {
        log('error', 'JSON_PARSE_FAILED', { requestId, pageIndex, error: e.message });
      }
    } else if (Array.isArray(v)) {
      itemsData = v;
    } else if (v?.tables && Array.isArray(v.tables[0]?.rows)) {
      itemsData = v.tables[0].rows;
    } else if (v?.rows) {
      itemsData = v.rows;
    } else if (raw7.tables?.[0]?.rows) {
      itemsData = raw7.tables[0].rows;
    } else if (raw7.rows) {
      itemsData = raw7.rows;
    }
  }
  
  // Try to find line items in ANY field that contains arrays
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
  
  const processedItems = [];
  
  if (Array.isArray(itemsData) && itemsData.length > 0) {
    itemsData.forEach((row, rowIndex) => {
      let itemNumber = '';
      let unitCost = 0;
      let quantity = 0;
      let description = '';
      
      if (Array.isArray(row)) {
        itemNumber = String(row[0] || '').trim();
        unitCost = parseFloat(row[1]) || 0;
        quantity = parseFloat(row[2]) || 0;
        description = String(row[3] || '').trim();
      } else if (typeof row === 'object' && row !== null) {
        itemNumber = String(row['Item Number'] || row.item_number || row.itemNumber || row.number || row.item || '').trim();
        unitCost = parseFloat(row['Unit Cost'] || row.unit_cost || row.unitCost || row.cost || row.price || 0);
        quantity = parseFloat(row.Quantity || row.quantity || row.qty || row.amount || 0);
        description = String(row.Description || row.description || row.desc || row.item_description || '').trim();
      } else {
        itemNumber = String(row || '').trim();
      }
      
      if (itemNumber || (unitCost > 0) || (quantity > 0)) {
        processedItems.push({
          item_number: itemNumber || `Item ${rowIndex + 1}`,
          description: description,
          quantity: quantity,
          unit_cost: unitCost,
          amount: quantity * unitCost,
          source_page: pageIndex + 1
        });
      }
    });
  }
  
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
