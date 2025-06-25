const express = require('express');
const axios = require('axios');
const multer = require('multer');
const FormData = require('form-data');

const app = express();
app.use(express.json());

// Configuration
const INSTABASE_CONFIG = {
  baseUrl: 'https://aihub.instabase.com',
  apiKey: 'jEmrseIwOb9YtmJ6GzPAywtz53KnpS',
  deploymentId: '0197a3fe-599d-7bac-a34b-81704cc83beb',
  headers: {
    'IB-Context': 'sturgeontire',
    'Authorization': 'Bearer jEmrseIwOb9YtmJ6GzPAywtz53KnpS',
    'Content-Type': 'application/json'
  }
};

const MONDAY_CONFIG = {
  apiKey: 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjUzMDYzOTcxOSwiYWFpIjoxMSwidWlkIjo2Nzg2NjA4MywiaWFkIjoiMjAyNS0wNi0yNFQyMjoxNjowMC42NTJaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MjYyMDQ5OTgsInJnbiI6InVzZTEifQ.zv9EsISZnchs7WKSqN2t3UU1GwcLrzPGeaP7ssKIla8',
  fileUploadsBoardId: '9445652448', // File Uploads board
  extractedDocsBoardId: '9446325745' // Extracted Documents board
};

// Main webhook endpoint that Monday.com will call
app.post('/webhook/monday-to-instabase', async (req, res) => {
  try {
    console.log('=== WEBHOOK RECEIVED ===');
    console.log('Full request body:', JSON.stringify(req.body, null, 2));
    console.log('Headers:', JSON.stringify(req.headers, null, 2));
    
    // Handle Monday.com webhook validation challenge
    if (req.body.challenge) {
      console.log('Responding to Monday.com challenge:', req.body.challenge);
      return res.json({ challenge: req.body.challenge });
    }
    
    // Respond immediately to Monday.com to prevent timeout
    res.json({ 
      success: true, 
      message: 'Webhook received, processing started',
      timestamp: new Date().toISOString()
    });
    
    // Process in background
    processWebhookData(req.body);
    
  } catch (error) {
    console.error('=== WEBHOOK ERROR ===');
    console.error('Error details:', error);
    console.error('Stack trace:', error.stack);
    res.status(500).json({ error: error.message });
  }
});

// Process webhook data in background
async function processWebhookData(webhookData) {
  try {
    console.log('=== STARTING BACKGROUND PROCESSING ===');
    console.log('Webhook data structure:', JSON.stringify(webhookData, null, 2));
    
    // Extract Monday.com event data
    const event = webhookData.event;
    if (!event) {
      console.log('No event data found');
      return;
    }
    
    // Get the item details from Monday.com format
    const itemId = event.pulseId;
    const boardId = event.boardId;
    const columnId = event.columnId;
    const newValue = event.value?.label?.text;
    
    console.log('Extracted data:', {
      itemId,
      boardId, 
      columnId,
      newValue
    });
    
    // Only process if status changed to "Processing"
    if (columnId !== 'status' || newValue !== 'Processing') {
      console.log('Not a status change to Processing, skipping');
      return;
    }
    
    console.log('Status changed to Processing, getting item files...');
    
    // Get the full item data including files using the corrected method
    const pdfFiles = await getMondayItemFilesWithPublicUrl(itemId, boardId);
    
    if (!pdfFiles || pdfFiles.length === 0) {
      console.log('No PDF files found in Monday.com item');
      return;
    }
    
    console.log(`Found ${pdfFiles.length} PDF files, sending to Instabase...`);
    
    // Process files through Instabase
    const extractedData = await processFilesWithInstabase(pdfFiles, itemId);
    
    // Group pages by invoice number
    const groupedDocuments = groupPagesByInvoiceNumber(extractedData);
    
    console.log(`Grouped into ${groupedDocuments.length} documents, creating Monday.com items...`);
    
    // Create items in Monday.com Extracted Documents board
    await createMondayExtractedItems(groupedDocuments, itemId);
    
    console.log('=== PROCESSING COMPLETED SUCCESSFULLY ===');
    
  } catch (error) {
    console.error('=== BACKGROUND PROCESSING ERROR ===');
    console.error('Error details:', error);
    console.error('Stack trace:', error.stack);
  }
}

// CORRECTED: Get PDF files using public_url instead of protected_static URLs
async function getMondayItemFilesWithPublicUrl(itemId, boardId) {
  try {
    console.log(`Getting files with public_url for item ${itemId} on board ${boardId}`);
    
    // Use the corrected GraphQL query that gets assets with public_url
    const query = `
      query {
        items(ids: [${itemId}]) {
          id
          name
          assets {
            id
            name
            file_extension
            file_size
            public_url
            created_at
          }
        }
      }
    `;
    
    console.log('Monday.com GraphQL query for assets:', query);
    
    const response = await axios.post('https://api.monday.com/v2', {
      query: query
    }, {
      headers: {
        'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
        'Content-Type': 'application/json'
      }
    });
    
    console.log('Monday.com API response:', JSON.stringify(response.data, null, 2));
    
    if (!response.data.data || !response.data.data.items || response.data.data.items.length === 0) {
      throw new Error('Item not found in Monday.com');
    }
    
    const item = response.data.data.items[0];
    console.log('Found item:', item.name);
    
    // Get assets (files) from the item
    const assets = item.assets || [];
    console.log('Assets found:', assets.length);
    
    if (assets.length === 0) {
      throw new Error('No assets found in item');
    }
    
    // Filter for PDF files and convert to our expected format
    const pdfFiles = assets
      .filter(asset => {
        const isPdf = asset.file_extension?.toLowerCase() === 'pdf' || 
                     asset.name?.toLowerCase().endsWith('.pdf');
        console.log(`Asset ${asset.name}: isPdf=${isPdf}, extension=${asset.file_extension}`);
        return isPdf;
      })
      .map(asset => ({
        name: asset.name,
        public_url: asset.public_url, // This is the key - use public_url not protected URLs
        assetId: asset.id,
        file_extension: asset.file_extension,
        file_size: asset.file_size,
        created_at: asset.created_at
      }));
    
    console.log('PDF files extracted:', JSON.stringify(pdfFiles, null, 2));
    
    if (pdfFiles.length === 0) {
      throw new Error('No PDF files found in assets');
    }
    
    return pdfFiles;
    
  } catch (error) {
    console.error('Error getting Monday files with public_url:', error);
    console.error('Error details:', error.response?.data || error.message);
    throw error;
  }
}

// CORRECTED: Process files using public_url for downloads
async function processFilesWithInstabase(files, sourceItemId) {
  try {
    console.log('=== STARTING INSTABASE PROCESSING ===');
    
    // Step 1: Create batch
    const batchResponse = await axios.post(
      `${INSTABASE_CONFIG.baseUrl}/api/v2/batches`,
      { workspace: "nileshn_sturgeontire.com" },
      { headers: INSTABASE_CONFIG.headers }
    );
    
    const batchId = batchResponse.data.id;
    console.log('✅ Created Instabase batch:', batchId);
    
    // Step 2: Upload files to batch using public_url
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      console.log('Processing file:', JSON.stringify(file, null, 2));
      
      // Use the public_url directly - this is the corrected approach
      const fileUrl = file.public_url;
      
      if (!fileUrl) {
        console.error('No public_url found for file:', file);
        continue;
      }
      
      console.log('Downloading file from public_url:', fileUrl);
      
      // Download file using public_url (no authorization needed for public URLs)
      const fileResponse = await axios.get(fileUrl, { 
        responseType: 'arraybuffer',
        timeout: 30000 // 30 second timeout
      });
      
      const fileBuffer = Buffer.from(fileResponse.data);
      
      // Validate the downloaded file
      console.log(`Downloaded ${file.name}: ${fileBuffer.length} bytes`);
      
      // Enhanced PDF validation
      const pdfHeader = fileBuffer.slice(0, 4).toString();
      const pdfEnd = fileBuffer.slice(-20).toString();
      console.log(`PDF header for ${file.name}: ${pdfHeader}`);
      console.log(`PDF end for ${file.name}: ${pdfEnd}`);
      console.log(`File size: ${fileBuffer.length} bytes`);
      
      if (!pdfHeader.startsWith('%PDF')) {
        console.error(`File ${file.name} doesn't appear to be a valid PDF (header: ${pdfHeader})`);
        console.error('First 100 bytes:', fileBuffer.slice(0, 100).toString('hex'));
        continue;
      }
      
      // Check if PDF is password protected
      const pdfContent = fileBuffer.toString('binary');
      if (pdfContent.includes('/Encrypt')) {
        console.error(`File ${file.name} appears to be password protected`);
        continue;
      }
      
      console.log(`✅ PDF ${file.name} appears valid`);
      
      // Upload to Instabase
      console.log(`Uploading ${file.name} to Instabase batch ${batchId}...`);
      await axios.put(
        `${INSTABASE_CONFIG.baseUrl}/api/v2/batches/${batchId}/files/${file.name}`,
        fileBuffer,
        {
          headers: {
            'IB-Context': 'sturgeontire',
            'Authorization': `Bearer ${INSTABASE_CONFIG.apiKey}`,
            'Content-Type': 'application/octet-stream'
          },
          timeout: 60000 // 60 second timeout for upload
        }
      );
      console.log(`✅ Successfully uploaded ${file.name} to Instabase`);
    }
    
    // Step 3: Run deployment (CORRECTED API ENDPOINT) with retry logic
    console.log(`Starting Instabase processing with deployment ${INSTABASE_CONFIG.deploymentId}...`);
    
    let runResponse;
    let runId;
    
    // Retry logic for network issues
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        console.log(`Attempt ${attempt}: Starting deployment run...`);
        
        runResponse = await axios.post(
          `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/deployments/${INSTABASE_CONFIG.deploymentId}/runs`,
          { batch_id: batchId },
          { 
            headers: INSTABASE_CONFIG.headers,
            timeout: 30000 // 30 second timeout
          }
        );
        
        runId = runResponse.data.id;
        console.log(`✅ Started processing run: ${runId}`);
        break; // Success, exit retry loop
        
      } catch (error) {
        console.error(`Attempt ${attempt} failed:`, error.message);
        
        if (attempt === 3) {
          throw error; // Final attempt failed
        }
        
        // Wait before retry
        console.log(`Waiting 5 seconds before retry...`);
        await new Promise(resolve => setTimeout(resolve, 5000));
      }
    }
    
    // Step 4: Poll for completion with better status handling
    let status = 'RUNNING';
    let attempts = 0;
    const maxAttempts = 60; // 5 minutes timeout
    
    while (status === 'RUNNING' || status === 'PENDING') {
      if (attempts >= maxAttempts) {
        throw new Error('Processing timeout after 5 minutes');
      }
      
      await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5 seconds
      
      const statusResponse = await axios.get(
        `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${runId}`,
        { 
          headers: INSTABASE_CONFIG.headers,
          timeout: 15000 // 15 second timeout for status checks
        }
      );
      
      status = statusResponse.data.status;
      attempts++;
      console.log(`Run status: ${status} (attempt ${attempts})`);
      
      // If status is ERROR or FAILED, get detailed error information
      if (status === 'ERROR' || status === 'FAILED') {
        console.log('=== INSTABASE PROCESSING FAILED ===');
        console.log('Final status:', status);
        console.log('Full status response:', JSON.stringify(statusResponse.data, null, 2));
        
        // Try to get more error details
        try {
          const logsResponse = await axios.get(
            `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${runId}/logs`,
            { 
              headers: INSTABASE_CONFIG.headers,
              timeout: 15000
            }
          );
          console.log('=== INSTABASE ERROR LOGS ===');
          console.log(JSON.stringify(logsResponse.data, null, 2));
        } catch (logError) {
          console.log('Could not retrieve error logs:', logError.message);
        }
        
        // Try to get run details for more context
        try {
          const detailsResponse = await axios.get(
            `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${runId}/details`,
            { 
              headers: INSTABASE_CONFIG.headers,
              timeout: 15000
            }
          );
          console.log('=== RUN DETAILS ===');
          console.log(JSON.stringify(detailsResponse.data, null, 2));
        } catch (detailsError) {
          console.log('Could not retrieve run details:', detailsError.message);
        }
        
        throw new Error(`Instabase processing failed with status: ${status}. Check logs above for details.`);
      }
    }
    
    if (status !== 'COMPLETE') {
      console.log('=== INSTABASE PROCESSING UNEXPECTED STATUS ===');
      console.log('Final status:', status);
      throw new Error(`Processing ended with unexpected status: ${status}`);
    }
    
    console.log('✅ Instabase processing completed successfully');
    
    // Step 5: Get results (CORRECTED API ENDPOINT) with timeout
    console.log('Retrieving extraction results...');
    const resultsResponse = await axios.get(
      `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${runId}/results`,
      { 
        headers: INSTABASE_CONFIG.headers,
        timeout: 30000 // 30 second timeout for results
      }
    );
    
    console.log('✅ Extraction results received');
    console.log('Results summary:', {
      filesProcessed: resultsResponse.data.files?.length || 0,
      totalDocuments: resultsResponse.data.files?.reduce((sum, file) => sum + (file.documents?.length || 0), 0) || 0
    });
    
    // DEBUG: Log the actual extracted data structure
    console.log('=== EXTRACTED DATA DEBUG ===');
    resultsResponse.data.files?.forEach((file, fileIndex) => {
      console.log(`File ${fileIndex}: ${file.original_file_name}`);
      file.documents?.forEach((doc, docIndex) => {
        console.log(`  Document ${docIndex}:`);
        console.log(`    Page Type: ${doc.fields?.page_type?.value || 'none'}`);
        console.log(`    Available fields:`, Object.keys(doc.fields || {}));
        
        // Log key field values
        ['document_number', 'invoice_number', 'number', 'supplier', 'total', 'document_type', 'document_date'].forEach(fieldName => {
          if (doc.fields?.[fieldName]) {
            console.log(`    ${fieldName}: ${doc.fields[fieldName].value}`);
          }
        });
        
        // ALSO log the numeric fields to see what data is there
        console.log(`    Numeric field values:`);
        for (let i = 0; i < 10; i++) {
          if (doc.fields?.[i.toString()]) {
            console.log(`      Field ${i}: ${doc.fields[i.toString()].value}`);
          }
        }
      });
    });
    console.log('=== END DEBUG ===');
    
    return resultsResponse.data.files;
    
  } catch (error) {
    console.error('=== INSTABASE PROCESSING ERROR ===');
    console.error('Error details:', error.message);
    console.error('Error response:', error.response?.data);
    console.error('Stack trace:', error.stack);
    throw error;
  }
}

// Group pages by invoice number to reassemble complete documents
function groupPagesByInvoiceNumber(extractedFiles) {
  console.log('=== GROUPING DEBUG ===');
  console.log('Input files:', extractedFiles?.length || 0);
  
  const documentGroups = {};
  
  extractedFiles.forEach((file, fileIndex) => {
    console.log(`Processing file ${fileIndex}: ${file.original_file_name}`);
    
    file.documents.forEach((doc, docIndex) => {
      console.log(`  Processing document ${docIndex}`);
      const fields = doc.fields;
      console.log(`  Available fields:`, Object.keys(fields || {}));
      
      // UPDATED: Use numeric field mapping based on what we discovered
      const invoiceNumber = fields['0']?.value || 'unknown'; // Field 0 = Document Number
      const pageType = fields['1']?.value || 'unknown';      // Field 1 = Page Type
      const documentType = fields['2']?.value || 'invoice';  // Field 2 = Document Type
      const supplier = fields['3']?.value || '';             // Field 3 = Supplier
      const terms = fields['4']?.value || '';                // Field 4 = Terms
      const documentDate = fields['5']?.value || '';         // Field 5 = Document Date
      const dueDateData = fields['6']?.value || '';          // Field 6 = Due Date
      const itemsData = fields['7']?.value || [];            // Field 7 = Items
      const totalAmount = fields['8']?.value || 0;           // Field 8 = Total
      const taxAmount = fields['9']?.value || 0;             // Field 9 = Tax
      
      console.log(`  Extracted data:`);
      console.log(`    Invoice Number: "${invoiceNumber}"`);
      console.log(`    Page Type: "${pageType}"`);
      console.log(`    Document Type: "${documentType}"`);
      console.log(`    Supplier: "${supplier}"`);
      console.log(`    Total: "${totalAmount}"`);
      
      // Skip pages without invoice numbers (continuation pages)
      if (!invoiceNumber || invoiceNumber === 'none' || invoiceNumber === 'unknown') {
        console.log(`  Skipping document - no valid invoice number`);
        return;
      }
      
      console.log(`  Processing invoice: ${invoiceNumber}`);
      
      // Initialize group if it doesn't exist
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
          items: [], // For subitems
          pages: [],
          confidence: 0
        };
        console.log(`  Created new group for invoice: ${invoiceNumber}`);
      }
      
      // Add page data to group
      const group = documentGroups[invoiceNumber];
      group.pages.push({
        page_type: pageType,
        file_name: file.original_file_name,
        fields: fields
      });
      
      // Update document-level fields - prioritize "main" page data over "continuation"
      if (pageType === 'main' || !group.supplier_name) {
        if (supplier) {
          console.log(`  Found supplier: ${supplier}`);
          group.supplier_name = supplier;
        }
        
        if (totalAmount) {
          // Clean up the total amount - remove currency symbols and convert to number
          const totalStr = String(totalAmount).replace(/[^0-9.-]/g, '');
          group.total_amount = parseFloat(totalStr) || 0;
          console.log(`  Found total: ${group.total_amount}`);
        }
        
        if (taxAmount) {
          const taxStr = String(taxAmount).replace(/[^0-9.-]/g, '');
          group.tax_amount = parseFloat(taxStr) || 0;
          console.log(`  Found tax: ${group.tax_amount}`);
        }
        
        if (documentDate) {
          group.document_date = documentDate;
          console.log(`  Found document date: ${group.document_date}`);
        }
        
        if (terms) {
          group.terms = terms;
          console.log(`  Found terms: ${group.terms}`);
        }
      }
      
      // Handle Due Date (table format: [["Due Date"], ["2025-06-25"]])
      if (dueDateData && Array.isArray(dueDateData)) {
        console.log(`  Found due date data:`, dueDateData);
        
        // Extract dates from table format
        const dates = [];
        dueDateData.forEach(row => {
          if (Array.isArray(row) && row.length > 0) {
            // Skip header rows (like "Due Date")
            const cellValue = row[0];
            if (cellValue && cellValue !== 'Due Date' && cellValue.match(/\d{4}-\d{2}-\d{2}/)) {
              dates.push(cellValue);
            }
          }
        });
        
        group.due_date = dates[0] || '';
        group.due_date_2 = dates[1] || '';
        group.due_date_3 = dates[2] || '';
        console.log(`  Set due dates: ${group.due_date}, ${group.due_date_2}, ${group.due_date_3}`);
      }
      
      // Handle Items (array format for line items) - only from main pages
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
      
      // Calculate average confidence (if available)
      const confidences = Object.values(fields)
        .map(field => field.confidence?.model || 0)
        .filter(conf => conf > 0);
      
      if (confidences.length > 0) {
        group.confidence = confidences.reduce((a, b) => a + b, 0) / confidences.length;
      }
    });
  });
  
  console.log(`=== GROUPING RESULT ===`);
  console.log(`Found ${Object.keys(documentGroups).length} document groups:`);
  Object.keys(documentGroups).forEach(key => {
    console.log(`  Group "${key}": ${documentGroups[key].pages.length} pages, ${documentGroups[key].items.length} items`);
  });
  console.log('=== END GROUPING DEBUG ===');
  
  return Object.values(documentGroups);
}

// Create items in Monday.com Extracted Documents board
async function createMondayExtractedItems(documents, sourceItemId) {
  try {
    // First, let's get the board structure to see the actual column IDs
    console.log('=== GETTING BOARD STRUCTURE ===');
    const boardQuery = `
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
    
    const boardResponse = await axios.post('https://api.monday.com/v2', {
      query: boardQuery
    }, {
      headers: {
        'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
        'Content-Type': 'application/json'
      }
    });
    
    console.log('Board structure:', JSON.stringify(boardResponse.data, null, 2));
    
    const columns = boardResponse.data.data?.boards?.[0]?.columns || [];
    console.log('Available columns:');
    columns.forEach(col => {
      console.log(`  ${col.title}: ID = "${col.id}", Type = ${col.type}`);
    });
    
    for (const doc of documents) {
      console.log(`Creating Monday.com item for ${doc.document_type} ${doc.invoice_number}...`);
      
      // Escape strings to prevent GraphQL syntax errors
      const escapedSupplier = (doc.supplier_name || '').replace(/"/g, '\\"');
      const escapedInvoiceNumber = (doc.invoice_number || '').replace(/"/g, '\\"');
      const escapedDocumentType = (doc.document_type || '').replace(/"/g, '\\"');
      const escapedTerms = (doc.terms || '').replace(/"/g, '\\"');
      
      // Format dates for Monday.com (YYYY-MM-DD format)
      const formatDate = (dateStr) => {
        if (!dateStr) return '';
        try {
          const date = new Date(dateStr);
          return date.toISOString().split('T')[0];
        } catch (e) {
          return String(dateStr).slice(0, 10); // Try to extract YYYY-MM-DD format
        }
      };
      
      // Map to the actual column IDs we find
      const columnValues = {};
      
      // Try to map to the most likely column IDs based on your board export
      columns.forEach(col => {
        const title = col.title.toLowerCase();
        const id = col.id;
        const type = col.type;
        
        if (title.includes('supplier')) {
          columnValues[id] = escapedSupplier;
        } else if (title.includes('document number') || (title.includes('number') && !title.includes('total'))) {
          columnValues[id] = escapedInvoiceNumber;
        } else if (title.includes('document type') || (title.includes('type') && !title.includes('document'))) {
          // For dropdown columns, we need to be more careful
          if (type === 'dropdown') {
            // Let's just skip dropdowns for now since they need specific labels
            console.log(`Skipping dropdown column "${title}" - needs manual configuration`);
          } else {
            columnValues[id] = escapedDocumentType;
          }
        } else if (title.includes('document date') || (title.includes('date') && !title.includes('due'))) {
          columnValues[id] = formatDate(doc.document_date);
        } else if (title.includes('due date')) {
          columnValues[id] = formatDate(doc.due_date);
        } else if (title.includes('amount') && !title.includes('total') && !title.includes('tax')) {
          columnValues[id] = doc.total_amount || 0;
        } else if (title.includes('total amount')) {
          columnValues[id] = doc.total_amount || 0;
        } else if (title.includes('tax amount')) {
          columnValues[id] = doc.tax_amount || 0;
        } else if (title.includes('extraction status')) {
          // For status columns, use a valid status ID
          if (type === 'status') {
            columnValues[id] = { "index": 1 }; // Use "Done" status
          } else {
            columnValues[id] = "Extracted";
          }
        } else if (title.includes('status') && !title.includes('extraction')) {
          // Generic status column
          if (type === 'status') {
            columnValues[id] = { "index": 1 }; // Use "Done" status
          } else {
            columnValues[id] = "Extracted";
          }
        }
      });
      
      console.log('Mapped column values:', columnValues);
      
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
      
      console.log('Creating item with mutation:', mutation);
      
      const response = await axios.post('https://api.monday.com/v2', {
        query: mutation
      }, {
        headers: {
          'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
          'Content-Type': 'application/json'
        }
      });
      
      if (response.data.errors) {
        console.error('Monday.com GraphQL errors:', response.data.errors);
        continue;
      }
      
      const createdItemId = response.data.data.create_item.id;
      console.log(`✅ Created Monday.com item for ${doc.document_type} ${doc.invoice_number} (ID: ${createdItemId})`);
      
      // Create subitems for line items if they exist
      if (doc.items && doc.items.length > 0) {
        console.log(`Creating ${doc.items.length} subitems for line items...`);
        await createSubitemsForLineItems(createdItemId, doc.items, doc.invoice_number);
      }
    }
  } catch (error) {
    console.error('Error creating Monday.com items:', error);
    console.error('Error response:', error.response?.data);
    throw error;
  }
}

// Create subitems for inventory line items
async function createSubitemsForLineItems(parentItemId, items, invoiceNumber) {
  try {
    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      
      // Clean up item data based on the structure we discovered
      const itemNumber = String(item.item_number || '').replace(/"/g, '\\"');
      const description = String(item.description || itemNumber || `Item ${i + 1}`).replace(/"/g, '\\"');
      const quantity = item.quantity || 0;
      const unitCost = item.unit_cost || 0;
      const amount = item.amount || (quantity * unitCost);
      
      const subitemName = `${itemNumber}: ${description.substring(0, 40)}${description.length > 40 ? '...' : ''}`;
      
      console.log(`Creating subitem: ${subitemName} (Qty: ${quantity}, Cost: ${unitCost})`);
      
      // Create subitem with inventory data
      const subitemColumnValues = {
        item_number: itemNumber,
        description: description,
        quantity: quantity,
        unit_cost: unitCost,
        amount: amount
      };
      
      const subitemMutation = `
        mutation {
          create_subitem(
            parent_item_id: ${parentItemId}
            item_name: "${subitemName}"
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
          'Content-Type': 'application/json'
        }
      });
      
      if (subitemResponse.data.errors) {
        console.error(`Error creating subitem ${i + 1}:`, subitemResponse.data.errors);
      } else {
        console.log(`✅ Created subitem ${i + 1}: ${subitemName}`);
      }
    }
  } catch (error) {
    console.error('Error creating subitems:', error);
    throw error;
  }
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    timestamp: new Date().toISOString(),
    service: 'Digital Mailroom Webhook v2.0 - Using Monday.com public_url method'
  });
});

// Test endpoint to manually trigger file processing
app.post('/test/process-item/:itemId', async (req, res) => {
  try {
    const itemId = req.params.itemId;
    console.log(`Manual test triggered for item ${itemId}`);
    
    const files = await getMondayItemFilesWithPublicUrl(itemId, MONDAY_CONFIG.fileUploadsBoardId);
    
    res.json({
      success: true,
      itemId,
      filesFound: files.length,
      files: files.map(f => ({ name: f.name, hasPublicUrl: !!f.public_url }))
    });
    
  } catch (error) {
    console.error('Test endpoint error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Digital Mailroom webhook service v2.0 running on port ${PORT}`);
  console.log('📋 Key Changes:');
  console.log('   - Using Monday.com public_url instead of protected_static URLs');
  console.log('   - Enhanced error logging for Instabase processing');
  console.log('   - Improved PDF validation and file handling');
  console.log('   - Added test endpoint for manual testing');
  console.log('');
  console.log('🔗 Endpoints:');
  console.log(`   POST /webhook/monday-to-instabase - Main webhook`);
  console.log(`   POST /test/process-item/:itemId - Manual test`);
  console.log(`   GET  /health - Health check`);
});

module.exports = app;
