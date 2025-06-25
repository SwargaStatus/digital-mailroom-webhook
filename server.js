const express = require('express');
const axios = require('axios');
const multer = require('multer');
const FormData = require('form-data');

const app = express();
app.use(express.json());

// Configuration
const INSTABASE_CONFIG = {
  baseUrl: 'https://aihub.instabase.com',
  apiKey: '8m91cy8nOMrZcZI5YspXpOPQtJ6p8o',
  deploymentId: '0197a3fe-599d-7bac-a34b-81704cc83beb',
  headers: {
    'IB-Context': 'sturgeontire',
    'Authorization': 'Bearer 8m91cy8nOMrZcZI5YspXpOPQtJ6p8o',
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
    
    // Step 3: Run deployment (CORRECTED API ENDPOINT)
    console.log(`Starting Instabase processing with deployment ${INSTABASE_CONFIG.deploymentId}...`);
    const runResponse = await axios.post(
      `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/deployments/${INSTABASE_CONFIG.deploymentId}/runs`,
      { batch_id: batchId },
      { headers: INSTABASE_CONFIG.headers }
    );
    
    const runId = runResponse.data.id;
    console.log('✅ Started processing run:', runId);
    
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
        { headers: INSTABASE_CONFIG.headers }
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
            { headers: INSTABASE_CONFIG.headers }
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
            { headers: INSTABASE_CONFIG.headers }
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
    
    // Step 5: Get results (CORRECTED API ENDPOINT)
    console.log('Retrieving extraction results...');
    const resultsResponse = await axios.get(
      `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${runId}/results`,
      { headers: INSTABASE_CONFIG.headers }
    );
    
    console.log('✅ Extraction results received');
    console.log('Results summary:', {
      filesProcessed: resultsResponse.data.files?.length || 0,
      totalDocuments: resultsResponse.data.files?.reduce((sum, file) => sum + (file.documents?.length || 0), 0) || 0
    });
    
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
  const documentGroups = {};
  
  extractedFiles.forEach(file => {
    file.documents.forEach(doc => {
      const fields = doc.fields;
      
      // Get invoice number (handle different field names)
      const invoiceNumber = 
        fields.invoice_number?.value || 
        fields.document_number?.value || 
        fields.number?.value ||
        'unknown';
      
      // Skip pages without invoice numbers (continuation pages)
      if (!invoiceNumber || invoiceNumber === 'none' || invoiceNumber === 'unknown') {
        return;
      }
      
      // Initialize group if it doesn't exist
      if (!documentGroups[invoiceNumber]) {
        documentGroups[invoiceNumber] = {
          invoice_number: invoiceNumber,
          document_type: fields.document_type?.value || 'invoice',
          supplier_name: '',
          total_amount: 0,
          document_date: '',
          due_date: '',
          pages: [],
          confidence: 0
        };
      }
      
      // Add page data to group
      const group = documentGroups[invoiceNumber];
      group.pages.push({
        page_type: fields.page_type?.value,
        file_name: file.original_file_name,
        fields: fields
      });
      
      // Update document-level fields from header pages or any page with the data
      if (fields.supplier_name?.value) {
        group.supplier_name = fields.supplier_name.value;
      }
      if (fields.total_amount?.value) {
        group.total_amount = fields.total_amount.value;
      }
      if (fields.document_date?.value) {
        group.document_date = fields.document_date.value;
      }
      if (fields.due_date?.value) {
        group.due_date = fields.due_date.value;
      }
      
      // Calculate average confidence
      const confidences = Object.values(fields)
        .map(field => field.confidence?.model || 0)
        .filter(conf => conf > 0);
      
      if (confidences.length > 0) {
        group.confidence = confidences.reduce((a, b) => a + b, 0) / confidences.length;
      }
    });
  });
  
  return Object.values(documentGroups);
}

// Create items in Monday.com Extracted Documents board
async function createMondayExtractedItems(documents, sourceItemId) {
  try {
    for (const doc of documents) {
      console.log(`Creating Monday.com item for ${doc.document_type} ${doc.invoice_number}...`);
      
      // Escape strings to prevent GraphQL syntax errors
      const escapedSupplier = (doc.supplier_name || '').replace(/"/g, '\\"');
      const escapedInvoiceNumber = (doc.invoice_number || '').replace(/"/g, '\\"');
      const escapedDocumentType = (doc.document_type || '').replace(/"/g, '\\"');
      
      const mutation = `
        mutation {
          create_item(
            board_id: ${MONDAY_CONFIG.extractedDocsBoardId}
            item_name: "${escapedDocumentType.toUpperCase()} ${escapedInvoiceNumber}"
            column_values: "{\\"source_request_id\\": \\"${sourceItemId}\\", \\"document_number\\": \\"${escapedInvoiceNumber}\\", \\"document_type\\": \\"${escapedDocumentType}\\", \\"amount\\": ${doc.total_amount || 0}, \\"supplier\\": \\"${escapedSupplier}\\", \\"document_date\\": \\"${doc.document_date}\\", \\"due_date\\": \\"${doc.due_date}\\", \\"extraction_status\\": \\"Extracted\\"}"
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
        console.error('Monday.com GraphQL errors:', response.data.errors);
      } else {
        console.log(`✅ Created Monday.com item for ${doc.document_type} ${doc.invoice_number} (ID: ${response.data.data.create_item.id})`);
      }
    }
  } catch (error) {
    console.error('Error creating Monday.com items:', error);
    console.error('Error response:', error.response?.data);
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
