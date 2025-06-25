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
    
    // Get the full item data including files
    const pdfFiles = await getMondayItemFiles(itemId, boardId);
    
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

// Get PDF files from Monday.com item
async function getMondayItemFiles(itemId, boardId) {
  try {
    console.log(`Getting files for item ${itemId} on board ${boardId}`);
    
    const query = `
      query {
        items(ids: [${itemId}]) {
          id
          name
          column_values {
            id
            value
            text
            type
          }
        }
      }
    `;
    
    console.log('Monday.com GraphQL query:', query);
    
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
    
    // Find the files column (look for file type columns)
    const fileColumns = item.column_values.filter(col => 
      col.type === 'file' || col.id === 'files' || col.id.includes('file')
    );
    
    console.log('File columns found:', fileColumns);
    
    if (fileColumns.length === 0) {
      throw new Error('No file columns found');
    }
    
    // Get files from the first file column
    const fileColumn = fileColumns[0];
    if (!fileColumn.value) {
      throw new Error('No files in file column');
    }
    
    let filesData;
    try {
      filesData = JSON.parse(fileColumn.value);
    } catch (e) {
      console.error('Error parsing file column value:', fileColumn.value);
      throw new Error('Invalid file data format');
    }
    
    console.log('Parsed files data:', JSON.stringify(filesData, null, 2));
    
    // Monday.com file structure might be different
    const files = filesData.files || filesData || [];
    
    if (!Array.isArray(files)) {
      console.error('Files data is not an array:', files);
      throw new Error('Invalid files structure');
    }
    
    return files;
    
  } catch (error) {
    console.error('Error getting Monday files:', error);
    console.error('Error details:', error.response?.data || error.message);
    throw error;
  }
}

// Process files through Instabase API
async function processFilesWithInstabase(files, sourceItemId) {
  try {
    // Step 1: Create batch
    const batchResponse = await axios.post(
      `${INSTABASE_CONFIG.baseUrl}/api/v2/batches`,
      { workspace: "nileshn_sturgeontire.com" },
      { headers: INSTABASE_CONFIG.headers }
    );
    
    const batchId = batchResponse.data.id;
    console.log('Created batch:', batchId);
    
    // Step 2: Upload files to batch
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      console.log('Processing file:', JSON.stringify(file, null, 2));
      
      // Initialize fileUrl variable
      let fileUrl = file.url || file.public_url;
      
      // If no direct URL, use Monday.com assets API to get the file URL
      if (!fileUrl && file.assetId) {
        // Use Monday.com assets API to get the file
        try {
          const assetQuery = `
            query {
              assets(ids: [${file.assetId}]) {
                id
                name
                url
                file_extension
                file_size
              }
            }
          `;
          
          const assetResponse = await axios.post('https://api.monday.com/v2', {
            query: assetQuery
          }, {
            headers: {
              'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
              'Content-Type': 'application/json'
            }
          });
          
          if (assetResponse.data.data?.assets?.[0]?.url) {
            fileUrl = assetResponse.data.data.assets[0].url;
            console.log('Got asset URL from Monday.com API:', fileUrl);
          }
        } catch (assetError) {
          console.error('Error getting asset URL:', assetError);
        }
      }
      
      if (!fileUrl) {
        console.error('No URL found for file:', file);
        continue;
      }
      
      console.log('Downloading file from URL:', fileUrl);
      
      // Download file from Monday.com
      const fileResponse = await axios.get(fileUrl, { 
        responseType: 'arraybuffer',
        headers: {
          'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`
        }
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
          }
        }
      );
      console.log(`✅ Successfully uploaded ${file.name} to Instabase`);
    }
    
    // Step 3: Run deployment
    console.log(`Starting Instabase processing with deployment ${INSTABASE_CONFIG.deploymentId}...`);
    const runResponse = await axios.post(
      `${INSTABASE_CONFIG.baseUrl}/api/v2/deployments/${INSTABASE_CONFIG.deploymentId}/runs`,
      { batch_id: batchId },
      { headers: INSTABASE_CONFIG.headers }
    );
    
    const runId = runResponse.data.id;
    console.log('Started processing run:', runId);
    
    // Step 4: Poll for completion
    let status = 'RUNNING';
    let attempts = 0;
    const maxAttempts = 60; // 5 minutes timeout
    
    while (status === 'RUNNING' || status === 'PENDING') {
      if (attempts >= maxAttempts) {
        throw new Error('Processing timeout after 5 minutes');
      }
      
      await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5 seconds
      
      const statusResponse = await axios.get(
        `${INSTABASE_CONFIG.baseUrl}/api/v2/runs/${runId}`,
        { headers: INSTABASE_CONFIG.headers }
      );
      
      status = statusResponse.data.status;
      attempts++;
      console.log(`Run status: ${status} (attempt ${attempts})`);
      
      // If status is ERROR, let's get more details before failing
      if (status === 'ERROR') {
        console.log('=== INSTABASE PROCESSING FAILED ===');
        console.log('Final status:', status);
        console.log('Full status response:', JSON.stringify(statusResponse.data, null, 2));
        
        // Try to get more error details
        try {
          const errorResponse = await axios.get(
            `${INSTABASE_CONFIG.baseUrl}/api/v2/runs/${runId}/logs`,
            { headers: INSTABASE_CONFIG.headers }
          );
          console.log('Instabase error logs:', JSON.stringify(errorResponse.data, null, 2));
        } catch (logError) {
          console.log('Could not retrieve error logs:', logError.message);
        }
        
        throw new Error(`Processing failed with status: ${status}. Check logs above for details.`);
      }
    }
    
    if (status !== 'COMPLETE') {
      console.log('=== INSTABASE PROCESSING UNEXPECTED STATUS ===');
      console.log('Final status:', status);
      throw new Error(`Processing ended with unexpected status: ${status}`);
    }
    
    console.log('✅ Instabase processing completed successfully');
    
    // Step 5: Get results
    console.log('Retrieving extraction results...');
    const resultsResponse = await axios.get(
      `${INSTABASE_CONFIG.baseUrl}/api/v2/runs/${runId}/results`,
      { headers: INSTABASE_CONFIG.headers }
    );
    
    console.log('Extraction results received:', JSON.stringify(resultsResponse.data, null, 2));
    return resultsResponse.data.files;
    
  } catch (error) {
    console.error('Instabase processing error:', error);
    console.error('Error response:', error.response?.data);
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
      
      // Update document-level fields from header pages
      if (fields.page_type?.value === 'header') {
        group.supplier_name = fields.supplier_name?.value || '';
        group.total_amount = fields.total_amount?.value || 0;
        group.document_date = fields.document_date?.value || '';
        group.due_date = fields.due_date?.value || '';
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
      
      const mutation = `
        mutation {
          create_item(
            board_id: ${MONDAY_CONFIG.extractedDocsBoardId}
            item_name: "${doc.document_type.toUpperCase()} ${doc.invoice_number}"
            column_values: "{\\"source_request_id\\": \\"${sourceItemId}\\", \\"document_number\\": \\"${doc.invoice_number}\\", \\"document_type\\": \\"${doc.document_type}\\", \\"amount\\": ${doc.total_amount}, \\"supplier\\": \\"${doc.supplier_name}\\", \\"document_date\\": \\"${doc.document_date}\\", \\"due_date\\": \\"${doc.due_date}\\", \\"extraction_status\\": \\"Extracted\\"}"
          ) {
            id
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
        console.log(`✅ Created Monday.com item for ${doc.document_type} ${doc.invoice_number}`);
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
    service: 'Digital Mailroom Webhook'
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Digital Mailroom webhook service running on port ${PORT}`);
  console.log('Endpoints:');
  console.log(`  POST /webhook/monday-to-instabase - Main webhook`);
  console.log(`  GET  /health - Health check`);
});

module.exports = app;
