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
    
    // For now, just log what we receive and return success
    console.log('=== WEBHOOK PROCESSING COMPLETED (LOGGING ONLY) ===');
    return;
    
    // TODO: Re-enable this once we understand the data structure
    /*
    const { item_id, board_id, column_values } = webhookData;
    console.log('Processing item_id:', item_id, 'board_id:', board_id);
    
    if (!item_id || !board_id) {
      console.error('Missing item_id or board_id:', { item_id, board_id });
      return;
    }
    
    // Extract PDF file URLs from Monday.com item
    const pdfFiles = await getMondayItemFiles(item_id, board_id);
    
    if (!pdfFiles || pdfFiles.length === 0) {
      console.log('No PDF files found in Monday.com item');
      return;
    }
    
    // Process files through Instabase
    const extractedData = await processFilesWithInstabase(pdfFiles, item_id);
    
    // Group pages by invoice number
    const groupedDocuments = groupPagesByInvoiceNumber(extractedData);
    
    // Create items in Monday.com Extracted Documents board
    await createMondayExtractedItems(groupedDocuments, item_id);
    */
    
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
    const query = `
      query {
        items(ids: [${itemId}]) {
          column_values {
            id
            value
            text
          }
        }
      }
    `;
    
    const response = await axios.post('https://api.monday.com/v2', {
      query: query
    }, {
      headers: {
        'Authorization': MONDAY_CONFIG.apiKey,
        'Content-Type': 'application/json'
      }
    });
    
    // Extract file URLs from the PDF Attachments column
    const fileColumn = response.data.data.items[0].column_values.find(
      col => col.id === 'files' // Assuming 'files' is your PDF Attachments column ID
    );
    
    if (!fileColumn || !fileColumn.value) {
      throw new Error('No files found in Monday.com item');
    }
    
    const filesData = JSON.parse(fileColumn.value);
    return filesData.files || [];
    
  } catch (error) {
    console.error('Error getting Monday files:', error);
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
      console.log('Uploading file:', file.name);
      
      // Download file from Monday.com
      const fileResponse = await axios.get(file.url, { responseType: 'arraybuffer' });
      const fileBuffer = Buffer.from(fileResponse.data);
      
      // Upload to Instabase
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
    }
    
    // Step 3: Run deployment
    const runResponse = await axios.post(
      `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/deployments/${INSTABASE_CONFIG.deploymentId}/runs`,
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
        throw new Error('Processing timeout');
      }
      
      await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5 seconds
      
      const statusResponse = await axios.get(
        `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${runId}`,
        { headers: INSTABASE_CONFIG.headers }
      );
      
      status = statusResponse.data.status;
      attempts++;
      console.log(`Run status: ${status} (attempt ${attempts})`);
    }
    
    if (status !== 'COMPLETE') {
      throw new Error(`Processing failed with status: ${status}`);
    }
    
    // Step 5: Get results
    const resultsResponse = await axios.get(
      `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${runId}/results`,
      { headers: INSTABASE_CONFIG.headers }
    );
    
    return resultsResponse.data.files;
    
  } catch (error) {
    console.error('Instabase processing error:', error);
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
      
      await axios.post('https://api.monday.com/v2', {
        query: mutation
      }, {
        headers: {
          'Authorization': MONDAY_CONFIG.apiKey,
          'Content-Type': 'application/json'
        }
      });
      
      console.log(`Created Monday.com item for ${doc.document_type} ${doc.invoice_number}`);
    }
  } catch (error) {
    console.error('Error creating Monday.com items:', error);
    throw error;
  }
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'healthy', timestamp: new Date().toISOString() });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Digital Mailroom webhook service running on port ${PORT}`);
  console.log('Endpoints:');
  console.log(`  POST /webhook/monday-to-instabase - Main webhook`);
  console.log(`  GET  /health - Health check`);
});

module.exports = app;
