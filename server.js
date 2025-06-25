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
  fileUploadsBoardId: '9445652448',
  extractedDocsBoardId: '9446325745'
};

app.post('/webhook/monday-to-instabase', async (req, res) => {
  try {
    console.log('=== WEBHOOK RECEIVED ===');
    console.log('Full request body:', JSON.stringify(req.body, null, 2));
    
    if (req.body.challenge) {
      console.log('Responding to Monday.com challenge:', req.body.challenge);
      return res.json({ challenge: req.body.challenge });
    }
    
    res.json({ 
      success: true, 
      message: 'Webhook received, processing started',
      timestamp: new Date().toISOString()
    });
    
    processWebhookData(req.body);
    
  } catch (error) {
    console.error('=== WEBHOOK ERROR ===');
    console.error('Error details:', error);
    res.status(500).json({ error: error.message });
  }
});

async function processWebhookData(webhookData) {
  try {
    console.log('=== STARTING BACKGROUND PROCESSING ===');
    
    const event = webhookData.event;
    if (!event) {
      console.log('No event data found');
      return;
    }
    
    const itemId = event.pulseId;
    const boardId = event.boardId;
    const columnId = event.columnId;
    const newValue = event.value?.label?.text;
    
    console.log('Extracted data:', { itemId, boardId, columnId, newValue });
    
    if (columnId !== 'status' || newValue !== 'Processing') {
      console.log('Not a status change to Processing, skipping');
      return;
    }
    
    console.log('Status changed to Processing, getting item files...');
    
    const pdfFiles = await getMondayItemFilesWithPublicUrl(itemId, boardId);
    
    if (!pdfFiles || pdfFiles.length === 0) {
      console.log('No PDF files found in Monday.com item');
      return;
    }
    
    console.log(`Found ${pdfFiles.length} PDF files, sending to Instabase...`);
    
    const extractionResult = await processFilesWithInstabase(pdfFiles, itemId);
    const groupedDocuments = groupPagesByInvoiceNumber(extractionResult.files);
    
    console.log(`Grouped into ${groupedDocuments.length} documents, creating Monday.com items...`);
    
    await createMondayExtractedItems(groupedDocuments, itemId, extractionResult.originalFiles);
    
    console.log('=== PROCESSING COMPLETED SUCCESSFULLY ===');
    
  } catch (error) {
    console.error('=== BACKGROUND PROCESSING ERROR ===');
    console.error('Error details:', error);
  }
}

async function getMondayItemFilesWithPublicUrl(itemId, boardId) {
  try {
    console.log(`Getting files with public_url for item ${itemId} on board ${boardId}`);
    
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
    
    const assets = item.assets || [];
    console.log('Assets found:', assets.length);
    
    if (assets.length === 0) {
      throw new Error('No assets found in item');
    }
    
    const pdfFiles = assets
      .filter(asset => {
        const isPdf = asset.file_extension?.toLowerCase() === 'pdf' || 
                     asset.name?.toLowerCase().endsWith('.pdf');
        console.log(`Asset ${asset.name}: isPdf=${isPdf}, extension=${asset.file_extension}`);
        return isPdf;
      })
      .map(asset => ({
        name: asset.name,
        public_url: asset.public_url,
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
    throw error;
  }
}

async function processFilesWithInstabase(files, sourceItemId) {
  try {
    console.log('=== STARTING INSTABASE PROCESSING ===');
    
    const batchResponse = await axios.post(
      `${INSTABASE_CONFIG.baseUrl}/api/v2/batches`,
      { workspace: "nileshn_sturgeontire.com" },
      { headers: INSTABASE_CONFIG.headers }
    );
    
    const batchId = batchResponse.data.id;
    console.log('✅ Created Instabase batch:', batchId);
    
    const originalFiles = [];
    
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      console.log('Processing file:', JSON.stringify(file, null, 2));
      
      const fileUrl = file.public_url;
      
      if (!fileUrl) {
        console.error('No public_url found for file:', file);
        continue;
      }
      
      console.log('Downloading file from public_url:', fileUrl);
      
      const fileResponse = await axios.get(fileUrl, { 
        responseType: 'arraybuffer',
        timeout: 30000
      });
      
      const fileBuffer = Buffer.from(fileResponse.data);
      
      originalFiles.push({
        name: file.name,
        buffer: fileBuffer,
        public_url: fileUrl,
        asset_id: file.assetId
      });
      
      console.log(`Downloaded ${file.name}: ${fileBuffer.length} bytes`);
      
      const pdfHeader = fileBuffer.slice(0, 4).toString();
      console.log(`PDF header for ${file.name}: ${pdfHeader}`);
      
      if (!pdfHeader.startsWith('%PDF')) {
        console.error(`File ${file.name} doesn't appear to be a valid PDF`);
        continue;
      }
      
      const pdfContent = fileBuffer.toString('binary');
      if (pdfContent.includes('/Encrypt')) {
        console.error(`File ${file.name} appears to be password protected`);
        continue;
      }
      
      console.log(`✅ PDF ${file.name} appears valid`);
      
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
          timeout: 60000
        }
      );
      console.log(`✅ Successfully uploaded ${file.name} to Instabase`);
    }
    
    console.log(`Starting Instabase processing with deployment ${INSTABASE_CONFIG.deploymentId}...`);
    
    let runResponse;
    let runId;
    
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        console.log(`Attempt ${attempt}: Starting deployment run...`);
        
        runResponse = await axios.post(
          `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/deployments/${INSTABASE_CONFIG.deploymentId}/runs`,
          { batch_id: batchId },
          { 
            headers: INSTABASE_CONFIG.headers,
            timeout: 30000
          }
        );
        
        runId = runResponse.data.id;
        console.log(`✅ Started processing run: ${runId}`);
        break;
        
      } catch (error) {
        console.error(`Attempt ${attempt} failed:`, error.message);
        
        if (attempt === 3) {
          throw error;
        }
        
        console.log(`Waiting 5 seconds before retry...`);
        await new Promise(resolve => setTimeout(resolve, 5000));
      }
    }
    
    let status = 'RUNNING';
    let attempts = 0;
    const maxAttempts = 60;
    
    while (status === 'RUNNING' || status === 'PENDING') {
      if (attempts >= maxAttempts) {
        throw new Error('Processing timeout after 5 minutes');
      }
      
      await new Promise(resolve => setTimeout(resolve, 5000));
      
      const statusResponse = await axios.get(
        `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${runId}`,
        { 
          headers: INSTABASE_CONFIG.headers,
          timeout: 15000
        }
      );
      
      status = statusResponse.data.status;
      attempts++;
      console.log(`Run status: ${status} (attempt ${attempts})`);
      
      if (status === 'ERROR' || status === 'FAILED') {
        console.log('=== INSTABASE PROCESSING FAILED ===');
        console.log('Final status:', status);
        console.log('Full status response:', JSON.stringify(statusResponse.data, null, 2));
        
        throw new Error(`Instabase processing failed with status: ${status}`);
      }
    }
    
    if (status !== 'COMPLETE') {
      console.log('=== INSTABASE PROCESSING UNEXPECTED STATUS ===');
      console.log('Final status:', status);
      throw new Error(`Processing ended with unexpected status: ${status}`);
    }
    
    console.log('✅ Instabase processing completed successfully');
    
    console.log('Retrieving extraction results...');
    const resultsResponse = await axios.get(
      `${INSTABASE_CONFIG.baseUrl}/api/v2/apps/runs/${runId}/results`,
      { 
        headers: INSTABASE_CONFIG.headers,
        timeout: 30000
      }
    );
    
    console.log('✅ Extraction results received');
    console.log('Results summary:', {
      filesProcessed: resultsResponse.data.files?.length || 0,
      totalDocuments: resultsResponse.data.files?.reduce((sum, file) => sum + (file.documents?.length || 0), 0) || 0
    });
    
    console.log('=== EXTRACTED DATA DEBUG ===');
    resultsResponse.data.files?.forEach((file, fileIndex) => {
      console.log(`File ${fileIndex}: ${file.original_file_name}`);
      file.documents?.forEach((doc, docIndex) => {
        console.log(`  Document ${docIndex}:`);
        console.log(`    Page Type: ${doc.fields?.['1']?.value || 'none'}`);
        console.log(`    Available fields:`, Object.keys(doc.fields || {}));
        
        console.log(`    Numeric field values:`);
        for (let i = 0; i < 10; i++) {
          if (doc.fields?.[i.toString()]) {
            console.log(`      Field ${i}: ${doc.fields[i.toString()].value}`);
          }
        }
      });
    });
    console.log('=== END DEBUG ===');
    
    return {
      files: resultsResponse.data.files,
      originalFiles: originalFiles
    };
    
  } catch (error) {
    console.error('=== INSTABASE PROCESSING ERROR ===');
    console.error('Error details:', error.message);
    throw error;
  }
}

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
      
      console.log(`  Extracted data:`);
      console.log(`    Invoice Number: "${invoiceNumber}"`);
      console.log(`    Page Type: "${pageType}"`);
      console.log(`    Document Type: "${documentType}"`);
      console.log(`    Supplier: "${supplier}"`);
      console.log(`    Total: "${totalAmount}"`);
      
      if (!invoiceNumber || invoiceNumber === 'none' || invoiceNumber === 'unknown') {
        console.log(`  Skipping document - no valid invoice number`);
        return;
      }
      
      console.log(`  Processing invoice: ${invoiceNumber}`);
      
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
        console.log(`  Created new group for invoice: ${invoiceNumber}`);
      }
      
      const group = documentGroups[invoiceNumber];
      group.pages.push({
        page_type: pageType,
        file_name: file.original_file_name,
        fields: fields
      });
      
      if (pageType === 'main' || !group.supplier_name) {
        if (supplier) {
          console.log(`  Found supplier: ${supplier}`);
          group.supplier_name = supplier;
        }
        
        if (totalAmount) {
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
      
      if (dueDateData && Array.isArray(dueDateData)) {
        console.log(`  Found due date data on ${pageType} page:`, dueDateData);
        
        const dates = [];
        dueDateData.forEach(row => {
          if (Array.isArray(row) && row.length > 0) {
            const cellValue = row[0];
            console.log(`    Checking cell value: "${cellValue}"`);
            if (cellValue && cellValue !== 'Due Date' && cellValue.match(/\d{4}-\d{2}-\d{2}/)) {
              dates.push(cellValue);
              console.log(`    Added date: ${cellValue}`);
            }
          }
        });
        
        console.log(`  Extracted dates array:`, dates);
        
        // Always update if we have dates (prioritize main page, but accept any page with dates)
        if (dates.length > 0 && (pageType === 'main' || !group.due_date)) {
          group.due_date = dates[0] || '';
          group.due_date_2 = dates[1] || '';
          group.due_date_3 = dates[2] || '';
          console.log(`  Set due dates: ${group.due_date}, ${group.due_date_2}, ${group.due_date_3}`);
        }
      }
      
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

async function createMondayExtractedItems(documents, sourceItemId, originalFiles) {
  try {
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
    console.log('Available columns:');
    columns.forEach(col => {
      console.log(`  ${col.title}: ID = "${col.id}", Type = ${col.type}`);
    });
    
    for (const doc of documents) {
      console.log(`Creating Monday.com item for ${doc.document_type} ${doc.invoice_number}...`);
      
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
      
      const columnValues = {};
      
      columns.forEach(col => {
        const title = col.title.toLowerCase();
        const id = col.id;
        const type = col.type;
        
        if (title.includes('supplier')) {
          columnValues[id] = escapedSupplier;
        } else if (title.includes('document number') || (title.includes('number') && !title.includes('total'))) {
          columnValues[id] = escapedInvoiceNumber;
        } else if (title.includes('document type') || (title.includes('type') && !title.includes('document'))) {
          if (type === 'dropdown') {
            let settings = {};
            try {
              settings = JSON.parse(col.settings_str || '{}');
            } catch (e) {
              console.log('Could not parse dropdown settings');
            }
            
            if (settings.labels && settings.labels.length > 0) {
              const matchingLabel = settings.labels.find(label => 
                label.name?.toLowerCase() === escapedDocumentType.toLowerCase()
              );
              
              if (matchingLabel) {
                columnValues[id] = matchingLabel.name;
                console.log(`Found matching dropdown option: ${matchingLabel.name}`);
              } else {
                columnValues[id] = settings.labels[0].name;
                console.log(`Using first available dropdown option: ${settings.labels[0].name}`);
              }
            }
          } else {
            columnValues[id] = escapedDocumentType;
          }
        } else if (title.includes('document date') || (title.includes('date') && !title.includes('due'))) {
          columnValues[id] = formatDate(doc.document_date);
        } else if (title === 'due date' || title.includes('due date') && !title.includes('2') && !title.includes('3')) {
          columnValues[id] = formatDate(doc.due_date);
        } else if (title.includes('due date 2')) {
          columnValues[id] = formatDate(doc.due_date_2);
        } else if (title.includes('due date 3')) {
          columnValues[id] = formatDate(doc.due_date_3);
        } else if (title.includes('amount') && !title.includes('total') && !title.includes('tax')) {
          columnValues[id] = doc.total_amount || 0;
        } else if (title.includes('total amount')) {
          columnValues[id] = doc.total_amount || 0;
        } else if (title.includes('tax amount')) {
          columnValues[id] = doc.tax_amount || 0;
        } else if (title.includes('extraction status')) {
          if (type === 'status') {
            columnValues[id] = { "index": 1 };
          } else {
            columnValues[id] = "Extracted";
          }
        } else if (title.includes('status') && !title.includes('extraction')) {
          if (type === 'status') {
            columnValues[id] = { "index": 1 };
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
      
      // Upload the PDF file to Document File column
      await uploadPdfToMondayItem(createdItemId, originalFiles, columns);
      
      if (doc.items && doc.items.length > 0) {
        console.log(`Creating ${doc.items.length} subitems for line items...`);
        await createSubitemsForLineItems(createdItemId, doc.items);
      }
    }
  } catch (error) {
    console.error('Error creating Monday.com items:', error);
    throw error;
  }
}

async function uploadPdfToMondayItem(itemId, originalFiles, columns) {
  try {
    if (!originalFiles || originalFiles.length === 0) {
      console.log('No original files to upload');
      return;
    }
    
    // Find the Document File column
    const fileColumn = columns.find(col => 
      col.title.toLowerCase().includes('document file') || 
      col.title.toLowerCase().includes('file')
    );
    
    if (!fileColumn) {
      console.log('No document file column found');
      return;
    }
    
    console.log(`Uploading PDF to column: ${fileColumn.title} (${fileColumn.id})`);
    
    // Upload the first PDF file
    const pdfFile = originalFiles[0];
    
    // Use Monday.com's file upload API with form-data
    const FormData = require('form-data');
    const form = new FormData();
    
  const fileUploadMutation = `
    mutation add_file_to_column($item_id: ID!, $column_id: String!, $file: File!) {
        add_file_to_column(item_id: $item_id, column_id: $column_id, file: $file) {
          id
        }
      }
    `;
    
    form.append('query', fileUploadMutation);
    form.append('variables', JSON.stringify({
      item_id: itemId.toString(),
      column_id: fileColumn.id
    }));
    form.append('file', pdfFile.buffer, {
      filename: pdfFile.name,
      contentType: 'application/pdf'
    });
    
    const uploadResponse = await axios.post('https://api.monday.com/v2/file', form, {
      headers: {
        'Authorization': `Bearer ${MONDAY_CONFIG.apiKey}`,
        ...form.getHeaders()
      },
      timeout: 30000
    });
    
    if (uploadResponse.data.errors) {
      console.error('File upload errors:', uploadResponse.data.errors);
    } else {
      console.log(`✅ Uploaded PDF file to Monday.com item ${itemId}`);
    }
    
  } catch (error) {
    console.error('Error uploading PDF to Monday.com:', error.message);
    // Don't throw error - continue with other processing
  }
}

async function createSubitemsForLineItems(parentItemId, items) {
  try {
    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      
      const itemNumber = String(item.item_number || '').replace(/"/g, '\\"');
      const description = String(item.description || itemNumber || `Item ${i + 1}`).replace(/"/g, '\\"');
      const quantity = item.quantity || 0;
      const unitCost = item.unit_cost || 0;
      const amount = item.amount || (quantity * unitCost);
      
      const subitemName = `${itemNumber}: ${description.substring(0, 40)}${description.length > 40 ? '...' : ''}`;
      
      console.log(`Creating subitem: ${subitemName} (Qty: ${quantity}, Cost: ${unitCost})`);
      
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

      const subitemResponse = await axios.post(
        'https://api.monday.com/v2',
        { query: subitemMutation },
        {
          headers: {
            Authorization: `Bearer ${MONDAY_CONFIG.apiKey}`,
            'Content-Type': 'application/json',
          },
          timeout: 30000,
        }
      );

      if (subitemResponse.data.errors) {
        console.error(
          `Error creating subitem ${i + 1}:`,
          subitemResponse.data.errors
        );
      } else {
        console.log(
          `✅ Created subitem ${i + 1}: ${subitemName} (ID: ${subitemResponse.data.data.create_subitem.id})`
        );
      }
    }
  } catch (error) {
    console.error('Error creating subitems:', error);
    throw error;
  }
}
/* ---------- health + test endpoints ---------- */
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    service: 'Digital-Mailroom Webhook v3.1'
  });
});

app.post('/test/process-item/:itemId', async (req, res) => {
  try {
    const itemId = req.params.itemId;
    const files = await getMondayItemFilesWithPublicUrl(
      itemId,
      MONDAY_CONFIG.fileUploadsBoardId
    );
    res.json({
      success: true,
      itemId,
      filesFound: files.length,
      files: files.map(f => ({ name: f.name, hasPublicUrl: !!f.public_url }))
    });
  } catch (err) {
    console.error('Test endpoint error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

/* ---------- start the server ---------- */
const PORT = process.env.PORT || 3000;          // Railway injects PORT
app.listen(PORT, '0.0.0.0', () => {
  console.log(
    `🚀 Digital-Mailroom webhook service running on port ${PORT}`
  );
});

/* optional for tests */
module.exports = app;

