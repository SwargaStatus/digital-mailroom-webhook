// Digital Mailroom Webhook — FINAL VERSION with Updated Field Mapping + Line Number Support + Status Updates
// -----------------------------------------------------------------------------
// This version works with the updated Instabase field mapping (page type field removed)
// and includes Line Number extraction for subitems and status updates
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
    log('error', 'WEBHOOK_ERROR',
