const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { fork } = require('child_process');
const path = require('path');
const amqp = require('amqplib');
const { sendOrder } = require('./producer');

const PORT = process.env.PORT || 3000;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Track child processes
let cozinhaProcess = null;
let financeiroProcess = null;

// Broadcast helper
function broadcast(data) {
  const payload = JSON.stringify(data);
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });
}

// Log streaming function
function sendSystemLog(message) {
  const timestamp = new Date().toLocaleTimeString();
  broadcast({
    type: 'system_log',
    message: `[Sistema - ${timestamp}] ${message}`
  });
}

// Start Consumer Cozinha
function startCozinha() {
  if (cozinhaProcess) {
    sendSystemLog('Consumidor da Cozinha já está rodando.');
    return;
  }

  sendSystemLog('Iniciando processo do Consumidor da Cozinha...');
  cozinhaProcess = fork(path.join(__dirname, 'consumer_cozinha.js'));

  cozinhaProcess.on('message', (msg) => {
    if (msg.type === 'log') {
      broadcast({ type: 'cozinha_log', message: msg.message });
    }
  });

  cozinhaProcess.on('exit', (code, signal) => {
    cozinhaProcess = null;
    sendSystemLog(`Processo da Cozinha encerrado (Código: ${code || 0}, Sinal: ${signal || 'nenhum'})`);
    broadcastStatus();
  });

  broadcastStatus();
}

// Stop Consumer Cozinha
function stopCozinha() {
  if (!cozinhaProcess) {
    sendSystemLog('Consumidor da Cozinha já está parado.');
    return;
  }

  sendSystemLog('Parando processo do Consumidor da Cozinha...');
  cozinhaProcess.kill('SIGTERM');
}

// Start Consumer Financeiro
function startFinanceiro() {
  if (financeiroProcess) {
    sendSystemLog('Consumidor do Financeiro já está rodando.');
    return;
  }

  sendSystemLog('Iniciando processo do Consumidor do Financeiro...');
  financeiroProcess = fork(path.join(__dirname, 'consumer_financeiro.js'));

  financeiroProcess.on('message', (msg) => {
    if (msg.type === 'log') {
      broadcast({ type: 'financeiro_log', message: msg.message });
    }
  });

  financeiroProcess.on('exit', (code, signal) => {
    financeiroProcess = null;
    sendSystemLog(`Processo do Financeiro encerrado (Código: ${code || 0}, Sinal: ${signal || 'nenhum'})`);
    broadcastStatus();
  });

  broadcastStatus();
}

// Stop Consumer Financeiro
function stopFinanceiro() {
  if (!financeiroProcess) {
    sendSystemLog('Consumidor do Financeiro já está parado.');
    return;
  }

  sendSystemLog('Parando processo do Consumidor do Financeiro...');
  financeiroProcess.kill('SIGTERM');
}

// Broadcast Status
function broadcastStatus() {
  broadcast({
    type: 'status',
    cozinhaRunning: cozinhaProcess !== null,
    financeiroRunning: financeiroProcess !== null
  });
}

// WebSocket connections
wss.on('connection', (ws) => {
  console.log('Cliente conectado via WebSocket');
  
  // Send initial statuses
  ws.send(JSON.stringify({
    type: 'status',
    cozinhaRunning: cozinhaProcess !== null,
    financeiroRunning: financeiroProcess !== null
  }));

  ws.on('message', async (message) => {
    try {
      const data = JSON.parse(message);
      switch (data.action) {
        case 'toggle_cozinha':
          if (cozinhaProcess) stopCozinha();
          else startCozinha();
          break;
        case 'toggle_financeiro':
          if (financeiroProcess) stopFinanceiro();
          else startFinanceiro();
          break;
        case 'send_order':
          await sendOrder(data.order);
          sendSystemLog(`Pedido #${data.order.pedido_id} enviado com sucesso!`);
          break;
        default:
          console.log('Ação desconhecida:', data.action);
      }
    } catch (err) {
      console.error('Erro ao processar mensagem WS:', err);
    }
  });
});

// Periodically fetch Queue Metrics from RabbitMQ and broadcast them
let amqpConn = null;
let amqpChannel = null;

async function checkQueueMetrics() {
  try {
    if (!amqpConn) {
      amqpConn = await amqp.connect(RABBITMQ_URL);
      amqpChannel = await amqpConn.createChannel();
      // Declare exchange to make sure it exists
      await amqpChannel.assertExchange('pedidos_exchange', 'fanout', { durable: true });
    }
    
    // We assert the queues (which returns the counts if they already exist, or creates them)
    const cozinhaQueue = await amqpChannel.assertQueue('fila_cozinha', { durable: true });
    const financeiroQueue = await amqpChannel.assertQueue('fila_pagamento', { durable: true });

    broadcast({
      type: 'queue_metrics',
      fila_cozinha: {
        messageCount: cozinhaQueue.messageCount,
        consumerCount: cozinhaQueue.consumerCount
      },
      fila_pagamento: {
        messageCount: financeiroQueue.messageCount,
        consumerCount: financeiroQueue.consumerCount
      }
    });
  } catch (error) {
    // If RabbitMQ is offline or starting up, log it once
    console.error('Erro ao conectar ao RabbitMQ para obter métricas:', error.message);
    amqpConn = null;
    amqpChannel = null;
    broadcast({
      type: 'rabbitmq_status',
      connected: false
    });
    return;
  }

  broadcast({
    type: 'rabbitmq_status',
    connected: true
  });
}

// Start polling RabbitMQ every 1500ms
setInterval(checkQueueMetrics, 1500);

// Auto-start consumers by default on boot for demo ease
setTimeout(() => {
  sendSystemLog('Inicializando consumidores padrão...');
  startCozinha();
  startFinanceiro();
}, 2000);

// API endpoint for submitting order
app.post('/api/pedidos', async (req, res) => {
  try {
    const { item, mesa } = req.body;
    if (!item || !mesa) {
      return res.status(400).json({ error: 'Item e Mesa são obrigatórios' });
    }
    const orderId = Math.floor(100 + Math.random() * 900); // 3-digit order id
    const order = {
      pedido_id: orderId,
      item,
      mesa: parseInt(mesa),
      timestamp: new Date().toISOString()
    };
    await sendOrder(order);
    sendSystemLog(`Pedido #${orderId} recebido via API e enviado à exchange.`);
    res.status(201).json({ success: true, order });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Start express server
server.listen(PORT, () => {
  console.log(`Servidor rodando em http://localhost:${PORT}`);
});
