// Websocket connection
let socket;
const wsUrl = `ws://${window.location.host}`;

function connectWS() {
  socket = new WebSocket(wsUrl);

  socket.onopen = () => {
    console.log('Conectado ao servidor WebSocket');
    document.getElementById('status-server').classList.add('online');
  };

  socket.onclose = () => {
    console.log('Conexão WebSocket perdida. Tentando reconectar...');
    document.getElementById('status-server').classList.remove('online');
    document.getElementById('status-rabbitmq').classList.remove('online');
    setTimeout(connectWS, 2000);
  };

  socket.onerror = (error) => {
    console.error('Erro no WebSocket:', error);
  };

  socket.onmessage = (event) => {
    const data = JSON.parse(event.data);
    
    switch (data.type) {
      case 'status':
        updateConsumerUI('cozinha', data.cozinhaRunning);
        updateConsumerUI('financeiro', data.financeiroRunning);
        break;

      case 'rabbitmq_status':
        const rmqBadge = document.getElementById('status-rabbitmq');
        if (data.connected) {
          rmqBadge.classList.add('online');
        } else {
          rmqBadge.classList.remove('online');
        }
        break;

      case 'queue_metrics':
        updateQueueMetrics('cozinha', data.fila_cozinha);
        updateQueueMetrics('financeiro', data.fila_pagamento);
        break;

      case 'cozinha_log':
        appendLog('terminal-cozinha', data.message);
        break;

      case 'financeiro_log':
        appendLog('terminal-financeiro', data.message);
        break;

      case 'system_log':
        appendLog('terminal-sistema', data.message);
        break;
    }
  };
}

// Update consumer card display
function updateConsumerUI(type, isRunning) {
  const card = document.getElementById(`consumer-${type}-card`);
  const btn = document.getElementById(`btn-toggle-${type}`);
  const stateLabel = card.querySelector('.consumer-state');

  if (isRunning) {
    card.classList.add('active');
    btn.textContent = 'Desligar Consumidor';
    stateLabel.textContent = 'Ativo (Consumindo...)';
  } else {
    card.classList.remove('active');
    btn.textContent = 'Ligar Consumidor';
    stateLabel.textContent = 'Desconectado / Inativo';
  }
}

// Update queue numbers
function updateQueueMetrics(type, metrics) {
  const countSpan = document.getElementById(`${type}-queue-count`);
  countSpan.textContent = metrics.messageCount;
  
  if (metrics.messageCount > 0) {
    countSpan.classList.add('has-items');
  } else {
    countSpan.classList.remove('has-items');
  }
}

// Append log to terminal
function appendLog(terminalId, message) {
  const terminal = document.getElementById(terminalId);
  const entry = document.createElement('div');
  entry.className = 'log-entry';
  entry.textContent = message;
  
  terminal.appendChild(entry);
  terminal.scrollTop = terminal.scrollHeight;
  
  // Keep terminal history limited to 100 entries
  while (terminal.childNodes.length > 100) {
    terminal.removeChild(terminal.firstChild);
  }
}

// Setup Event Listeners
document.addEventListener('DOMContentLoaded', () => {
  connectWS();

  // Toggle buttons
  document.getElementById('btn-toggle-cozinha').addEventListener('click', () => {
    socket.send(JSON.stringify({ action: 'toggle_cozinha' }));
  });

  document.getElementById('btn-toggle-financeiro').addEventListener('click', () => {
    socket.send(JSON.stringify({ action: 'toggle_financeiro' }));
  });

  // Clear logs button
  document.getElementById('btn-clear-logs').addEventListener('click', () => {
    document.getElementById('terminal-cozinha').innerHTML = '';
    document.getElementById('terminal-financeiro').innerHTML = '';
    document.getElementById('terminal-sistema').innerHTML = '<div class="log-entry system-msg">[Sistema] Logs limpos.</div>';
  });

  // Submit Order Form
  const form = document.getElementById('order-form');
  form.addEventListener('submit', (e) => {
    e.preventDefault();
    
    const itemSelect = document.getElementById('order-item');
    const tableInput = document.getElementById('order-table');
    
    const order = {
      pedido_id: Math.floor(100 + Math.random() * 900),
      item: itemSelect.value,
      mesa: parseInt(tableInput.value),
      timestamp: new Date().toISOString()
    };

    socket.send(JSON.stringify({
      action: 'send_order',
      order: order
    }));
  });

  // Send 5 Random Orders
  const randomItems = [
    '🍔 Hambúrguer Duplo',
    '🍕 Pizza Margherita',
    'Batata Frita Grande',
    '🥤 Refrigerante Lata',
    'Milkshake de Chocolate',
    '🍛 Executivo de Frango'
  ];

  document.getElementById('btn-send-5').addEventListener('click', () => {
    for (let i = 0; i < 5; i++) {
      setTimeout(() => {
        const orderId = Math.floor(100 + Math.random() * 900);
        const item = randomItems[Math.floor(Math.random() * randomItems.length)];
        const mesa = Math.floor(1 + Math.random() * 20);
        
        const order = {
          pedido_id: orderId,
          item: item,
          mesa: mesa,
          timestamp: new Date().toISOString()
        };

        socket.send(JSON.stringify({
          action: 'send_order',
          order: order
        }));
      }, i * 300); // slight stagger when sending
    }
  });
});
