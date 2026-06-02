const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const EXCHANGE_NAME = 'pedidos_exchange';

async function sendOrder(order) {
  let connection;
  try {
    connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();
    
    // Declaring a fanout exchange. This ensures messages are routed to all bound queues.
    await channel.assertExchange(EXCHANGE_NAME, 'fanout', { durable: true });
    
    const messageBuffer = Buffer.from(JSON.stringify(order));
    
    // Publish message to the exchange
    channel.publish(EXCHANGE_NAME, '', messageBuffer, { persistent: true });
    console.log(`[Produtor] Pedido enviado para a exchange:`, order);
    
    await channel.close();
  } catch (error) {
    console.error('[Produtor] Erro ao enviar pedido:', error);
    throw error;
  } finally {
    if (connection) {
      await connection.close();
    }
  }
}

// Standalone execution support
if (require.main === module) {
  const args = process.argv.slice(2);
  const orderId = parseInt(args[0]) || Math.floor(Math.random() * 1000);
  const item = args[1] || 'Hambúrguer Duplo';
  const mesa = parseInt(args[2]) || Math.floor(Math.random() * 10) + 1;
  
  const order = { pedido_id: orderId, item, mesa, timestamp: new Date().toISOString() };
  
  sendOrder(order)
    .then(() => process.exit(0))
    .catch((err) => {
      console.error(err);
      process.exit(1);
    });
}

module.exports = { sendOrder };
