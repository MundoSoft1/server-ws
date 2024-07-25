import * as amqp from 'amqplib';
import axios from 'axios';

const RABBITMQ_URL = 'amqp://44.223.219.154'; 
const IP_QUEUE_NAME = 'ip.camera';
const ALERT_QUEUE_NAME = 'alerta';

let espIpAddresses: string[] = [];

async function sendAlertToESP32(espIpAddress: string) {
  try {
    const response = await axios.post(`http://${espIpAddress}/alert`);
    console.log('Alerta enviada al ESP32:', response.data);
  } catch (error) {
    console.error('Error al enviar alerta al ESP32:', error);
  }
}

export async function consumeMessages(callback: (message: string) => void) {
  try {
    const connection = await amqp.connect(RABBITMQ_URL);
    console.log('Conectado a RabbitMQ');

    const channel = await connection.createChannel();
    console.log('Canal creado');

    await channel.assertQueue(IP_QUEUE_NAME, { durable: true });
    await channel.assertQueue(ALERT_QUEUE_NAME, { durable: true });

    channel.consume(IP_QUEUE_NAME, (msg) => {
      if (msg !== null) {
        const ipAddress = msg.content.toString();
        console.log('IP de ESP32 recibida:', ipAddress);
        if (!espIpAddresses.includes(ipAddress)) {
          espIpAddresses.push(ipAddress);
        }
        channel.ack(msg);
        callback(`Nueva IP de ESP32: ${ipAddress}`);
      }
    });

    channel.consume(ALERT_QUEUE_NAME, async (msg) => {
      if (msg !== null) {
        const alertMessage = msg.content.toString();
        console.log('Alerta recibida:', alertMessage);

        for (const ipAddress of espIpAddresses) {
          await sendAlertToESP32(ipAddress);
        }

        channel.ack(msg);
        callback(`Alerta enviada a ${espIpAddresses.length} ESP32(s)`);
      }
    });

    console.log('Esperando mensajes de RabbitMQ...');
  } catch (error) {
    console.error('Error:', error);
  }
}