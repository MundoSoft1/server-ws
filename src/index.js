const amqp = require('amqplib');
const axios = require('axios');
const FormData = require('form-data');
const { Readable } = require('stream');

const RABBITMQ_URL = 'amqp://44.223.219.154';
const IP_QUEUE_NAME = 'ip.camera';
const ALERT_QUEUE_NAME = 'alerta';  
const API_URL = 'http://localhost:3000/images';

let cameraIP = null;  // Guardar solo la última IP de la cámara

async function getImageFromURL(imageUrl) {
  try {
    const response = await axios({
      url: imageUrl,
      method: 'GET',
      responseType: 'stream',
    });
    return response.data;
  } catch (error) {
    console.error('Error al obtener la imagen:', error);
    throw error;
  }
}

async function sendImageToAPI(apiUrl, imageStream) {
  try {
    const formData = new FormData();
    formData.append('image', imageStream, { filename: 'image.jpg', contentType: 'image/jpeg' });

    const response = await axios.post(apiUrl, formData, {
      headers: {
        ...formData.getHeaders()
      }
    });
    console.log('Imagen enviada a la API:', response.data);
  } catch (error) {
    console.error('Error al enviar imagen a la API:', error);
  }
}

async function consumeMessages(callback) {
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
        console.log('IP de cámara recibida:', ipAddress);
        cameraIP = ipAddress;  // Guardar la última IP recibida
        channel.ack(msg);
        callback(`Nueva IP de cámara: ${ipAddress}`);
      }
    });

    channel.consume(ALERT_QUEUE_NAME, async (msg) => {
      if (msg !== null) {
        const alertMessage = msg.content.toString();
        console.log('Alerta recibida:', alertMessage);

        if (cameraIP) {
          const imageUrl = `http://${cameraIP}/capture?${new Date().getTime()}`;
          try {
            const imageStream = await getImageFromURL(imageUrl);
            await sendImageToAPI(API_URL, imageStream);
          } catch (error) {
            console.error('Error al procesar la imagen:', error);
          }
        } else {
          console.log('No hay IPs disponibles para enviar imágenes.');
        }

        channel.ack(msg);
        callback(`Imagen enviada a la API`);
      }
    });

    console.log('Esperando mensajes de RabbitMQ...');
  } catch (error) {
    console.error('Error:', error);
  }
}

module.exports = { consumeMessages };
