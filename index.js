const AWS = require('aws-sdk');
const csv = require('csv-parser');
const fs = require('fs');

const sqs = new AWS.SQS({ region: 'us-east-1' });
const queueUrl = 'https://sqs.us-east-1.amazonaws.com/272467826288/csv-parse';

exports.handler = async (event) => {
  const stream = fs.createReadStream('./assets/difference.csv');
  const parserStream = stream.pipe(csv());
  
  const upload = new Promise((resolve, reject) => {
    let messageCount = 0;
    let messages = [];
    
    const sendMessages = async (messageBatch) => {
      const params = {
        Entries: messageBatch,
        QueueUrl: queueUrl,
      };
      
      try {
        await sqs.sendMessageBatch(params).promise();
        console.log(`${messageBatch.length} messages successfully sent to queue`);
      } catch (err) {
        console.log(`Error sending messages to queue: ${err}`);
        reject(err);
      }
    };
    
    parserStream.on('data', async (data) => {
      const message = {
        Id: `${messageCount}`,
        MessageBody: JSON.stringify(data),
      };
      
      messages.push(message);
      messageCount++;
      
      if (messages.length >= 10) {
        const messageBatch = messages.splice(0, 10);
        await sendMessages(messageBatch);
      }
    });
    
    parserStream.on('end', async () => {
      if (messages.length > 0) {
        await sendMessages(messages);
      }
      resolve({ message: `Finished processing CSV at ${new Date().toLocaleString()}` });
    });
    
    parserStream.on('error', (err) => {
      console.log(`Error parsing CSV: ${err}`);
      reject(err);
    });
  });
  
  await upload
    .then((msg) => console.log(msg))
    .catch((err) => console.log(err));

  const logMessages = async () => {

    let messageReceiveCount = 0;
    users = [];

    const attributes = await sqs.getQueueAttributes({
        QueueUrl: queueUrl,
        AttributeNames: ['All'],
      }).promise();
    
    const numberOfMessage = Number(attributes.Attributes.ApproximateNumberOfMessages);
    
    if(users.length !== 0 && users.length % 1000 == 0)
    {
      await sleep(60000);
    }

    while(users.length < numberOfMessage)
    {
      try {
        const data = await sqs.receiveMessage({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: 10,
          VisibilityTimeout: 1,
          WaitTimeSeconds: 5,
        }).promise();
  
        if (!data.Messages) {
          console.log('No messages to log');
        }
  
        data.Messages.forEach((message) => {
          messageReceiveCount +=1 ;
          
          const body = JSON.parse(message.Body);
          const userId = body.user_id;
          
          if(!(userId in users))
          {
            users.push(userId);
          }
          else {
            console.log('duplicate found');
          }
          
          console.log(`Message received success: ${message.Body}`);
        });
      } catch (err) {
        console.error(`Error receiving message: ${err}`);
      }
    }
  };

  const workers = [];
  for (let i = 0; i < 10; i++) {
    workers.push(logMessages());
  }
  
  async function receive(){
    await Promise.all(workers)
  }

  function sleep(ms) {
    return new Promise((resolve) => {
      console.log('pausing for 1 minute')
      setTimeout(resolve, ms);
    });
  }

  await receive()
    .then(() => console.log('Finished fetching messages'))
    .catch((err) => console.error(`Error fetching messages: ${err}`));

};
