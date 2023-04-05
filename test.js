// const AWS = require('aws-sdk');
// const csv = require('csv-parser');
// const fs = require('fs');

// const sqs = new AWS.SQS({ region: 'us-east-1' });
// const queueUrl = 'https://sqs.us-east-1.amazonaws.com/272467826288/csv-parse';

// handler = async (event) => {
//   const stream = fs.createReadStream('./assets/difference2.csv');
//   const parserStream = stream.pipe(csv());
  
//   const upload = new Promise((resolve, reject) => {
//     let messageCount = 0;
//     parserStream.on('data', async (data) => {
//       const params = {
//         MessageBody: JSON.stringify(data),
//         QueueUrl: queueUrl
//       };
      
//       try {
//         await sqs.sendMessage(params).promise();
//         messageCount++;
//         console.log(`Row ${messageCount} successfully sent to queue`);
//       } catch (err) {
//         console.log(`Error sending message to queue: ${err}`);
//         reject(err);
//       }
//     });
    
//     parserStream.on('end', () => {
//       resolve({ message: `Finished processing CSV at ${new Date().toLocaleString()}` });
//     });
    
//     parserStream.on('error', (err) => {
//       console.log(`Error parsing CSV: ${err}`);
//       reject(err);
//     });
//   });
  
//   const result = await upload;
//   return result;

// };

// handler();





//sending through batches


// const AWS = require('aws-sdk');
// const csv = require('csv-parser');
// const fs = require('fs');

// const sqs = new AWS.SQS({ region: 'us-east-1' });
// const queueUrl = 'https://sqs.us-east-1.amazonaws.com/272467826288/csv-parse';

// handler = async (event) => {
//   const stream = fs.createReadStream('./assets/difference2.csv');
//   const parserStream = stream.pipe(csv());
  
//   const upload = new Promise((resolve, reject) => {
//     let messageCount = 0;
//     let messages = [];
    
//     const sendMessages = async (messageBatch) => {
//       const params = {
//         Entries: messageBatch,
//         QueueUrl: queueUrl,
//       };
      
//       try {
//         await sqs.sendMessageBatch(params).promise();
//         console.log(`${messageBatch.length} messages successfully sent to queue`);
//       } catch (err) {
//         console.log(`Error sending messages to queue: ${err}`);
//         reject(err);
//       }
//     };
    
//     parserStream.on('data', async (data) => {
//       const message = {
//         Id: `${messageCount}`,
//         MessageBody: JSON.stringify(data),
//       };
      
//       messages.push(message);
//       messageCount++;
      
//       if (messages.length >= 10) {
//         const messageBatch = messages.splice(0, 10);
//         // console.log(messageBatch);
//         await sendMessages(messageBatch);
//       }
//     });
    
//     parserStream.on('end', async () => {
//       if (messages.length > 0) {
//         await sendMessages(messages);
//       }
//       resolve({ message: `Finished processing CSV at ${new Date().toLocaleString()}` });
//     });
    
//     parserStream.on('error', (err) => {
//       console.log(`Error parsing CSV: ${err}`);
//       reject(err);
//     });
//   });
  
//   const result = await upload;
//   return result;
// };

// handler();







//sending and receiving the data

const AWS = require('aws-sdk');
const csv = require('csv-parser');
const fs = require('fs');

const sqs = new AWS.SQS({ region: 'us-east-1' });
const queueUrl = 'https://sqs.us-east-1.amazonaws.com/272467826288/csv-parse';

handler = async (event) => {
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
          
          console.log(`Message ${messageReceiveCount} received success: ${userId}`);
        });
      } catch (err) {
        console.error(`Error receiving message: ${err}`);
      }
    }
  };

  await logMessages()
    .then(() => console.log('Finished fetching messages', users.length))
  	.catch((err) => console.error(`Error fetching messages: ${err}`));

};

handler();