// const AWS = require('aws-sdk');
// const sqs = new AWS.SQS({ region: 'us-east-1' });
// const queueUrl = 'https://sqs.us-east-1.amazonaws.com/272467826288/csv-parse';
// let messageReceiveCount = 0;
// users = [];

// const logMessages = async () => {
// 	const attributes = await sqs.getQueueAttributes({
// 			QueueUrl: queueUrl,
// 			AttributeNames: ['All'],
// 		}).promise();
	
// 	const numberOfMessage = Number(attributes.Attributes.ApproximateNumberOfMessages);
// 	console.log(numberOfMessage);
// 	while(users.length <= numberOfMessage)
// 	{
// 		try {
// 			const data = await sqs.receiveMessage({
// 				QueueUrl: queueUrl,
// 				MaxNumberOfMessages: 10,
// 				VisibilityTimeout: 1,
// 				WaitTimeSeconds: 5,
// 			}).promise();

// 			if (!data.Messages) {
// 				console.log('No messages to log');
// 			}

// 			data.Messages.forEach((message) => {
// 				messageReceiveCount +=1 ;
// 				// console.log(messageReceiveCount);
// 				const body = JSON.parse(message.Body);
// 				const userId = body.user_id;
// 				if(!(userId in users))
// 				{
// 					users.push(userId);
// 				}
// 				else {
// 					console.log('duplicate found');
// 				}
// 				console.log(`Message ${messageReceiveCount} received success: ${userId}`);
// 			});
// 		} catch (err) {
// 			console.error(`Error receiving message: ${err}`);
// 		}
// 	}
// };

// logMessages()
// 	.then(() => console.log('Finished fetching messages', users.length))
// 	.catch((err) => console.error(`Error fetching messages: ${err}`));





const AWS = require('aws-sdk');
const sqs = new AWS.SQS({ region: 'us-east-1' });
const queueUrl = 'https://sqs.us-east-1.amazonaws.com/272467826288/csv-parse';
let messageReceiveCount = 0;
users = [];

handler = async (event) => {
	const attributes = await sqs.getQueueAttributes({
			QueueUrl: queueUrl,
			AttributeNames: ['All'],
		}).promise();

		const numberOfMessage = Number(attributes.Attributes.ApproximateNumberOfMessages);
		
const logMessages = new Promise((resolve,reject) => {
		while(users.length <= numberOfMessage)
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
					// console.log(messageReceiveCount);
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
				reject(err)
			}
		}

		resolve({message: `Finshed fetching messages: ${users.length}`})
}) 
	logMessages()
		.then((msg) => console.log(msg))
		.catch((err) => console.error(`Error fetching messages: ${err}`));
};
	


handler();