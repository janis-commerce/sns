'use strict';

require('lllog')('none');
const sinon = require('sinon');
const assert = require('assert');

// const { v4: uuidv4 } = uuid; // Extrae la función v4
// const uuid = require('uuid');
const { mockClient } = require('aws-sdk-client-mock');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { SNSClient, PublishCommand, PublishBatchCommand } = require('@aws-sdk/client-sns');
const { SSMClient, GetParameterCommand } = require('@aws-sdk/client-ssm');
const { RAMClient, ListResourcesCommand } = require('@aws-sdk/client-ram');
const { STSClient, AssumeRoleCommand } = require('@aws-sdk/client-sts');

const { SnsTrigger } = require('../lib');

describe('SnsTrigger', () => {

	let snsMock;
	let ssmMock;
	let ramMock;
	let s3Mock;
	let stsMock;
	let clock;

	const parameterName = 'shared/internal-storage';

	const sampleTopicArn = 'arn:aws:sns:us-east-1:123456789012:MyTopic';

	const parameterNameStoreArn = `arn:aws:ssm:us-east-1:12345678:parameter/${parameterName}`;

	// const s3ContentPath = 'topics/defaultClient/service-name/MyTopic/2025/03/06/3e3c0305-4508-422e-bc69-7027dfaaa8be.json';

	const defaultSnsMessageAttributes = {
		topicName: {
			DataType: 'String',
			StringValue: 'MyTopic'
		}
	};

	const credentials = {
		AccessKeyId: 'accessKeyIdTest',
		SecretAccessKey: 'secretAccessKeyTest',
		SessionToken: 'sessionTokenTest'
	};

	const buckets = [
		{
			bucketName: 'sample-bucket-name-us-east-1',
			roleArn: 'arn:aws:iam::1234567890:role/defaultRoleName',
			region: 'us-east-1',
			default: true
		},
		{
			bucketName: 'sample-bucket-name-us-west-1',
			roleArn: 'arn:aws:iam::1234567890:role/defaultRoleName',
			region: 'us-west-1'
		}
	];

	const publishBatchRequestEntries = [
		{
			Id: '1',
			Message: JSON.stringify({ foo: 'bar' }),
			MessageAttributes: {
				topicName: {
					DataType: 'String',
					StringValue: 'MyTopic'
				}
			}
		},
		{
			Id: '2',
			Message: JSON.stringify({ foo: 'baz' }),
			MessageAttributes: {
				topicName: {
					DataType: 'String',
					StringValue: 'MyTopic'
				}
			}
		}
	];

	beforeEach(() => {
		const fakeDate = new Date(2025, 2, 6);
		clock = sinon.useFakeTimers(fakeDate.getTime());
	});

	afterEach(() => {
		snsMock.restore();
		ssmMock.restore();
		ramMock.restore();
		s3Mock.restore();
		stsMock.restore();
		clock.restore();
	});

	beforeEach(() => {
		ssmMock = mockClient(SSMClient);
		snsMock = mockClient(SNSClient);
		ramMock = mockClient(RAMClient);
		s3Mock = mockClient(S3Client);
		stsMock = mockClient(STSClient);
		process.env.JANIS_SERVICE_NAME = 'service-name';
	});

	describe('publishEvent', () => {

		const singleEventResponse = {
			messageId: '4ac0a219-1122-33b3-4445-5556666d734d'
		};

		const singleEventFifoResponse = {
			messageId: '4ac0a219-1122-33b3-4445-5556666d734d',
			sequenceNumber: '222222222222222222222222'
		};

		it('Should publish a single event with content only as minimal requirement (Standard Topic)', async () => {

			snsMock.on(PublishCommand).resolves({
				MessageId: singleEventResponse.messageId
			});

			const snsTrigger = new SnsTrigger();
			const result = await snsTrigger.publishEvent(sampleTopicArn, {
				content: { foo: 'bar' }
			});

			assert.deepStrictEqual(result, singleEventResponse);
			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand, {
				TopicArn: sampleTopicArn,
				Message: JSON.stringify({
					foo: 'bar'
				}),
				MessageAttributes: defaultSnsMessageAttributes
			}, true).length, 1);
		});

		it('Should publish a single event with content only as minimal requirement (FIFO Topic)', async () => {

			snsMock.on(PublishCommand).resolves({
				MessageId: singleEventFifoResponse.messageId,
				SequenceNumber: singleEventFifoResponse.sequenceNumber
			});

			const snsTrigger = new SnsTrigger();
			const result = await snsTrigger.publishEvent(sampleTopicArn, {
				content: {
					foo: 'bar'
				}
			});

			assert.deepStrictEqual(result, singleEventFifoResponse);

			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand, {
				TopicArn: sampleTopicArn,
				Message: JSON.stringify({
					foo: 'bar'
				}),
				MessageAttributes: defaultSnsMessageAttributes
			}, true).length, 1);
		});

		it('Should publish a single event with all available properties (Standard Topic)', async () => {

			snsMock.on(PublishCommand).resolves({
				MessageId: singleEventResponse.messageId
			});

			const snsTrigger = new SnsTrigger();
			const result = await snsTrigger.publishEvent(sampleTopicArn, {
				content: { foo: 'bar' },
				attributes: { foo: 'bar' },
				subject: 'test'
			});

			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand, {
				TopicArn: sampleTopicArn,
				Message: JSON.stringify({
					foo: 'bar'
				}),
				MessageAttributes: {
					...defaultSnsMessageAttributes,
					foo: {
						DataType: 'String',
						StringValue: 'bar'
					}
				},
				Subject: 'test'
			}, true).length, 1);

			assert.deepStrictEqual(result, singleEventResponse);
		});

		it('Should publish a single event with all available properties (FIFO Topic)', async () => {

			snsMock.on(PublishCommand).resolves({
				MessageId: singleEventResponse.messageId
			});

			const snsTrigger = new SnsTrigger();
			const result = await snsTrigger.publishEvent(sampleTopicArn, {
				content: { foo: 'bar' },
				attributes: { foo: 'bar' },
				subject: 'test',
				messageGroupId: 'group1',
				messageDeduplicationId: 'dedup1',
				messageStructure: 'json'
			});

			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand, {
				TopicArn: sampleTopicArn,
				Message: JSON.stringify({
					foo: 'bar'
				}),
				MessageAttributes: {
					...defaultSnsMessageAttributes,
					foo: {
						DataType: 'String',
						StringValue: 'bar'
					}
				},
				Subject: 'test',
				MessageGroupId: 'group1',
				MessageDeduplicationId: 'dedup1',
				MessageStructure: 'json'
			}, true).length, 1);

			assert.deepStrictEqual(result, singleEventResponse);
		});

		it('Should add janis-client message attribute if session with clientCode is set', async () => {

			snsMock.on(PublishCommand).resolves({
				MessageId: singleEventResponse.messageId
			});

			const snsTrigger = new SnsTrigger();
			snsTrigger.session = {
				clientCode: 'test'
			};
			const result = await snsTrigger.publishEvent(sampleTopicArn, {
				content: { foo: 'bar' },
				attributes: {
					foo: 'bar',
					arrayAttribute: ['option1', 'option2']
				},
				subject: 'test',
				messageGroupId: 'group1',
				messageDeduplicationId: 'dedup1',
				messageStructure: 'json'
			});

			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand, {
				TopicArn: sampleTopicArn,
				Message: JSON.stringify({
					foo: 'bar'
				}),
				MessageAttributes: {
					...defaultSnsMessageAttributes,
					'janis-client': {
						DataType: 'String',
						StringValue: 'test'
					},
					foo: {
						DataType: 'String',
						StringValue: 'bar'
					},
					arrayAttribute: {
						DataType: 'String.Array',
						StringValue: JSON.stringify(['option1', 'option2'])
					}
				},
				Subject: 'test',
				MessageGroupId: 'group1',
				MessageDeduplicationId: 'dedup1',
				MessageStructure: 'json'
			}, true).length, 1);

			assert.deepStrictEqual(result, singleEventResponse);
		});

	});

	describe('publishEvents', () => {

		const multiEventResponse = {
			successCount: 1,
			failedCount: 1,
			outputs: [
				{
					success: true,
					messageId: '4ac0a219-1122-33b3-4445-5556666d734d'
				},
				{
					success: false,
					errorCode: 'SQS001',
					errorMessage: 'SQS Failed'
				}
			]
		};

		const multiEventFifoResponse = {
			successCount: 1,
			failedCount: 1,
			outputs: [
				{
					success: true,
					messageId: '4ac0a219-1122-33b3-4445-5556666d734d',
					sequenceNumber: '222222222222222222222222'
				},
				{
					success: false,
					errorCode: 'SQS001',
					errorMessage: 'SQS Failed'
				}
			]
		};

		it('Should publish multiple events with content only as minimal requirement (Standard Topic)', async () => {

			snsMock.on(PublishBatchCommand).resolves({
				Successful: [
					{ MessageId: '4ac0a219-1122-33b3-4445-5556666d734d' }
				],
				Failed: [
					{
						Code: 'SQS001',
						Message: 'SQS Failed'
					}
				]
			});

			const snsTrigger = new SnsTrigger();
			const result = await snsTrigger.publishEvents(sampleTopicArn, [
				{
					content: { foo: 'bar' }
				},
				{
					content: { foo: 'baz' }
				}
			]);

			assert.deepStrictEqual(result, multiEventResponse);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand, {
				TopicArn: sampleTopicArn,
				PublishBatchRequestEntries: publishBatchRequestEntries
			}, true).length, 1);
		});

		it('Should publish multiple events with content only as minimal requirement (FIFO Topic)', async () => {

			snsMock.on(PublishBatchCommand).resolves({
				Successful: [
					{
						MessageId: '4ac0a219-1122-33b3-4445-5556666d734d',
						SequenceNumber: '222222222222222222222222'
					}
				],
				Failed: [
					{
						Code: 'SQS001',
						Message: 'SQS Failed'
					}
				]
			});

			const snsTrigger = new SnsTrigger();
			const result = await snsTrigger.publishEvents(sampleTopicArn, [
				{
					content: { foo: 'bar' }
				},
				{
					content: { foo: 'baz' }
				}
			]);

			assert.deepStrictEqual(result, multiEventFifoResponse);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand, {
				TopicArn: sampleTopicArn,
				PublishBatchRequestEntries: [
					{
						Id: '1',
						Message: JSON.stringify({ foo: 'bar' }),
						MessageAttributes: {
							topicName: {
								DataType: 'String',
								StringValue: 'MyTopic'
							}
						}
					},
					{
						Id: '2',
						Message: JSON.stringify({ foo: 'baz' }),
						MessageAttributes: {
							topicName: {
								DataType: 'String',
								StringValue: 'MyTopic'
							}
						}
					}
				]
			}, true).length, 1);
		});

		// it('Should process events that are greater than 256KB', async () => {

		// 	snsMock.on(PublishBatchCommand).resolves({
		// 		Successful: [
		// 			{ MessageId: '4ac0a219-1122-33b3-4445-5556666d734d' },
		// 			{ MessageId: '4ac0a219-1122-33b3-4445-5556666d735d' }
		// 		]
		// 	});

		// 	ramMock.on(ListResourcesCommand).resolves({
		// 		resources: [{ arn: parameterNameStoreArn }]
		// 	});

		// 	ssmMock.on(GetParameterCommand).resolves({
		// 		Parameter: {
		// 			Value: JSON.stringify(buckets)
		// 		}
		// 	});

		// 	stsMock.on(AssumeRoleCommand).resolves({
		// 		Credentials: {
		// 			AccessKeyId: 'accessKeyIdTest',
		// 			SecretAccessKey: 'secretAccessKeyTest',
		// 			SessionToken: 'sessionTokenTest'
		// 		}
		// 	});

		// 	s3Mock.on(PutObjectCommand).resolves({
		// 		ETag: '5d41402abc4b2a76b9719d911017c590'
		// 	});

		// 	const partiallySentResponse = {
		// 		successCount: 2,
		// 		failedCount: 0,
		// 		outputs: [
		// 			{ success: true, messageId: '4ac0a219-1122-33b3-4445-5556666d734d' },
		// 			{ success: true, messageId: '4ac0a219-1122-33b3-4445-5556666d735d' }
		// 		]
		// 	};

		// 	const snsTrigger = new SnsTrigger();

		// 	snsTrigger.session = { clientCode: 'defaultClient' };

		// 	const result = await snsTrigger.publishEvents(sampleTopicArn, [
		// 		{
		// 			payloadFixedProperties: ['bar'],
		// 			content: { foo: 'bar' }
		// 		},
		// 		{
		// 			payloadFixedProperties: ['bar'],
		// 			content: {
		// 				bar: 'bar',
		// 				foo: 'x'.repeat(256 * 1024)
		// 			}
		// 		}
		// 	]);

		// 	assert.deepStrictEqual(result, partiallySentResponse);
		// 	assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 1);
		// 	assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
		// 	assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
		// 	assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 1);
		// 	assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 1);

		// 	assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand, {
		// 		resourceOwner: 'OTHER-ACCOUNTS'
		// 	}, true).length, 1);

		// 	assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand, {
		// 		Name: parameterNameStoreArn,
		// 		WithDecryption: true
		// 	}, true).length, 1);

		// 	assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand, {
		// 		RoleArn: buckets[0].roleArn,
		// 		RoleSessionName: 'service-name',
		// 		DurationSeconds: 1800
		// 	}, true).length, 1);

		// 	// assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand, {
		// 	// 	Bucket: buckets[0].bucketName,
		// 	// 	Key: s3ContentPath,
		// 	// 	Body: JSON.stringify({
		// 	// 		bar: 'bar',
		// 	// 		foo: 'x'.repeat(256 * 1024)
		// 	// 	})
		// 	// }, true).length, 1);

		// });

		it('Should save the event content to S3 if it is greater than 256KB', async () => {

			const partiallySentResponse = {
				successCount: 1,
				failedCount: 0,
				outputs: [
					{ success: true, messageId: '4ac0a219-1122-33b3-4445-5556666d734d' }
				]
			};

			snsMock.on(PublishBatchCommand).resolves({
				Successful: [
					{ MessageId: '4ac0a219-1122-33b3-4445-5556666d734d' }
				]
			});

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			stsMock.on(AssumeRoleCommand).resolves({
				Credentials: credentials
			});

			s3Mock.on(PutObjectCommand).resolves({
				ETag: '5d41402abc4b2a76b9719d911017c590'
			});

			const snsTrigger = new SnsTrigger();

			snsTrigger.session = { clientCode: 'defaultClient' };

			const result = await snsTrigger.publishEvents(sampleTopicArn, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]);

			assert.deepStrictEqual(result, partiallySentResponse);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 1);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 1);
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 1);

			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand, {
				resourceOwner: 'OTHER-ACCOUNTS'
			}, true).length, 1);

			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand, {
				Name: parameterNameStoreArn,
				WithDecryption: true
			}, true).length, 1);

			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand, {
				RoleArn: buckets[0].roleArn,
				RoleSessionName: 'service-name',
				DurationSeconds: 1800
			}, true).length, 1);
		});

		it('Should split events in batches not greater than 256KB', async () => {

			snsMock.on(PublishBatchCommand)
				.resolvesOnce({
					Successful: [
						{ MessageId: '4ac0a219-1122-33b3-4445-5556666d734d' }
					]
				})
				.resolvesOnce({
					Failed: [{
						Code: 'SQS001',
						Message: 'SQS Failed'
					}]
				});

			const snsTrigger = new SnsTrigger();
			const result = await snsTrigger.publishEvents(sampleTopicArn, [
				{
					content: {
						foo: 'x'.repeat(150 * 1024)
					}
				},
				{
					content: {
						foo: 'y'.repeat(150 * 1024)
					}
				}
			]);

			assert.deepStrictEqual(result, multiEventResponse);

			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 2);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand, {
				TopicArn: sampleTopicArn,
				PublishBatchRequestEntries: [
					{
						Id: '1',
						Message: JSON.stringify({
							foo: 'x'.repeat(150 * 1024)
						}),
						MessageAttributes: defaultSnsMessageAttributes
					}
				]
			}, true).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand, {
				TopicArn: sampleTopicArn,
				PublishBatchRequestEntries: [
					{
						Id: '2',
						Message: JSON.stringify({
							foo: 'y'.repeat(150 * 1024)
						}),
						MessageAttributes: defaultSnsMessageAttributes

					}
				]
			}, true).length, 1);
		});

		it('Should split events in batches not greater than 10 entries', async () => {

			snsMock.on(PublishBatchCommand)
				.resolvesOnce({
					Successful: [
						{ MessageId: 'msg-1' },
						{ MessageId: 'msg-2' }
					]
				})
				.resolvesOnce({
					Failed: [{
						Code: 'SQS001',
						Message: 'SQS Failed'
					}]
				});

			// should have 2 batches, first with 10 and the second with 5
			const events = Array.from({ length: 15 }, (_, index) => ({
				content: { message: `Event ${index + 1}` }
			}));

			const snsTrigger = new SnsTrigger();
			const result = await snsTrigger.publishEvents(sampleTopicArn, events);

			assert.deepStrictEqual(result.successCount, 2);
			assert.deepStrictEqual(result.failedCount, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 2);

			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand, {
				TopicArn: sampleTopicArn,
				PublishBatchRequestEntries: Array.from({ length: 10 }, (_, index) => ({
					Id: `${index + 1}`,
					Message: JSON.stringify({ message: `Event ${index + 1}` }),
					MessageAttributes: defaultSnsMessageAttributes

				}))
			}, true).length, 1);

			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand, {
				TopicArn: sampleTopicArn,
				PublishBatchRequestEntries: Array.from({ length: 5 }, (_, index) => ({
					Id: `${index + 11}`,
					Message: JSON.stringify({ message: `Event ${index + 11}` }),
					MessageAttributes: defaultSnsMessageAttributes
				}))
			}, true).length, 1);

		});

	});

});
