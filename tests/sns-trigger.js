'use strict';

require('lllog')('none');
const sinon = require('sinon');
const assert = require('assert');

const { mockClient } = require('aws-sdk-client-mock');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { SNSClient, PublishCommand, PublishBatchCommand } = require('@aws-sdk/client-sns');
const { SSMClient, GetParameterCommand } = require('@aws-sdk/client-ssm');
const { RAMClient, ListResourcesCommand } = require('@aws-sdk/client-ram');

const { SnsTrigger } = require('../lib');
const ParameterStore = require('../lib/helpers/parameter-store');

describe('SnsTrigger', () => {

	let snsMock;
	let ssmMock;
	let ramMock;
	let s3Mock;
	let clock;

	const fakeDate = new Date(2025, 2, 6);
	const randomId = 'fake-id';
	const parameterName = 'shared/internal-storage';
	const sampleTopicArn = 'arn:aws:sns:us-east-1:123456789012:MyTopic';
	const sampleTopicFifo = `${sampleTopicArn}.fifo`;
	const parameterNameStoreArn = `arn:aws:ssm:us-east-1:12345678:parameter/${parameterName}`;
	const contentS3Path = `topics/defaultClient/service-name/MyTopic/2025/03/06/${randomId}.json`;

	const buckets = [
		{
			bucketName: 'sample-bucket-name-us-east-1',
			region: 'us-east-1',
			default: true
		},
		{
			bucketName: 'sample-bucket-name-us-west-1',
			region: 'us-west-1'
		}
	];

	const stubRandomId = () => {
		sinon.stub(this.snsTrigger, 'randomId').get(() => randomId);
	};

	const assertRamListResourceCommand = () => {
		assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand, {
			resourceOwner: 'OTHER-ACCOUNTS'
		}, true).length, 1);
	};

	const assertSsmGetParameterCommand = () => {
		assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand, {
			Name: parameterNameStoreArn,
			WithDecryption: true
		}, true).length, 1);
	};

	const assertS3PutObjectCommand = (body, bucketName = buckets[0].bucketName, key = contentS3Path) => {
		assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand, {
			Bucket: bucketName,
			Key: key,
			Body: JSON.stringify(body)
		}, true).length, 1);
	};

	beforeEach(() => {
		ssmMock = mockClient(SSMClient);
		snsMock = mockClient(SNSClient);
		ramMock = mockClient(RAMClient);
		s3Mock = mockClient(S3Client);
		clock = sinon.useFakeTimers(fakeDate.getTime());

		this.snsTrigger = new SnsTrigger();
		this.snsTrigger.session = { clientCode: 'defaultClient' };
		stubRandomId();

		process.env.JANIS_SERVICE_NAME = 'service-name';

	});

	afterEach(() => {
		snsMock.restore();
		ssmMock.restore();
		ramMock.restore();
		s3Mock.restore();
		clock.restore();
	});

	describe('publishEvent', () => {

		beforeEach(() => {
			ssmMock = mockClient(SSMClient);
			snsMock = mockClient(SNSClient);
			ramMock = mockClient(RAMClient);
			s3Mock = mockClient(S3Client);
			process.env.JANIS_SERVICE_NAME = 'service-name';
		});

		afterEach(() => {
			sinon.restore();
			snsMock.restore();
			ssmMock.restore();
			ramMock.restore();
			s3Mock.restore();
			ParameterStore.clearCache();
		});

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

			const result = await this.snsTrigger.publishEvent(sampleTopicArn, {
				content: { foo: 'bar' }
			});

			assert.deepStrictEqual(result, singleEventResponse);
			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand, {
				TopicArn: sampleTopicArn,
				Message: JSON.stringify({
					foo: 'bar'
				}),
				MessageAttributes: {
					'janis-client': {
						DataType: 'String',
						StringValue: 'defaultClient'
					},
					topicName: {
						DataType: 'String',
						StringValue: 'MyTopic'
					}
				}
			}, true).length, 1);
		});

		it('Should publish a single event with content only as minimal requirement (FIFO Topic)', async () => {

			snsMock.on(PublishCommand).resolves({
				MessageId: singleEventFifoResponse.messageId,
				SequenceNumber: singleEventFifoResponse.sequenceNumber
			});

			const result = await this.snsTrigger.publishEvent(sampleTopicFifo, {
				content: { foo: 'bar' }
			});

			assert.deepStrictEqual(result, singleEventFifoResponse);
			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand, {
				TopicArn: sampleTopicFifo,
				Message: JSON.stringify({ foo: 'bar' }),
				MessageAttributes: {
					'janis-client': {
						DataType: 'String',
						StringValue: 'defaultClient'
					},
					topicName: {
						DataType: 'String',
						StringValue: 'MyTopic'
					}
				}
			}, true).length, 1);
		});

		it('Should publish a single event with all available properties (Standard Topic)', async () => {

			snsMock.on(PublishCommand).resolves({
				MessageId: singleEventResponse.messageId
			});

			const result = await this.snsTrigger.publishEvent(sampleTopicArn, {
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
					'janis-client': {
						DataType: 'String',
						StringValue: 'defaultClient'
					},
					topicName: {
						DataType: 'String',
						StringValue: 'MyTopic'
					},
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

			const result = await this.snsTrigger.publishEvent(sampleTopicFifo, {
				content: { foo: 'bar' },
				attributes: { foo: 'bar' },
				subject: 'test',
				messageGroupId: 'group1',
				messageDeduplicationId: 'dedup1',
				messageStructure: 'json'
			});

			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand, {
				TopicArn: sampleTopicFifo,
				Message: JSON.stringify({
					foo: 'bar'
				}),
				MessageAttributes: {
					'janis-client': {
						DataType: 'String',
						StringValue: 'defaultClient'
					},
					topicName: {
						DataType: 'String',
						StringValue: 'MyTopic'
					},
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

		it('Should publish a single event with s3 content path if it is greater than 256KB (FIFO SNS)', async () => {

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			s3Mock.on(PutObjectCommand).resolves({
				ETag: '5d41402abc4b2a76b9719d911017c590'
			});

			snsMock.on(PublishCommand).resolves({
				MessageId: singleEventFifoResponse.messageId,
				SequenceNumber: singleEventFifoResponse.sequenceNumber
			});

			const result = await this.snsTrigger.publishEvent(sampleTopicFifo, {
				content: {
					bar: 'bar',
					foo: 'x'.repeat(256 * 1024)
				},
				payloadFixedProperties: ['bar']
			});

			assert.deepStrictEqual(result, singleEventFifoResponse);
			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand).length, 1);

			assertS3PutObjectCommand({
				bar: 'bar',
				foo: 'x'.repeat(256 * 1024)
			});

			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand, {
				TopicArn: sampleTopicFifo,
				Message: JSON.stringify({
					contentS3Location: {
						path: contentS3Path,
						bucketName: buckets[0].bucketName,
						region: buckets[0].region
					},
					bar: 'bar'
				}),
				MessageAttributes: {
					'janis-client': {
						DataType: 'String',
						StringValue: 'defaultClient'
					},
					topicName: {
						DataType: 'String',
						StringValue: 'MyTopic'
					}
				}
			}, true).length, 1);
		});

		it('Should add janis-client message attribute if session with clientCode is set', async () => {

			snsMock.on(PublishCommand).resolves({
				MessageId: singleEventResponse.messageId
			});

			this.snsTrigger.session = {
				clientCode: 'test'
			};
			const result = await this.snsTrigger.publishEvent(sampleTopicArn, {
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
					'janis-client': {
						DataType: 'String',
						StringValue: 'test'
					},
					topicName: {
						DataType: 'String',
						StringValue: 'MyTopic'
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

		it('Should emit event with s3 content path if it is greater than 256KB and the session is missing', async () => {

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			s3Mock.on(PutObjectCommand).resolves({
				ETag: '5d41402abc4b2a76b9719d911017c590'
			});

			snsMock.on(PublishCommand).resolves({
				MessageId: '4ac0a219-1122-33b3-4445-5556666d734d'
			});

			const partiallySentResponse = {
				messageId: '4ac0a219-1122-33b3-4445-5556666d734d'
			};

			const content = {
				bar: 'bar',
				foo: 'x'.repeat(256 * 1024)
			};

			const customContentS3Path = `topics/core/service-name/MyTopic/2025/03/06/${randomId}.json`;

			this.snsTrigger.session = null;

			const result = await this.snsTrigger.publishEvent(sampleTopicArn, {
				payloadFixedProperties: ['bar'],
				content
			});

			assert.deepStrictEqual(result, partiallySentResponse);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand).length, 1);

			assertRamListResourceCommand();
			assertSsmGetParameterCommand();
			assertS3PutObjectCommand(content, buckets[0].bucketName, customContentS3Path);
			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand, {
				TopicArn: sampleTopicArn,
				MessageAttributes: { topicName: { DataType: 'String', StringValue: 'MyTopic' } },
				Message: JSON.stringify({
					contentS3Location: {
						path: customContentS3Path,
						bucketName: buckets[0].bucketName,
						region: buckets[0].region
					},
					bar: 'bar'
				})
			}, true).length, 1);
		});

	});

	describe('publishEvents', () => {

		afterEach(() => {
			sinon.restore();
			ParameterStore.clearCache();
		});

		it('Should publish multiple events with content only as minimal requirement (Standard Topic)', async () => {

			snsMock.on(PublishBatchCommand).resolves({
				Successful: [
					{ Id: '1', MessageId: '4ac0a219-1122-33b3-4445-5556666d734d' }
				],
				Failed: [
					{ Id: '2', Code: 'SNS001', Message: 'SNS Failed' }
				]
			});

			const result = await this.snsTrigger.publishEvents(sampleTopicArn, [
				{
					content: { foo: 'bar' }
				},
				{
					content: { foo: 'baz' }
				}
			]);

			assert.deepStrictEqual(result, {
				successCount: 1,
				failedCount: 1,
				success: [
					{
						Id: '1',
						messageId: '4ac0a219-1122-33b3-4445-5556666d734d'
					}
				],
				failed: [
					{
						Id: '2',
						Code: 'SNS001',
						Message: 'SNS Failed'
					}
				]
			});

			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand, {
				TopicArn: sampleTopicArn,
				PublishBatchRequestEntries: [
					{
						Id: '1',
						Message: JSON.stringify({ foo: 'bar' }),
						MessageAttributes: {
							'janis-client': {
								DataType: 'String',
								StringValue: 'defaultClient'
							},
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
							'janis-client': {
								DataType: 'String',
								StringValue: 'defaultClient'
							},
							topicName: {
								DataType: 'String',
								StringValue: 'MyTopic'
							}
						}
					}
				]
			}, true).length, 1);
		});

		it('Should publish multiple events with content only as minimal requirement (FIFO Topic)', async () => {

			snsMock.on(PublishBatchCommand).resolves({
				Successful: [
					{
						Id: '1',
						MessageId: '4ac0a219-1122-33b3-4445-5556666d734d',
						SequenceNumber: '222222222222222222222222'
					}
				],
				Failed: [
					{
						Id: '2',
						Code: 'SNS001',
						Message: 'SNS Failed'
					}
				]
			});

			const result = await this.snsTrigger.publishEvents(sampleTopicArn, [
				{
					content: { foo: 'bar' }
				},
				{
					content: { foo: 'baz' }
				}
			]);

			assert.deepStrictEqual(result, {
				successCount: 1,
				failedCount: 1,
				success: [
					{
						Id: '1',
						messageId: '4ac0a219-1122-33b3-4445-5556666d734d',
						sequenceNumber: '222222222222222222222222'
					}
				],
				failed: [
					{
						Id: '2',
						Code: 'SNS001',
						Message: 'SNS Failed'
					}
				]
			});
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand, {
				TopicArn: sampleTopicArn,
				PublishBatchRequestEntries: [
					{
						Id: '1',
						Message: JSON.stringify({ foo: 'bar' }),
						MessageAttributes: {
							'janis-client': {
								DataType: 'String',
								StringValue: 'defaultClient'
							},
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
							'janis-client': {
								DataType: 'String',
								StringValue: 'defaultClient'
							},
							topicName: {
								DataType: 'String',
								StringValue: 'MyTopic'
							}
						}
					}
				]
			}, true).length, 1);
		});

		it('Should reject if fails retrieve parameter from ssm parameter store', async () => {

			s3Mock.on(PutObjectCommand);
			snsMock.on(PublishBatchCommand);

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).rejects(new Error('SSM Internal Error'));

			const result = await assert.rejects(this.snsTrigger.publishEvents(sampleTopicArn, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]), { message: `Unable to get parameter with arn ${parameterNameStoreArn} - SSM Internal Error` });

			assert.deepStrictEqual(result, undefined);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 0);

			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand, {
				resourceOwner: 'OTHER-ACCOUNTS'
			}, true).length, 1);

			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand, {
				Name: parameterNameStoreArn,
				WithDecryption: true
			}, true).length, 1);

			assert.deepEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);

		});

		it('Should not reject if cannot upload a payload to provided S3 buckets', async () => {

			snsMock.on(PublishBatchCommand);

			s3Mock.on(PutObjectCommand)
				.rejectsOnce(new Error('Error fetching S3'))
				.rejectsOnce(new Error('Error fetching S3'));

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			const expectedResult = {
				successCount: 0,
				failedCount: 1,
				success: [],
				failed: [
					{
						Id: '1',
						Code: 'S3_ERROR',
						Message: 'Failed to upload to all provided s3 buckets'
					}
				]
			};

			const result = await this.snsTrigger.publishEvents(sampleTopicArn, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]);

			assert.deepStrictEqual(result, expectedResult);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 2);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 0);
		});

		it('Should reject if fails retrieve parameter name from ram resources', async () => {

			s3Mock.on(PutObjectCommand);
			snsMock.on(PublishBatchCommand);
			ssmMock.on(GetParameterCommand);
			ramMock.on(ListResourcesCommand).rejects(new Error('RAM Internal Error'));

			const result = await assert.rejects(this.snsTrigger.publishEvents(sampleTopicArn, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]), { message: 'Resource Access Manager Error: RAM Internal Error' });

			assert.deepStrictEqual(result, undefined);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 0);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 0);
			assertRamListResourceCommand();
		});

		it('Should reject if cannot find resources with the parameter name in the ARN', async () => {

			s3Mock.on(PutObjectCommand);
			snsMock.on(PublishBatchCommand);
			ssmMock.on(GetParameterCommand);

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: 'other-arn-without-the-parameter-name' }]
			});

			const result = await assert.rejects(this.snsTrigger.publishEvents(sampleTopicArn, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]), { message: `Resource Access Manager Error: Unable to find resources with parameter /${parameterName} in the ARN` });

			assert.deepStrictEqual(result, undefined);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 0);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 0);
			assert.deepEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assertRamListResourceCommand();

		});

		it('Should upload a payload to the provisional S3 bucket if the default bucket upload fails', async () => {

			const content = {
				bar: 'bar',
				foo: 'x'.repeat(256 * 1024)
			};

			const partiallySentResponse = {
				successCount: 1,
				failedCount: 0,
				success: [
					{
						Id: '1',
						messageId: '4ac0a219-1122-33b3-4445-5556666d734d'
					}
				],
				failed: []
			};

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			s3Mock.on(PutObjectCommand)
				.rejectsOnce(new Error('Error fetching S3'))
				.resolvesOnce({
					ETag: '5d41402abc4b2a76b9719d911017c590'
				});

			snsMock.on(PublishBatchCommand).resolves({
				Successful: [
					{ Id: '1', MessageId: '4ac0a219-1122-33b3-4445-5556666d734d' }
				]
			});

			this.snsTrigger.session = { clientCode: 'defaultClient' };

			const result = await this.snsTrigger.publishEvents(sampleTopicArn, [
				{
					content,
					payloadFixedProperties: ['bar']
				}
			]);

			assert.deepStrictEqual(result, partiallySentResponse);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 2);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 1);

			assertRamListResourceCommand();

			assertSsmGetParameterCommand();

			assertS3PutObjectCommand(content, buckets[0].bucketName);

			assertS3PutObjectCommand(content, buckets[1].bucketName);

		});

		it('Should publish the event content in S3 if it is greater than 256KB', async () => {

			const partiallySentResponse = {
				successCount: 1,
				failedCount: 0,
				success: [
					{
						Id: '1',
						messageId: '4ac0a219-1122-33b3-4445-5556666d734d'
					}
				],
				failed: []
			};

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			s3Mock.on(PutObjectCommand).resolves({
				ETag: '5d41402abc4b2a76b9719d911017c590'
			});

			snsMock.on(PublishBatchCommand).resolves({
				Successful: [
					{ Id: '1', MessageId: '4ac0a219-1122-33b3-4445-5556666d734d' }
				]
			});

			this.snsTrigger.session = { clientCode: 'defaultClient' };

			const content = {
				bar: 'bar',
				foo: 'x'.repeat(256 * 1024)
			};

			const result = await this.snsTrigger.publishEvents(sampleTopicArn, [
				{
					payloadFixedProperties: ['bar'],
					content
				}
			]);

			assert.deepStrictEqual(result, partiallySentResponse);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 1);

			assertRamListResourceCommand();
			assertSsmGetParameterCommand();
			assertS3PutObjectCommand(content);

			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand, {
				TopicArn: sampleTopicArn,
				PublishBatchRequestEntries: [
					{
						Id: '1',
						MessageAttributes: {
							'janis-client': {
								DataType: 'String',
								StringValue: 'defaultClient'
							},
							topicName: {
								DataType: 'String',
								StringValue: 'MyTopic'
							}
						},
						Message: JSON.stringify({
							contentS3Location: {
								path: contentS3Path,
								bucketName: buckets[0].bucketName,
								region: buckets[0].region
							},
							bar: 'bar'
						})
					}
				]
			}, true).length, 1);

		});

		it('Should split events in batches not greater than 256KB', async () => {

			snsMock.on(PublishBatchCommand)
				.resolvesOnce({
					Successful: [
						{ Id: '1', MessageId: '4ac0a219-1122-33b3-4445-5556666d734d' }
					]
				})
				.resolvesOnce({
					Failed: [{
						Id: '2',
						Code: 'SNS001',
						Message: 'SNS Failed'
					}]
				});

			const result = await this.snsTrigger.publishEvents(sampleTopicArn, [
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

			assert.deepStrictEqual(result, {
				successCount: 1,
				failedCount: 1,
				success: [
					{
						Id: '1',
						messageId: '4ac0a219-1122-33b3-4445-5556666d734d'
					}
				],
				failed: [
					{
						Id: '2',
						Code: 'SNS001',
						Message: 'SNS Failed'
					}
				]
			});
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 2);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 0);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 0);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 2);

			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand, {
				TopicArn: sampleTopicArn,
				PublishBatchRequestEntries: [
					{
						Id: '1',
						Message: JSON.stringify({
							foo: 'x'.repeat(150 * 1024)
						}),
						MessageAttributes: {
							'janis-client': {
								DataType: 'String',
								StringValue: 'defaultClient'
							},
							topicName: {
								DataType: 'String',
								StringValue: 'MyTopic'
							}
						}
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
						MessageAttributes: {
							'janis-client': {
								DataType: 'String',
								StringValue: 'defaultClient'
							},
							topicName: {
								DataType: 'String',
								StringValue: 'MyTopic'
							}
						}

					}
				]
			}, true).length, 1);
		});

		it('Should split events in batches not greater than 10 entries', async () => {

			snsMock.on(PublishBatchCommand)
				.resolvesOnce({
					Successful: [
						{ Id: '1', MessageId: 'msg-1' },
						{ Id: '2', MessageId: 'msg-2' }
					]
				})
				.resolvesOnce({
					Failed: [{
						Id: '3',
						Code: 'SNS001',
						Message: 'SNS Failed'
					}]
				});

			// should have 2 batches, first with 10 and the second with 5
			const events = Array.from({ length: 15 }, (_, index) => ({
				content: { message: `Event ${index + 1}` }
			}));

			const result = await this.snsTrigger.publishEvents(sampleTopicArn, events);

			assert.deepStrictEqual(result, {
				successCount: 2,
				failedCount: 1,
				success: [
					{
						Id: '1',
						messageId: 'msg-1'
					},
					{
						Id: '2',
						messageId: 'msg-2'
					}
				],
				failed: [
					{
						Id: '3',
						Code: 'SNS001',
						Message: 'SNS Failed'
					}
				]
			});
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 2);

			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand, {
				TopicArn: sampleTopicArn,
				PublishBatchRequestEntries: Array.from({ length: 10 }, (_, index) => ({
					Id: `${index + 1}`,
					Message: JSON.stringify({ message: `Event ${index + 1}` }),
					MessageAttributes: {
						'janis-client': {
							DataType: 'String',
							StringValue: 'defaultClient'
						},
						topicName: {
							DataType: 'String',
							StringValue: 'MyTopic'
						}

					}

				}))
			}, true).length, 1);

			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand, {
				TopicArn: sampleTopicArn,
				PublishBatchRequestEntries: Array.from({ length: 5 }, (_, index) => ({
					Id: `${index + 11}`,
					Message: JSON.stringify({ message: `Event ${index + 11}` }),
					MessageAttributes: {
						'janis-client': {
							DataType: 'String',
							StringValue: 'defaultClient'
						},
						topicName: {
							DataType: 'String',
							StringValue: 'MyTopic'
						}
					}
				}))
			}, true).length, 1);

		});

		it('Should reject if the arn format is not valid', async () => {

			s3Mock.on(PutObjectCommand);
			snsMock.on(PublishBatchCommand);
			ssmMock.on(GetParameterCommand);
			ramMock.on(ListResourcesCommand);

			const invalidSnsArn = 'arn:invalid-arn';

			const result = await assert.rejects(this.snsTrigger.publishEvents(invalidSnsArn, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]), { message: `Invalid SNS ARN: ${invalidSnsArn}` });

			assert.deepStrictEqual(result, undefined);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 0);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 0);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 0);

		});

		it('Should handle S3 upload failure for multiple payloads and return failure result without rejecting', async () => {

			// Use to restore the stubbed randomId
			sinon.restore();

			snsMock
				.on(PublishBatchCommand)
				.resolvesOnce({
					Failed: [
						{ Id: '1', Code: 'SNS001', Message: 'SNS Failed' }
					]
				})
				.resolvesOnce({
					Successful: [
						{ Id: '2', MessageId: 'msg-2' }
					]
				});

			s3Mock
				.on(PutObjectCommand)
				.rejectsOnce(new Error('Error fetching S3'))
				.rejectsOnce(new Error('Error fetching S3'))
				.resolvesOnce({ ETag: '5d41402abc4b2a76b9719d911017c591' });

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			const expectedResult = {
				successCount: 0,
				failedCount: 2,
				success: [],
				failed: [
					{ Id: '1', Code: 'SNS001', Message: 'SNS Failed' },
					{
						Id: '3',
						Message: 'Failed to upload to all provided s3 buckets',
						Code: 'S3_ERROR'
					}
				]
			};

			const result = await this.snsTrigger.publishEvents(sampleTopicArn, [
				{
					payloadFixedProperties: ['foo'],
					content: {
						foo: 'bar',
						bar: 'bar'
					}
				},
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				},
				{
					payloadFixedProperties: ['foo'],
					content: {
						foo: 'foo',
						bar: 'x'.repeat(256 * 1024)
					}
				}
			]);

			assert.deepStrictEqual(result, expectedResult);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 4);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 1);
		});
	});

});
