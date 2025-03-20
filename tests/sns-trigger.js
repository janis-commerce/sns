'use strict';

require('lllog')('none');
const sinon = require('sinon');
const assert = require('assert');

const { mockClient } = require('aws-sdk-client-mock');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { SNSClient, PublishCommand, PublishBatchCommand } = require('@aws-sdk/client-sns');
const { SSMClient, GetParameterCommand } = require('@aws-sdk/client-ssm');
const { RAMClient, ListResourcesCommand } = require('@aws-sdk/client-ram');
const { STSClient, AssumeRoleCommand } = require('@aws-sdk/client-sts');

const { SnsTrigger } = require('../lib');
const ParameterStore = require('../lib/helpers/parameter-store');

describe('SnsTrigger', () => {

	let snsMock;
	let ssmMock;
	let ramMock;
	let s3Mock;
	let stsMock;
	let clock;

	const fakeDate = new Date(2025, 2, 6);
	const randomId = 'fake-id';
	const parameterName = 'shared/internal-storage';
	const sampleTopicArn = 'arn:aws:sns:us-east-1:123456789012:MyTopic';
	const sampleTopicFifo = `${sampleTopicArn}.fifo`;
	const parameterNameStoreArn = `arn:aws:ssm:us-east-1:12345678:parameter/${parameterName}`;
	const roleArn = 'arn:aws:iam::1234567890:role/defaultRoleName';
	const contentS3Path = `topics/defaultClient/service-name/MyTopic/2025/03/06/${randomId}.json`;

	const credentials = {
		AccessKeyId: 'accessKeyIdTest',
		SecretAccessKey: 'secretAccessKeyTest',
		SessionToken: 'sessionTokenTest'
	};

	const buckets = [
		{
			bucketName: 'sample-bucket-name-us-east-1',
			roleArn,
			region: 'us-east-1',
			default: true
		},
		{
			bucketName: 'sample-bucket-name-us-west-1',
			roleArn,
			region: 'us-west-1'
		}
	];

	const assertListResourceCommand = () => {
		assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand, {
			resourceOwner: 'OTHER-ACCOUNTS'
		}, true).length, 1);
	};

	const assertGetParameterCommand = () => {
		assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand, {
			Name: parameterNameStoreArn,
			WithDecryption: true
		}, true).length, 1);
	};

	const assertAssumeRoleCommand = (callsNumber = 1) => {
		assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand, {
			RoleArn: buckets[0].roleArn,
			RoleSessionName: 'service-name',
			DurationSeconds: 1800
		}, true).length, callsNumber);
	};

	const assertPutObjectCommand = (body, bucketName = buckets[0].bucketName) => {
		assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand, {
			Bucket: bucketName,
			Key: contentS3Path,
			Body: JSON.stringify(body)
		}, true).length, 1);
	};

	beforeEach(() => {
		ssmMock = mockClient(SSMClient);
		snsMock = mockClient(SNSClient);
		ramMock = mockClient(RAMClient);
		s3Mock = mockClient(S3Client);
		stsMock = mockClient(STSClient);
		clock = sinon.useFakeTimers(fakeDate.getTime());

		this.snsTrigger = new SnsTrigger();
		this.snsTrigger.session = { clientCode: 'defaultClient' };
		sinon.stub(this.snsTrigger, 'randomId').get(() => randomId);

		process.env.JANIS_SERVICE_NAME = 'service-name';

	});

	afterEach(() => {
		snsMock.restore();
		ssmMock.restore();
		ramMock.restore();
		s3Mock.restore();
		stsMock.restore();
		clock.restore();
	});

	describe('publishEvent', () => {

		beforeEach(() => {
			ssmMock = mockClient(SSMClient);
			snsMock = mockClient(SNSClient);
			ramMock = mockClient(RAMClient);
			s3Mock = mockClient(S3Client);
			stsMock = mockClient(STSClient);
			process.env.JANIS_SERVICE_NAME = 'service-name';
		});

		afterEach(() => {
			snsMock.restore();
			ssmMock.restore();
			ramMock.restore();
			s3Mock.restore();
			stsMock.restore();
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

		it('Should publish a single event with s3 content path if it is greater than 256KB (FIFO SQS)', async () => {

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
			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand, {
				TopicArn: sampleTopicFifo,
				Message: JSON.stringify({ contentS3Path, bar: 'bar' }),
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

		it('Should fail if session with clientCode is missing', async () => {

			stsMock.on(AssumeRoleCommand);
			s3Mock.on(PutObjectCommand);
			snsMock.on(PublishCommand);
			ssmMock.on(GetParameterCommand);
			ramMock.on(ListResourcesCommand);

			this.snsTrigger.session = {};

			const result = await assert.rejects(this.snsTrigger.publishEvents(sampleTopicArn, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]), { message: 'The session must have a clientCode' });

			assert.deepStrictEqual(result, undefined);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 0);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 0);
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 0);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand).length, 0);
			assert.deepEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
		});

	});

	describe('publishEvents', () => {

		beforeEach(() => {

		});

		afterEach(() => {
			sinon.restore();
		});

		afterEach(() => {
			ParameterStore.clearCache();
		});

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

			const result = await this.snsTrigger.publishEvents(sampleTopicArn, [
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

			const result = await this.snsTrigger.publishEvents(sampleTopicArn, [
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

			stsMock.on(AssumeRoleCommand);
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
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 0);
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

		it('Should reject if payload upload fails in both S3 buckets', async () => {

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

			stsMock.on(AssumeRoleCommand).resolves({
				Credentials: credentials
			});

			const result = await assert.rejects(this.snsTrigger.publishEvents(sampleTopicArn, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]), { message: 'Failed to upload to both default and provisional buckets' });

			assert.deepStrictEqual(result, undefined);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 2);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 2);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 0);
		});

		it('Should reject if fails retrieve parameter name from ram resources', async () => {

			stsMock.on(AssumeRoleCommand);
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
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 0);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 0);
			assertListResourceCommand();
		});

		it('Should reject if cannot find resources with the parameter name in the ARN', async () => {

			stsMock.on(AssumeRoleCommand);
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
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 0);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 0);
			assert.deepEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assertListResourceCommand();

		});

		it('Should reject if fail to retrieve credentials to sts assume role', async () => {

			snsMock.on(PublishBatchCommand);
			s3Mock.on(PutObjectCommand);

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			stsMock.on(AssumeRoleCommand)
				.rejects(new Error('Not authorized'));

			const result = await assert.rejects(this.snsTrigger.publishEvents(sampleTopicArn, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]), { message: `Error while trying to assume role arn ${roleArn}: Not authorized` });

			assert.deepStrictEqual(result, undefined);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 1);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 0);

			assertListResourceCommand();
			assertGetParameterCommand();
			assertAssumeRoleCommand(1);

		});

		it('Should upload a payload to the provisional S3 bucket if the default bucket upload fails', async () => {

			const content = {
				bar: 'bar',
				foo: 'x'.repeat(256 * 1024)
			};

			const partiallySentResponse = {
				successCount: 1,
				failedCount: 0,
				outputs: [
					{
						success: true,
						messageId: '4ac0a219-1122-33b3-4445-5556666d734d'
					}
				]
			};

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

			s3Mock.on(PutObjectCommand)
				.rejectsOnce(new Error('Error fetching S3'))
				.resolvesOnce({
					ETag: '5d41402abc4b2a76b9719d911017c590'
				});

			snsMock.on(PublishBatchCommand).resolves({
				Successful: [
					{ MessageId: '4ac0a219-1122-33b3-4445-5556666d734d' }
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
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 2);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 2);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 1);

			assertListResourceCommand();

			assertGetParameterCommand();

			assertAssumeRoleCommand(2);

			assertPutObjectCommand(content, buckets[0].bucketName);

			assertPutObjectCommand(content, buckets[1].bucketName);

		});

		it('Should publish the event content in S3 if it is greater than 256KB', async () => {

			const partiallySentResponse = {
				successCount: 1,
				failedCount: 0,
				outputs: [
					{
						success: true,
						messageId: '4ac0a219-1122-33b3-4445-5556666d734d'
					}
				]
			};

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

			snsMock.on(PublishBatchCommand).resolves({
				Successful: [
					{ MessageId: '4ac0a219-1122-33b3-4445-5556666d734d' }
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
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 1);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 1);

			assertListResourceCommand();
			assertGetParameterCommand();
			assertAssumeRoleCommand();
			assertPutObjectCommand(content);

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
						Message: JSON.stringify({ contentS3Path, bar: 'bar' })
					}
				]
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

			assert.deepStrictEqual(result, multiEventResponse);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 2);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 0);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 0);
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 0);
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

			const result = await this.snsTrigger.publishEvents(sampleTopicArn, events);

			assert.deepStrictEqual(result.successCount, 2);
			assert.deepStrictEqual(result.failedCount, 1);
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

		it('Should reject if the que url format is not valid', async () => {

			stsMock.on(AssumeRoleCommand);
			s3Mock.on(PutObjectCommand);
			snsMock.on(PublishBatchCommand);
			ssmMock.on(GetParameterCommand);
			ramMock.on(ListResourcesCommand);

			const invalidSqsUrl = 'arn:invalid-arn';

			const result = await assert.rejects(this.snsTrigger.publishEvents(invalidSqsUrl, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]), { message: `Invalid SNS ARN: ${invalidSqsUrl}` });

			assert.deepStrictEqual(result, undefined);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 0);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 0);
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 0);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 0);

		});
	});

});
