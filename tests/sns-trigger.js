'use strict';

const { mockClient } = require('aws-sdk-client-mock');
const { SNSClient, PublishCommand, PublishBatchCommand } = require('@aws-sdk/client-sns');

require('lllog')('none');

const assert = require('assert');

const {
	SnsTrigger
} = require('../lib');

describe('SnsTrigger', () => {

	const sampleTopicArn = 'arn:aws:sns:us-east-1:123456789012:MyTopic';

	let snsMock;

	afterEach(() => {
		snsMock.restore();
	});

	describe('publishEvent', () => {

		const singleEventResponse = {
			messageId: '4ac0a219-1122-33b3-4445-5556666d734d'
		};

		const singleEventFifoResponse = {
			messageId: '4ac0a219-1122-33b3-4445-5556666d734d',
			sequenceNumber: '222222222222222222222222'
		};

		beforeEach(() => {
			snsMock = mockClient(SNSClient);
		});

		it('Should publish a single event with content only as minimal requirement (Standard Topic)', async () => {

			snsMock.on(PublishCommand).resolves({
				MessageId: singleEventResponse.messageId
			});

			const snsTrigger = new SnsTrigger();
			const result = await snsTrigger.publishEvent(sampleTopicArn, {
				content: {
					foo: 'bar'
				}
			});

			assert.deepStrictEqual(result, singleEventResponse);

			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand, {
				TopicArn: sampleTopicArn,
				Message: JSON.stringify({
					foo: 'bar'
				})
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
				})
			}, true).length, 1);
		});

		it('Should publish a single event with all available properties (Standard Topic)', async () => {

			snsMock.on(PublishCommand).resolves({
				MessageId: singleEventResponse.messageId
			});

			const snsTrigger = new SnsTrigger();
			const result = await snsTrigger.publishEvent(sampleTopicArn, {
				content: {
					foo: 'bar'
				},
				attributes: {
					foo: 'bar'
				},
				subject: 'test'
			});

			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishCommand, {
				TopicArn: sampleTopicArn,
				Message: JSON.stringify({
					foo: 'bar'
				}),
				MessageAttributes: {
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
				content: {
					foo: 'bar'
				},
				attributes: {
					foo: 'bar'
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
				content: {
					foo: 'bar'
				},
				attributes: {
					foo: 'bar'
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

		beforeEach(() => {
			snsMock = mockClient(SNSClient);
		});

		it('Should publish multiple events with content only as minimal requirement (Standard Topic)', async () => {

			snsMock.on(PublishBatchCommand).resolves({
				Successful: [
					{
						MessageId: '4ac0a219-1122-33b3-4445-5556666d734d'
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
					content: {
						foo: 'bar'
					}
				},
				{
					content: {
						foo: 'baz'
					}
				}
			]);

			assert.deepStrictEqual(result, multiEventResponse);

			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand, {
				TopicArn: sampleTopicArn,
				PublishBatchRequestEntries: [
					{
						Id: '1',
						Message: JSON.stringify({
							foo: 'bar'
						})
					},
					{
						Id: '2',
						Message: JSON.stringify({
							foo: 'baz'
						})
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

			const snsTrigger = new SnsTrigger();
			const result = await snsTrigger.publishEvents(sampleTopicArn, [
				{
					content: {
						foo: 'bar'
					}
				},
				{
					content: {
						foo: 'baz'
					}
				}
			]);

			assert.deepStrictEqual(result, multiEventFifoResponse);

			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand, {
				TopicArn: sampleTopicArn,
				PublishBatchRequestEntries: [
					{
						Id: '1',
						Message: JSON.stringify({
							foo: 'bar'
						})
					},
					{
						Id: '2',
						Message: JSON.stringify({
							foo: 'baz'
						})
					}
				]
			}, true).length, 1);
		});

		it('Should skip events that are greater than 256KB', async () => {

			snsMock.on(PublishBatchCommand).resolves({
				Successful: [
					{
						MessageId: '4ac0a219-1122-33b3-4445-5556666d734d'
					}
				]
			});

			const partiallySentResponse = {
				successCount: 1,
				failedCount: 0,
				outputs: [
					multiEventResponse.outputs[0]
				]
			};

			const snsTrigger = new SnsTrigger();
			const result = await snsTrigger.publishEvents(sampleTopicArn, [
				{
					content: {
						foo: 'bar'
					}
				},
				{
					content: {
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]);

			assert.deepStrictEqual(result, partiallySentResponse);

			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 1);
			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand, {
				TopicArn: sampleTopicArn,
				PublishBatchRequestEntries: [
					{
						Id: '1',
						Message: JSON.stringify({
							foo: 'bar'
						})
					}
				]
			}, true).length, 1);
		});

		it('Should not call SNS if there are no valid events to publish', async () => {

			const partiallySentResponse = {
				successCount: 0,
				failedCount: 0,
				outputs: []
			};

			const snsTrigger = new SnsTrigger();
			const result = await snsTrigger.publishEvents(sampleTopicArn, [
				{
					content: {
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]);

			assert.deepStrictEqual(result, partiallySentResponse);

			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand).length, 0);
		});

		it('Should split events in batches not greater than 256KB', async () => {

			snsMock.on(PublishBatchCommand)
				.resolvesOnce({
					Successful: [
						{
							MessageId: '4ac0a219-1122-33b3-4445-5556666d734d'
						}
					]
				})
				.resolvesOnce({
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
						})
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
						})
					}
				]
			}, true).length, 1);
		});

		it('Should split events in batches not greater than 10 entries', async () => {

			snsMock.on(PublishBatchCommand)
				.resolvesOnce({
					Successful: [{
						MessageId: 'msg-1'
					}, {
						MessageId: 'msg-2'
					}]
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
					Message: JSON.stringify({ message: `Event ${index + 1}` })
				}))
			}, true).length, 1);

			assert.deepStrictEqual(snsMock.commandCalls(PublishBatchCommand, {
				TopicArn: sampleTopicArn,
				PublishBatchRequestEntries: Array.from({ length: 5 }, (_, index) => ({
					Id: `${index + 11}`,
					Message: JSON.stringify({ message: `Event ${index + 11}` })
				}))
			}, true).length, 1);
		});

	});

});
