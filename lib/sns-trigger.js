'use strict';

const { randomUUID } = require('node:crypto');

const { SNSClient, PublishBatchCommand, PublishCommand } = require('@aws-sdk/client-sns');
const logger = require('lllog')();

const AsyncWithConcurrency = require('./helpers/async-with-concurrency');

// 256 KB Limit
const SNS_MESSAGE_LIMIT_SIZE = 256 * 1024;

module.exports = class SnsTrigger {

	constructor() {
		/** @private */
		this.sns ??= new SNSClient();
	}

	/**
	 * @type {SNSClient}
	 * @private
	 */
	get sns() {
		return this._sns;
	}

	/**
	 * @private
	 */
	set sns(snsClient) {
		/** @private */
		this._sns = snsClient;
	}

	/**
	 * @param {string} topicArn
	 * @param {import('./types/sns-trigger').SNSEvent} event
	 */
	publishEvent(topicArn, event) {

		const parsedAttributes = this.parseMessageAttributes(event.attributes);

		return this.sns.send(new PublishCommand({
			TopicArn: topicArn,
			Message: JSON.stringify(event.content),
			...parsedAttributes && { MessageAttributes: parsedAttributes },
			...event.subject && { Subject: event.subject },
			...event.messageDeduplicationId && { MessageDeduplicationId: event.messageDeduplicationId },
			...event.messageGroupId && { MessageGroupId: event.messageGroupId },
			...event.messageStructure && { MessageStructure: event.messageStructure }
		}));
	}

	/**
	 * @param {string} topicArn
	 * @param {import('./types/sns-trigger').SNSEvent[]} events
	 * @returns {Promise<import('./types/sns-trigger').PublishEventsResponse>}
	 */
	async publishEvents(topicArn, events) {

		const parsedEvents = this.parseEvents(events);

		if(!parsedEvents?.length) {
			return {
				successCount: 0,
				failedCount: 0,
				outputs: []
			};
		}

		const asyncWithConcurrency = new AsyncWithConcurrency(batch => {

			return this.sns.send(new PublishBatchCommand({
				TopicArn: topicArn,
				PublishBatchRequestEntries: [batch]
			}));

		}, 25);

		/** @type {import('@aws-sdk/client-sns').PublishBatchCommandOutput[]} */
		const results = await asyncWithConcurrency.run(parsedEvents);

		let successCount = 0;
		let failedCount = 0;
		const outputs = [];

		results.forEach(result => {
			if(result.Successful) {
				successCount += result.Successful.length;
				result.Successful.forEach(success => outputs.push({
					success: true,
					messageId: success.MessageId
				}));
			}

			if(result.Failed) {
				failedCount += result.Failed.length;
				result.Failed.forEach(failed => outputs.push({
					success: false,
					errorCode: failed.Code,
					errorMessage: failed.Message
				}));
			}
		});

		return {
			successCount,
			failedCount,
			outputs
		};
	}

	/**
	 * @private
	 * @param {import('./types/sns-trigger').SNSEvent[]} events
	 */
	parseEvents(events) {

		let parsedEventsCount = 0;
		const parsedEvents = [
			[]
		];

		let currentBatchIndex = 0;
		let currentBatchSize = 0;

		for(const event of events) {

			const parsedEvent = this.parseEventToSNSRequestEntry(event);
			const parsedEventSize = JSON.stringify(parsedEvent).length;

			if(parsedEventSize > SNS_MESSAGE_LIMIT_SIZE) {
				logger.error('Parsed event size exceeds 256MB limit. It will not be sent.', parsedEvent.Message.substring(0, 100));
				continue;
			}

			if(currentBatchSize + parsedEventSize > SNS_MESSAGE_LIMIT_SIZE) {
				currentBatchIndex++;
				parsedEvents[currentBatchIndex] = [];
			}

			parsedEvents[currentBatchIndex].push(parsedEvent);
			currentBatchSize++;
			parsedEventsCount++;

		}

		return parsedEventsCount ? parsedEvents : [];
	}

	/**
	 * @private
	 * @param {import('./types/sns-trigger').SNSEvent} event
	 */
	parseEventToSNSRequestEntry(event) {

		const parsedAttributes = this.parseMessageAttributes(event.attributes);

		return {
			Id: randomUUID(),
			Message: JSON.stringify(event.content),
			...parsedAttributes && { MessageAttributes: parsedAttributes }
		};
	}

	/**
	 * @private
	 * @param {import('./types/sns-trigger').SNSEventAttributes} attributes
	 */
	parseMessageAttributes(attributes) {

		const parsedAttributes = {};

		if(this.session?.clientCode) {
			parsedAttributes['janis-client'] = {
				DataType: 'string',
				StringValue: this.session.clientCode
			};
		}

		if(attributes) {
			Object.entries(attributes).forEach(([key, value]) => {
				parsedAttributes[key] = {
					DataType: 'String',
					StringValue: value
				};
			});
		}

		return parsedAttributes;
	}

};
