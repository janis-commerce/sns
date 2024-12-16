'use strict';

const { SNSClient, PublishBatchCommand, PublishCommand } = require('@aws-sdk/client-sns');
const logger = require('lllog')();

const AsyncWithConcurrency = require('./helpers/async-with-concurrency');

// 256 KB Limit
const SNS_MESSAGE_LIMIT_SIZE = 256 * 1024;

// 10 messages per batch request
const SNS_MAX_BATCH_SIZE = 10;

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
	 * @returns {Promise<import('./types/sns-trigger').PublishEventResponse>}
	 */
	async publishEvent(topicArn, event) {

		const snsResponse = await this.sns.send(new PublishCommand({
			TopicArn: topicArn,
			...this.formatSNSEvent(event)
		}));

		return {
			messageId: snsResponse.MessageId,
			...snsResponse.SequenceNumber && { sequenceNumber: snsResponse.SequenceNumber }
		};
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
				PublishBatchRequestEntries: batch
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
					messageId: success.MessageId,
					...success.SequenceNumber && { sequenceNumber: success.SequenceNumber }
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

		let eventIndex = 0;

		for(const event of events) {

			eventIndex++;

			const parsedEvent = this.formatSNSEvent(event, eventIndex);
			const parsedEventSize = JSON.stringify(parsedEvent).length;

			if(parsedEventSize > SNS_MESSAGE_LIMIT_SIZE) {
				logger.error('Parsed event size exceeds 256KB limit. It will not be sent.', parsedEvent.Message.substring(0, 100));
				continue;
			}

			if(currentBatchSize + parsedEventSize > SNS_MESSAGE_LIMIT_SIZE
				|| parsedEvents[currentBatchIndex].length === SNS_MAX_BATCH_SIZE) {
				currentBatchIndex++;
				parsedEvents[currentBatchIndex] = [];
				currentBatchSize = 0;
			}

			parsedEvents[currentBatchIndex].push(parsedEvent);
			currentBatchSize += parsedEventSize;
			parsedEventsCount++;

		}

		return parsedEventsCount ? parsedEvents : [];
	}

	/**
	 * @private
	 * @param {import('./types/sns-trigger').SNSEvent} event
	 * @param {number} [eventIndex] For batch publish only
	 * @returns {import('@aws-sdk/client-sns').PublishCommandInput|import('@aws-sdk/client-sns').PublishBatchCommandInput}
	 */
	formatSNSEvent(event, eventIndex) {

		const parsedAttributes = this.parseMessageAttributes(event.attributes);

		return {
			...eventIndex && { Id: `${eventIndex}` },
			Message: JSON.stringify(event.content),
			...parsedAttributes && { MessageAttributes: parsedAttributes },
			...event.subject && { Subject: event.subject },
			...event.messageDeduplicationId && { MessageDeduplicationId: event.messageDeduplicationId },
			...event.messageGroupId && { MessageGroupId: event.messageGroupId },
			...event.messageStructure && { MessageStructure: event.messageStructure }
		};
	}

	/**
	 * @private
	 * @param {import('./types/sns-trigger').SNSEventAttributes} attributes
	 */
	parseMessageAttributes(attributes) {

		let hasAttributes = false;
		const parsedAttributes = {};

		if(this.session?.clientCode) {
			hasAttributes = true;
			parsedAttributes['janis-client'] = {
				DataType: 'String',
				StringValue: this.session.clientCode
			};
		}

		if(attributes) {
			hasAttributes = true;
			Object.entries(attributes).forEach(([key, value]) => {
				parsedAttributes[key] = {
					DataType: 'String',
					StringValue: value
				};
			});
		}

		return hasAttributes && parsedAttributes;
	}

};
