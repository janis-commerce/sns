/* eslint-disable import/no-extraneous-dependencies */

'use strict';

const logger = require('lllog')();
const { SNSClient, PublishBatchCommand, PublishCommand } = require('@aws-sdk/client-sns');

const AsyncWithConcurrency = require('./helpers/async-with-concurrency');
const ParameterStore = require('./helpers/parameter-store');
const S3Uploader = require('./helpers/s3-uploader');
const SnsTriggerError = require('./sns-trigger-error');
const { pickProperties } = require('./helpers/pick-properties');
const { randomValue } = require('./helpers/id-helper');

const MAX_CONCURRENCY = 25;

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

	get randomId() {
		return randomValue(13);
	}

	/**
	 * @param {string} topicArn
	 * @param {import('./types/sns-trigger').SNSEvent} event
	 * @returns {Promise<import('./types/sns-trigger').PublishEventResponse>}
	 */
	async publishEvent(topicArn, event) {

		const topicName = this.getTopicNameFromArn(topicArn);
		let parsedEvent = this.formatSNSEvent(event, topicName);
		let parsedEventSize = JSON.stringify(parsedEvent).length;

		const {
			extraProperties: { contentS3Path, payloadFixedProperties }, ...parsedEventBase
		} = parsedEvent;

		let formattedSnsMessage = parsedEventBase;

		[parsedEvent, parsedEventSize] = this.handleEventSizeLimit(parsedEvent, parsedEventSize);

		if(parsedEvent.limitExceeded) {

			const bucketList = await ParameterStore.getParameterValue();

			const sqsFixedContent = this.formatBodyWithContentS3Path(parsedEvent, contentS3Path, payloadFixedProperties);

			formattedSnsMessage = {
				...parsedEventBase,
				Message: JSON.stringify(sqsFixedContent)
			};

			await S3Uploader.uploadContentS3Path(bucketList, contentS3Path, parsedEvent.Message);

		}
		const snsResponse = await this.sns.send(new PublishCommand({
			...formattedSnsMessage,
			TopicArn: topicArn
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

		const parsedEvents = this.parseEvents(events, topicArn);

		const asyncWithConcurrency = new AsyncWithConcurrency(async batch => {

			const promises = [];
			const formattedSnsBatch = [];

			for(const { limitExceeded, extraProperties: { payloadFixedProperties, contentS3Path }, ...parsedEvent } of batch) {

				if(!limitExceeded) {
					formattedSnsBatch.push(parsedEvent);
					continue;
				}

				const bucketList = await ParameterStore.getParameterValue();

				const sqsFixedContent = this.formatBodyWithContentS3Path(parsedEvent, contentS3Path, payloadFixedProperties);

				formattedSnsBatch.push({ ...parsedEvent, Message: JSON.stringify(sqsFixedContent) });

				promises.push(S3Uploader.uploadContentS3Path(bucketList, contentS3Path, parsedEvent.Message));
			}

			await Promise.all(promises);

			return this.sns.send(new PublishBatchCommand({
				TopicArn: topicArn,
				PublishBatchRequestEntries: formattedSnsBatch
			}));

		}, MAX_CONCURRENCY);

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
	parseEvents(events, topicArn) {

		const parsedEvents = [
			[]
		];

		let currentBatchIndex = 0;
		let currentBatchSize = 0;

		let eventIndex = 0;

		const topicName = this.getTopicNameFromArn(topicArn);

		for(const event of events) {

			eventIndex++;

			let parsedEvent = this.formatSNSEvent(event, topicName, eventIndex);

			let parsedEventSize = JSON.stringify(parsedEvent).length;

			[parsedEvent, parsedEventSize] = this.handleEventSizeLimit(parsedEvent, parsedEventSize);

			if(currentBatchSize + parsedEventSize > SNS_MESSAGE_LIMIT_SIZE || parsedEvents[currentBatchIndex].length === SNS_MAX_BATCH_SIZE) {
				currentBatchIndex++;
				parsedEvents[currentBatchIndex] = [];
				currentBatchSize = 0;
			}

			parsedEvents[currentBatchIndex].push(parsedEvent);
			currentBatchSize += parsedEventSize;

		}

		return parsedEvents;
	}

	formatBodyWithContentS3Path(parsedEvent, contentS3Path, payloadFixedProperties) {
		return {
			contentS3Path,
			...payloadFixedProperties?.length && {
				...pickProperties(JSON.parse(parsedEvent.Message), payloadFixedProperties)
			}
		};
	}

	/**
	 * @private
	 * @param {import('./types/sns-trigger').SNSEvent} event
	 * @param {number} [eventIndex] For batch publish only
	 * @returns {import('@aws-sdk/client-sns').PublishCommandInput|import('@aws-sdk/client-sns').PublishBatchCommandInput}
	 */
	formatSNSEvent(event, topicName, eventIndex) {

		const parsedAttributes = this.parseMessageAttributes(event.attributes, topicName);
		const extraProperties = this.parseExtraProperties(event, topicName);

		return {
			...eventIndex && { Id: `${eventIndex}` },
			Message: JSON.stringify(event.content),
			MessageAttributes: parsedAttributes,
			...event.subject && { Subject: event.subject },
			...event.messageDeduplicationId && { MessageDeduplicationId: event.messageDeduplicationId },
			...event.messageGroupId && { MessageGroupId: event.messageGroupId },
			...event.messageStructure && { MessageStructure: event.messageStructure },
			...parsedAttributes && { MessageAttributes: parsedAttributes },
			...extraProperties && { extraProperties }
		};
	}

	/**
 	 * @param {Object} parsedEvent - The event object that has been parsed previously.
 	 * @param {number} parsedEventSize - The size of the parsed event in bytes.
 	 * @returns {[Object, number]} - Returns an array where the first element is the modified parsed event,
 	 * and the second element is the updated event size.
 	 */
	handleEventSizeLimit(parsedEvent, parsedEventSize) {

		const {
			extraProperties: { payloadFixedProperties, contentS3Path }
		} = parsedEvent;

		if(parsedEventSize > SNS_MESSAGE_LIMIT_SIZE) {

			const contentFixed = this.formatBodyWithContentS3Path(parsedEvent, contentS3Path, payloadFixedProperties);

			parsedEventSize = JSON.stringify(contentFixed).length;

			parsedEvent.limitExceeded = true;

			logger.info('Parsed event size exceeds the 256KB limit. It will be sent with an S3 content path: ', contentS3Path);
		}

		return [parsedEvent, parsedEventSize];
	}

	getContentS3Path(topicName) {

		const now = new Date();
		const extension = 'json';

		return [
			'topics',
			`${this.session.clientCode}`,
			`${process.env.JANIS_SERVICE_NAME}`,
			topicName,
			this.formatDate(now),
			`${this.randomId}.${extension}`
		].join('/');
	}

	formatDate(date) {
		return [
			date.getFullYear(),
			String(date.getMonth() + 1).padStart(2, '0'),
			String(date.getDate()).padStart(2, '0')
		].join('/');
	}

	/**
	 * @private
	 * @param {import('./types/sns-trigger').SNSEventAttributes} attributes
	 */
	parseMessageAttributes(attributes, topicName) {

		const parsedAttributes = {};

		if(this.session?.clientCode) {
			parsedAttributes['janis-client'] = {
				DataType: 'String',
				StringValue: this.session.clientCode
			};
		}

		if(attributes) {
			Object.entries(attributes).forEach(([key, value]) => {
				parsedAttributes[key] = {
					DataType: Array.isArray(value) ? 'String.Array' : 'String',
					StringValue: Array.isArray(value) ? JSON.stringify(value) : value
				};
			});
		}

		parsedAttributes.topicName = {
			DataType: 'String',
			StringValue: topicName
		};

		return parsedAttributes;
	}

	parseExtraProperties(event, sqsName) {

		if(!this.session?.clientCode)
			throw new SnsTriggerError('The session must have a clientCode', SnsTriggerError.codes.MISSING_CLIENT_CODE);

		return {
			...event.payloadFixedProperties && { payloadFixedProperties: event.payloadFixedProperties },
			contentS3Path: this.getContentS3Path(sqsName)
		};
	}

	getTopicNameFromArn(snsArn) {

		const isValidSqsUrl = this.isValidSnsArn(snsArn);

		if(!isValidSqsUrl)
			throw new SnsTriggerError(`Invalid SNS ARN: ${snsArn}`, SnsTriggerError.codes.INVALID_SNS_ARN);

		const arnParts = snsArn.split(':');
		let snsTopic = arnParts[arnParts.length - 1];

		if(snsTopic.search('fifo') !== -1)
			[snsTopic] = snsTopic.split('.');

		return snsTopic;
	}

	isValidSnsArn(arn) {
		const pattern = /^arn:aws:sns:[a-zA-Z0-9-]+:\d{12}:[a-zA-Z0-9]+(\.fifo)?$/;
		return pattern.test(arn);
	}

};
