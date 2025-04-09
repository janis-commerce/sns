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
		const parsedEvent = this.formatSNSEvent(event, topicName);
		const parsedEventSize = JSON.stringify(parsedEvent).length;

		const { extraProperties, ...parsedEventBase } = parsedEvent;

		let formattedSnsMessage = parsedEventBase;

		if(parsedEventSize > SNS_MESSAGE_LIMIT_SIZE) {

			const formattedEvent = await this.formatAndUploadEventWithS3Content(parsedEvent);

			formattedSnsMessage = formattedEvent;
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

			const formattedSnsPromises = [];

			for(const { limitExceeded, extraProperties, ...parsedEvent } of batch) {

				if(!limitExceeded) {
					formattedSnsPromises.push(parsedEvent);
					continue;
				}

				const promise = this.formatAndUploadEventWithS3Content({ ...parsedEvent, extraProperties });

				formattedSnsPromises.push(promise);
			}

			const formattedSnsBatch = await Promise.all(formattedSnsPromises);

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

	async formatAndUploadEventWithS3Content(parsedEvent) {

		const { limitExceeded, extraProperties: { payloadFixedProperties, contentS3Path }, ...parsedEventBase } = parsedEvent;

		const bucketList = await ParameterStore.getParameterValue();

		const bucketInfo = await S3Uploader.uploadContentS3Path(bucketList, contentS3Path, parsedEvent.Message);

		const snsFixedContent = this.formatBodyWithContentS3Path(parsedEventBase, payloadFixedProperties, contentS3Path, bucketInfo);

		return { ...parsedEventBase, Message: JSON.stringify(snsFixedContent) };
	}

	/**
		* Formats the SNS message body with the content location in S3.
		* This method generates an object that includes the content location in S3 and, optionally,
		* fixed payload properties extracted from the original message.
		*
		* @param {Object} parsedEvent - The processed SNS event.
		* @param {string[]} payloadFixedProperties - List of specific properties to extract from the original message.
		* @param {string} contentS3Path - The path to the content in S3.
		* @param {Object} bucketInfo - Information about the S3 bucket.
		* @param {string} bucketInfo.region - The region of the S3 bucket.
		* @param {string} bucketInfo.bucketName - The name of the S3 bucket.
		* @returns {Object} An object containing the content location in S3 and the fixed payload properties (if specified).
		*/
	formatBodyWithContentS3Path(parsedEvent, payloadFixedProperties, contentS3Path, bucketInfo = {}) {
		return {
			contentS3Location: { path: contentS3Path, bucketName: bucketInfo.bucketName, region: bucketInfo.region },
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

			const contentFixed = this.formatBodyWithContentS3Path(parsedEvent, payloadFixedProperties, contentS3Path);

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

		const parsedAttributes = {
			topicName: {
				DataType: 'String',
				StringValue: topicName
			}
		};

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

		return parsedAttributes;
	}

	parseExtraProperties(event, topicName) {

		if(!this.session?.clientCode)
			throw new SnsTriggerError('The session must have a clientCode', SnsTriggerError.codes.MISSING_CLIENT_CODE);

		return {
			...event.payloadFixedProperties && { payloadFixedProperties: event.payloadFixedProperties },
			contentS3Path: this.getContentS3Path(topicName)
		};
	}

	getTopicNameFromArn(snsArn) {

		const isValidSnsTopic = this.isValidSnsArn(snsArn);

		if(!isValidSnsTopic)
			throw new SnsTriggerError(`Invalid SNS ARN: ${snsArn}`, SnsTriggerError.codes.INVALID_SNS_ARN);

		const arnParts = snsArn.split(':');
		const snsTopic = arnParts[arnParts.length - 1];

		if(snsTopic.endsWith('.fifo'))
			return snsTopic.substring(0, snsTopic.length - 5);

		return snsTopic;
	}

	isValidSnsArn(arn) {
		const pattern = /^arn:aws:sns:[a-zA-Z0-9-]+:\d{12}:[a-zA-Z0-9]+(\.fifo)?$/;
		return pattern.test(arn);
	}

};
