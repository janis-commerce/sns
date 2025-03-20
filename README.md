# sns

![Build Status](https://github.com/janis-commerce/sns/workflows/Build%20Status/badge.svg)
[![npm version](https://badge.fury.io/js/%40janiscommerce%2Fsns.svg)](https://www.npmjs.com/package/@janiscommerce/sns)

SNS Wrapper

## Installation
```sh
npm install @janiscommerce/sns
```

### Install peer dependencies
```sh
# Install as devDependency if you run your code in AWS Lambda, which already includes the SDK
npm install --dev @aws-sdk/client-sns@3
```

> Why? This is to avoid installing the SDK in production and freezing the SDK version in this package

## API

> You can see `SnsTrigger` and every available property in the [types definition](types/sns-trigger.d.ts) or using your IDE intellisense.

### SNS Trigger

> This class is compatible with [@janiscommerce/api-session](https://npmjs.com/@janiscommerce/api-session). If it's instanciated using `getSessionInstance`, a message attribute `janis-client` with the session's `clientCode` will be automatically added to every event.

> The event `content` will be JSON-stringified before sending

> The event `attributes` can be either Strings or Arrays.  It's important to note that using other data types may cause issues or inconsistencies in the implemented filter policies. Ensure that the values provided for the attributes are always of the expected type to avoid errors in message processing.

> The `payloadFixedProperties` event must be an array of strings containing the properties that must be mandatorily sent in the content. This is to improve error management, as these properties will allow us to identify which data failed and make a decision accordingly.

***Note: This behavior applies from version 1.1.0 onward.***

!important: The session is required to obtain the `clientCode` and construct the `contentS3Pat`h for payloads that exceed the maximum SNS limit.

#### Publish single event

```js
const { SnsTrigger } = require('@janiscommerce/sns');

const snsTrigger = this.session.getSessionInstance(SnsTrigger);

const result = await snsTrigger.publishEvent('topicName', {
	content: {
		id: '1'
	},
	attributes: {
		source: 'user',
		platforms: ['mobile', 'web']
	},
	payloadFixedProperties: ['id']
});

/**
 * Sample Output
 *
 * {
 * 	MessageId: '8563a94f-59f3-4843-8b16-a012867fe97e',
 * 	SequenceNumber: '' // For FIFO topics only
 * }
 */
```

#### Publish multiple events

> This method will send multiple events in one SDK call. It will also separate in batches when the total size limit of 256KB payload size is exceeded. Batches will be sent with a smart concurrency protocol (optimizing calls with a maximum of 25 concurrent calls).

```js
const { SnsTrigger } = require('@janiscommerce/sns');

const snsTrigger = this.session.getSessionInstance(SnsTrigger);

const result = await snsTrigger.publishEvents('topicName', [
	{
		content: {
			id: '1'
		},
		attributes: {
			source: 'user',
			platform: 'mobile'
		},
		payloadFixedProperties: ['id']
	},
	{
		content: {
			id: '2'
		},
		attributes: {
			source: 'user',
			platform: 'mobile'
		},
		payloadFixedProperties: ['id']
	}
]);

/**
 * Sample Output
 *
 * {
 * 	successCount: 1,
 * 	failedCount: 1,
 * 	outputs: [
 * 		{
 * 			success: true,
 *				messageId: '8563a94f-59f3-4843-8b16-a012867fe97e'
 * 		},
 * 		{
 * 			success: false,
 * 			errorCode: '',
 * 			errorMessage: ''
 * 		}
 * 	]
 * }
 */
```
