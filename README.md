# sns

![Build Status](https://github.com/janis-commerce/sns/workflows/Build%20Status/badge.svg)
[![npm version](https://badge.fury.io/js/%40janiscommerce%2Fsns.svg)](https://www.npmjs.com/package/@janiscommerce/sns)

SNS Wrapper

## Installation
```sh
npm install @janiscommerce/sns
```

## API

> You can see `SnsTrigger` and every available property in the [types definition](types/sns-trigger.d.ts) or using your IDE intellisense.

### SNS Trigger

> This class is compatible with [@janiscommerce/api-session](https://npmjs.com/@janiscommerce/api-session). If it's instanciated using `getSessionInstance`, a message attribute `janis-client` with the session's `clientCode` will be automatically added to every event.

> The event `content` will be JSON-stringified before sending

#### Publish single event

```js
const { SnsTrigger } = require('@janiscommerce/sns');

const snsTrigger = new SnsTrigger();

const result = await snsTrigger.publishEvent('topicName', {
	content: {
		id: '1'
	},
	attributes: {
		source: 'user',
		platform: 'mobile'
	}
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

const snsTrigger = new SnsTrigger();

const result = await snsTrigger.publishEvents('topicName', [
	{
		content: {
			id: '1'
		},
		attributes: {
			source: 'user',
			platform: 'mobile'
		}
	},
	{
		content: {
			id: '2'
		},
		attributes: {
			source: 'user',
			platform: 'mobile'
		}
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
