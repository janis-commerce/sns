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

***Note: This behavior applies from version 1.1.0 onward.***

> The `payloadFixedProperties` event (available since v1.0.3) must be an array of strings containing the properties that must be mandatorily sent in the content. This is to improve error management, as these properties will allow us to identify which data failed and make a decision accordingly.

### Important Changes from Version ***1.0.3*** to ***Latest***

#### Changes from Version ***1.0.3***

##### Publish single event

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
 *   MessageId: '8563a94f-59f3-4843-8b16-a012867fe97e',
 *   SequenceNumber: '' // For FIFO topics only
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
      foo: 'foo'
    },
    attributes: {
      source: 'user',
      platform: 'mobile'
    },
    payloadFixedProperties: ['id']
  },
  {
    content: {
      bar: 'bar'
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
 *   successCount: 1,
 *   failedCount: 1,
 *   success: [
 *     {
 *       Id: '1',
 *       messageId: '8563a94f-59f3-4843-8b16-a012867fe97e'
 *     }
 *   ],
 *   failed: [
 *     {
 *       Id: '2',
 *       errorCode: 'SNS001',
 *       errorMessage: 'SNS Failed'
 *     }
 *   ]
 * }
 */
```

#### Changes from Version ***2.1.0***

##### Large Payload Support

When using this package with serverless, it's recommended to use `sls-helper-plugin-janis` version 10.2.0 or higher to handle messages that exceed the SNS payload limit. This version is required to ensure proper permissions are set up.

Additionally, it's recommended to update `@janiscommerce/sqs-consumer` to version 1.1.0 or higher in any service that listens to events emitted by this package. This way storage and retrieval of large payloads through S3 will be automatically handled when needed.


For proper permissions setup, you need to export the SNS permissions in your `serverless.yml`:

```js
const { snsPermissions } = require('@janiscommerce/sns');

await helper({
  hooks: [
    ...snsPermissions
  ]
})
```
