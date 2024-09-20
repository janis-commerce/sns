export type SNSEventAttributes = Record<string, string>;

type CommonSNSEvent = {
	attributes?: SNSEventAttributes;
	content: any;
	subject?: string;
	messageStructure?: 'json';
}

type FIFOSNSEvent = CommonSNSEvent & {
	messageDeduplicationId?: string;
	messageGroupId?: string;
}

export type SNSEvent = CommonSNSEvent | FIFOSNSEvent;

export type PublishEventOutputSuccess = {
	success: true;
	messageId: string;
}

type PublishEventOutputFailed = {
	success: false;
	errorCode: string;
	errorMessage: string;
}

type PublishEventOutput = PublishEventOutputSuccess | PublishEventOutputFailed;

export type PublishEventsResponse = {
	successCount: number;
	failedCount: number;
	outputs: PublishEventOutput[];
}
