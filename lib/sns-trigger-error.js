'use strict';

class SnsTriggerError extends Error {

	static get codes() {
		return {
			MISSING_CLIENT_CODE: 'MISSING_CLIENT_CODE',
			INVALID_SNS_ARN: 'INVALID_SNS_ARN',
			ASSUME_ROLE_ERROR: 'ASSUME_ROLE_ERROR',
			RAM_ERROR: 'RAM_ERROR',
			SSM_ERROR: 'SSM_ERROR',
			SQS_ERROR: 'SQS_ERROR',
			S3_ERROR: 'S3_ERROR'
		};
	}

	constructor(err, code) {
		super(err);
		this.message = err.message || err;
		this.code = code;
		this.name = 'SnsTriggerError';
	}
}

module.exports = SnsTriggerError;
