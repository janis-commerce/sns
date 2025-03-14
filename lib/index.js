'use strict';

const SnsTrigger = require('./sns-trigger');
const snsPermissions = require('./helpers/permissions');
const S3Uploader = require('./helpers/s3-uploader');
const ParameterStore = require('./helpers/parameter-store');
const AssumeRole = require('./helpers/assume-role');

module.exports = {
	SnsTrigger,
	S3Uploader,
	ParameterStore,
	AssumeRole,
	snsPermissions
};
