'use strict';

/* eslint-disable no-template-curly-in-string */

const snsPermissions = [
	{
		action: ['ssm:GetParameter'],
		resource: 'arn:aws:ssm:us-east-1:*:parameter/shared/internal-storage'
	},
	{
		action: ['ram:ListResources'],
		resource: '*'
	},
	{
		action: ['s3:PutObject', 's3:getObject'],
		resource: 'arn:aws:s3:::janis-internal-storage-us-east-1-beta/*'
	},
	{
		action: ['s3:PutObject', 's3:getObject'],
		resource: 'arn:aws:s3:::janis-internal-storage-us-west-1-beta/*'
	},
	{
		action: 'sts:AssumeRole',
		resource: 'arn:aws:iam::*:role/LambdaRemoteStorage'
	}
];

module.exports = snsPermissions.map(statement => ['iamStatement', statement]);
