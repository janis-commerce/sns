'use strict';

const snsPermissions = [
	{
		action: ['ssm:GetParameter'],
		resource: 'arn:aws:ssm:us-east-1:*:parameter/shared/internal-storage'
	},
	{
		action: ['ram:ListResources'],
		resource: '*'
	}
];

module.exports = snsPermissions.map(statement => ['iamStatement', statement]);
