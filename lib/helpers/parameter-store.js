/* eslint-disable import/no-extraneous-dependencies */
/* istanbul ignore file */

'use strict';

const { SSMClient, GetParameterCommand } = require('@aws-sdk/client-ssm');
const { RAMClient, ListResourcesCommand } = require('@aws-sdk/client-ram');

const logger = require('lllog')();

module.exports = class ParameterStore {

	/**
	 * @private
	 * @static
	 */
	static get parameterName() {
		return '/shared/internal-storage';
	}

	static async getParameterValue() {

		const parameterArn = await this.getParameterArnFromRAM();

		try {

			const ssmClient = new SSMClient();

			const params = {
				Name: parameterArn,
				WithDecryption: true
			};

			const response = await ssmClient.send(new GetParameterCommand(params));

			return JSON.parse(response.Parameter.Value);
		} catch(error) {
			logger.error(`Unable to get ParameterStore with arn ${parameterArn} - ${error.message}`);
		}
	}

	static async getParameterArnFromRAM() {

		try {

			const command = new ListResourcesCommand({ resourceOwner: 'OTHER-ACCOUNTS' });

			const ramClient = new RAMClient({ region: 'us-east-1' });

			const response = await ramClient.send(command);

			const filteredResources = response.resources.filter(
				resource => resource.arn.includes(this.parameterName)
			);

			if(!filteredResources.length)
				logger.info(`Unable to find resources with parameter ${this.parameterName} in the ARN`);

			return filteredResources[0].arn;
		} catch(error) {
			logger.error(`Error listing ram resources with parameter name: ${this.parameterName}`);
		}
	}

};
