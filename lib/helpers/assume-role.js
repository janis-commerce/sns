/* istanbul ignore file */

'use strict';

const logger = require('lllog')();

const { STSClient, AssumeRoleCommand } = require('@aws-sdk/client-sts');

module.exports = class AssumeRole {

	/**
    * @private
    * @static
    */
	static get roleSessionName() {
		return process.env.JANIS_SERVICE_NAME;
	}

	/**
    * @private
    * @static
    */
	static get roleSessionDuration() {
		return 1800; // 30 minutes
	}

	/**
   * @private
   * @static
   * @returns {object} AWS Role credentials
  */
	static async getCredentials(roleArn) {

		let assumedRole;

		try {

			const stsClient = new STSClient();

			assumedRole = await stsClient.send(new AssumeRoleCommand({
				RoleArn: roleArn,
				RoleSessionName: this.roleSessionName,
				DurationSeconds: this.roleSessionDuration
			}));

			return {
				accessKeyId: assumedRole.Credentials.AccessKeyId,
				secretAccessKey: assumedRole.Credentials.SecretAccessKey,
				sessionToken: assumedRole.Credentials.SessionToken
			};

		} catch(err) {
			logger.warn(`Error while trying to assume role ${roleArn}: ${err.message}`);
		}

		if(!assumedRole)
			throw new Error('Could not assume role for upload s3 file');

		const { Credentials } = assumedRole;

		return {
			accessKeyId: Credentials.AccessKeyId,
			secretAccessKey: Credentials.SecretAccessKey,
			sessionToken: Credentials.SessionToken,
			expiration: Credentials.Expiration
		};
	}
};
