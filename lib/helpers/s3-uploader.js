'use strict';

const logger = require('lllog')();

const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const AssumeRole = require('./assume-role');

module.exports = class S3Uploader {

	static async uploadToBucket(bucket, s3ContentPath, body) {

		try {

			const credentials = await AssumeRole.getCredentials(bucket.roleArn);

			if(!credentials)
				return;

			const s3Client = new S3Client({ region: bucket.region, credentials });

			const data = await s3Client.send(new PutObjectCommand({
				Bucket: bucket.bucketName,
				Key: s3ContentPath,
				Body: body
			}));

			return data;
		} catch(error) {
			logger.error(`Error uploading to bucket ${bucket.bucketName} in region ${bucket.region}: ${error.message}`);
		}
	}

	static async uploadS3ContentPath(buckets, s3ContentPath, body) {

		const [defaultBucket, provisionalBucket] = buckets;

		const success = await this.uploadToBucket(defaultBucket, s3ContentPath, body);

		if(success)
			return Promise.resolve();

		const fallbackSuccess = await this.uploadToBucket(provisionalBucket, s3ContentPath, body);

		if(fallbackSuccess)
			return Promise.resolve();

		logger.error('Failed to upload to both default and provisional buckets');

		return Promise.reject();
	}

};
