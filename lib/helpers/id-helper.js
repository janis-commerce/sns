'use strict';

// eslint-disable-next-line import/no-extraneous-dependencies
const { customAlphabet } = require('nanoid');

const randomValue = memoize(digits => customAlphabet('ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789', digits));

module.exports = {
	randomValue: digits => randomValue(digits)()
};
