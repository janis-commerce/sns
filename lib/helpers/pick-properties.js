/* istanbul ignore file */

'use strict';

module.exports.pickProperties = (object, keys) => {

	const result = {};

	for(const key of keys) {
		if(object.hasOwnProperty(key))
			result[key] = object[key];
	}

	return result;
};
