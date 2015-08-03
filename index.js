'use strict';

var elasticsearch = require('elasticsearch'),
	esLogAdapter = require('./esLogAdapter'),
	_ = require('lodash'),
	_api = null;

var DataSource = module.exports = function(api, config) {
	// config: https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html
	this.api = _api = api;
	this.client = new elasticsearch.Client({
		hosts: config.hosts,
		log: esLogAdapter.createEsLogAdapter(api.log)
	});
};

DataSource.prototype.prepare = function () {};

/**
 * @param {Object} request
 * @param {Function} callback
 */
DataSource.prototype.process = function (request, callback) {
	//this.api.log.info(request, "flora-elasticsearch processes req");
	var search = this.createSearchConfig(request);
	this.api.log.info(search, "flora-elasticsearch created search request");

	this.client.search(search, function(err, response) {
		if (callback) {
			var result = null;
			if (!err && response && response.hits && response.hits.hits) {
				var data = response.hits.hits.map(function(hit) {
					hit._source._id = hit._id;
					return hit._source;
				})
				
				result = {
					data: data
				};
			}

			callback(err, result);
		}
	});
};

DataSource.prototype.createSearchConfig = function(request) {
	var body = {query:{}};
	if (request.filter) body.query.filtered = {filter:createFilter(request.filter)}; 
	if (!request.limit) request.limit = 1000000;
    if (request.page) {
    	body.from = (request.page-1) * request.limit;
    }
    body.size = request.limit;

	var search = {};
	//search.fields = request.attributes;
	search.index = request.esindex;
	search.type = request.estype;
	if (body) search.body = body;

	return search;
};

/**
 * @param {Function} callback
 */
DataSource.prototype.close = function (callback) {
    // TODO: implement
    if (callback) callback();
};

function createFilter(floraFilter) {
	var orConditions = floraFilter.map(convertAndFilters);

	if (orConditions.length > 1) {
		return {
			or: orConditions
		}
	} else if (orConditions.length == 1) {
		return orConditions[0];
	} else {
		return null;
	}
}

function convertAndFilters(andFilters) {
	var byAttribute = _.groupBy(andFilters, function(filter) {
		return filter.attribute;
	});

	var andConditions = [];
	_.forEach(byAttribute, function(filters, attribute) {
		var f = combineFilters(filters, attribute);
		if (_.isArray(f)) andConditions = andConditions.concat(f)
		else if (f) andConditions.push(f)
	});

	if (andConditions.length > 1) {
		return {
			and: andConditions
		}
	} else if (andConditions.length == 1) {
		return andConditions[0]
	} else {
		return null;
	}
}

function combineFilters(conditions, attribute) {
	var result = {};

	_.forEach(conditions, function(condition) {
		if (condition.operator == 'equal') {
			if (attribute == '_id') {
				result.ids = result.ids || {};
				result.ids.values = result.ids.values || [];
				result.ids.values.push(condition.value);
			} else {
				/*  term filter */
				result.term = {}
				result.term[attribute] = condition.value; 
			}
		} else if (condition.operator == 'greater') {
			result.range = result.range || {};
			result.range[attribute] = result.range[attribute] || {};
			result.range[attribute].gt = condition.value;
		} else if (condition.operator == 'greaterOrEqual') {
			result.range = result.range || {};
			result.range[attribute] = result.range[attribute] || {};
			result.range[attribute].gte = condition.value;
		} else if (condition.operator == 'less') {
			result.range = result.range || {};
			result.range[attribute] = result.range[attribute] || {};
			result.range[attribute].lt = condition.value;
		} else if (condition.operator == 'lessOrEqual') {
			result.range = result.range || {};
			result.range[attribute] = result.range[attribute] || {};
			result.range[attribute].lte = condition.value;
		} else {
			throw new Error("not yet implemented: operator " + condition.operator);
		}
	});

	return result;

}
