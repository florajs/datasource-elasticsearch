'use strict';

var
    elasticsearch = require('elasticsearch'),
    esLogAdapter = require('./esLogAdapter'),
    _ = require('lodash');

function flattenObjectKeys(obj) {
    var result = {};

    _.forEach(obj, function (value, key) {
        if (_.isPlainObject(value)) {
            var flat = flattenObjectKeys(value);
            _.forEach(flat, function (v, subKey) {
                result[key + '.' + subKey] = v;
            });
        } else {
            result[key] = value;
        }
    });

    return result;
}

function convertAggregate(aggregate) {
    var aggregations = {};
    aggregate.forEach(function (agg, key) {
        if (!agg.alias) {
            /* TODO: think about this. */
            agg.alias = key;
        }

        var result = {};

        if (agg.functionName === 'count') {
            /* -> term aggregation */
            if (!agg.fields || agg.fields.length != 1) {
                // console.log(agg);
                throw new Error('Invalid count aggregation: requires exactly one field');
            }

            result.terms = {field:agg.fields[0]};
            if (agg.options.limit) {
                result.terms.size = parseInt(agg.options.limit, 10);
            }
            if (agg.aggregate.length) {
                result.terms.aggs = convertAggregate(agg.aggregate);
            }
        } else if (agg.functionName === 'values') {
            if (!agg.fields || agg.fields.length != 1) {
                // console.log(agg);
                throw new Error('Invalid count aggregation: requires exactly one field');
            }

            result.terms = {field:agg.fields[0]};
            if (agg.options.limit) {
                result.terms.size = parseInt(agg.options.limit, 10);
            }
            if (agg.aggregate.length) {
                result.terms.aggs = convertAggregate(agg.aggregate);
            }
        } else if (agg.functionName === 'max' || agg.functionName === 'min') {
            if (!agg.fields || agg.fields.length != 1) {
                throw new Error(`Invalid ${agg.functionName} aggregation: requires exactly one field`);
            }

            result[agg.functionName] = {field:agg.fields[0]};
        } else {
            throw new Error('Unsupported aggregate function: ' + agg.functionName);
        }

        aggregations[agg.alias] = result;
    });

    return aggregations;
}

function transformBuckets(agg, buckets) {
    buckets.forEach(function (bucket) {
        bucket.count = bucket.doc_count;
        delete bucket.doc_count;
    });

    return buckets;
}

function transformAggregateResponse(floraAggregate, elasticAggregations) {
    var result = {};
    floraAggregate.forEach(function (agg) {
        var elasticAgg = elasticAggregations[agg.alias];
        var r = null;
        if (agg.functionName === 'count') {
            r = transformBuckets(agg, elasticAgg.buckets);
        } else if (agg.functionName === 'values') {
            r = transformBuckets(agg, elasticAgg.buckets);
            r = r.map(function (item) {
                return item.key;
            });
        } else if (agg.functionName === 'min' || agg.functionName === 'max') {
            r = elasticAgg.value;
        }

        result[agg.alias] = r;
    });

    return result;
}

var DataSource = module.exports = function (api, config) {
    // config: https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html
    this.api = api;
    this.client = new elasticsearch.Client({
        hosts: config.hosts,
        log: esLogAdapter.createEsLogAdapter(api.log)
    });
};

DataSource.prototype.prepare = function () {
    this.api.log.debug(arguments, "flora-elasticsearch: PREPARE");
    this.api.log.debug(Object.keys(this.api), "flora-elasticsearch: THIS");

};

/**
* @param {Object} request
* @param {Function} callback
*/
DataSource.prototype.process = function (request, callback) {
    var search = this.createSearchConfig(request);
    var log = this.api.log;

    this.api.log.debug(request, "flora-elasticsearch req");


    log.debug(search, 'flora-elasticsearch created search request');

    this.client.search(search, function (err, response) {
        if (request._explain) {
            request._explain.elasticsearch = {
                search: JSON.stringify(search),
                took: response.took,
                _shards: response._shards,
                timed_out: response.timed_out
            };
        }

        if (callback) {
            var result = null;
            if (!err && response && response.hits && response.hits.hits) {
                var data = response.hits.hits.map(function (hit) {
                    hit._source._id = hit._id;
                    hit._source._type = hit._type;

                    /* XXX: API-769 */
                    return flattenObjectKeys(hit._source);
                });

                /*if (response.aggregations) {
                    result = {
                        data: {
                            aggregations: response.aggregations,
                            data: data
                        }
                    }
                } else {
                    result = {
                        data: data
                    };
                } */

                result = {
                    data: data,
                };

                if (response.aggregations) {
                    var transformedAggregateResponse = transformAggregateResponse(
                        request.aggregateTest,
                        response.aggregations
                    );
                    result.aggregations = JSON.parse(JSON.stringify(transformedAggregateResponse));
                }
            }

            callback(err, result);
        }
    });
};

DataSource.prototype.createSearchConfig = function (request) {
    var body = {};
    if (request.filter) {
        body.query = {};
        body.query.filtered = {filter: createFilter(request.filter)};
    }
    if (!request.limit && typeof request.limit !== 'number') request.limit = 1000000;
    if (request.page) {
        body.from = (request.page - 1) * request.limit;
    }
    body.size = request.limit;

    /* TODO: think about whether it should be possible to pass a custom elasticsearch query json
       from the client. actually, "search" param should be used to pass user's queries into the fulltext
       engine, which is what we do below now. */
    /*if (request.search) {
        var parsedSearchParameter = JSON.parse(request.search);
        _.forEach(parsedSearchParameter, function (value, key) {
            body[key] = value;
        });
    }*/

    var search = {};
    //search.fields = request.attributes;
    search.index = request.esindex;
    if (request.estype) search.type = request.estype;

    body = body || {};
    if (body) search.body = body;

    if (request.search && request.search.length > 0) {
        search.body.query = {'simple_query_string': {
            'query': request.search,
            'analyzer': 'snowball',
            'fields': ['_all'],
            'default_operator': 'and'
        }};
    }

    if (request.aggregateTest) {
        this.api.log.debug({aggregateTest: request.aggregateTest}, 'TODO: elasticsearch aggregations');
        body.aggs = convertAggregate(request.aggregateTest);
        if (!request.limit) {
            body.size = 0;
            search.search_type = 'count';
        }
    }

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
        };
    } else if (orConditions.length === 1) {
        return orConditions[0];
    } else {
        return null;
    }
}

function convertAndFilters(andFilters) {
    var byAttribute = _.groupBy(andFilters, function (filter) {
        return filter.attribute;
    });

    var andConditions = [];
    _.forEach(byAttribute, function (filters, attribute) {
        var f = combineFilters(filters, attribute);
        if (_.isArray(f)) andConditions = andConditions.concat(f);
        else if (f) andConditions.push(f);
    });

    if (andConditions.length > 1) {
        return {
            and: andConditions
        };
    } else if (andConditions.length === 1) {
        return andConditions[0];
    } else {
        return null;
    }
}

function combineFilters(conditions, attribute) {
    var result = {};

    _.forEach(conditions, function (condition) {
        if (condition.operator === 'equal') {
            if (attribute === '_id') {
                result.ids = result.ids || {};
                result.ids.values = result.ids.values || [];
                result.ids.values.push(condition.value);
            } else {
                /*  term filter */
                result.term = {};
                result.term[attribute] = condition.value;
            }
        } else if (condition.operator === 'greater') {
            result.range = result.range || {};
            result.range[attribute] = result.range[attribute] || {};
            result.range[attribute].gt = condition.value;
        } else if (condition.operator === 'greaterOrEqual') {
            result.range = result.range || {};
            result.range[attribute] = result.range[attribute] || {};
            result.range[attribute].gte = condition.value;
        } else if (condition.operator === 'less') {
            result.range = result.range || {};
            result.range[attribute] = result.range[attribute] || {};
            result.range[attribute].lt = condition.value;
        } else if (condition.operator === 'lessOrEqual') {
            result.range = result.range || {};
            result.range[attribute] = result.range[attribute] || {};
            result.range[attribute].lte = condition.value;
        } else {
            throw new Error('not yet implemented: operator ' + condition.operator);
        }
    });

    return result;

}
