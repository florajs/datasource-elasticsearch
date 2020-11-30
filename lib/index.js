'use strict';

const elasticsearch = require('@elastic/elasticsearch');
const _ = require('lodash');

const logAdapter = require('./log-adapter');
const createSearchConfig = require('./create-search-config');

class DataSource {
    /**
     * @param {flora.Api} api
     * @param {Object} config
     * @param {string} config.node
     * @param {Array} config.nodes
     * @param {Object} config._status
     */
    constructor(api, config) {
        this.api = api;
        this.log = api.log.child({ component: 'flora-elasticsearch' });
        this.client = new elasticsearch.Client({
            node: config.node,
            nodes: config.nodes,
            log: logAdapter.createEsLogAdapter(this.log)
        });
        this.status = config._status;
    }

    /**
     * @param {Object} dsConfig
     */
    prepare(dsConfig) {
        const queryOptions = {};
        if (dsConfig && dsConfig.boost) {
            queryOptions.boost = dsConfig.boost.split(',');
        }

        if (dsConfig && dsConfig.field_value_factor) {
            queryOptions.field_value_factor = JSON.parse(dsConfig.field_value_factor);
        }

        dsConfig.queryOptions = queryOptions;
    }

    /**
     * @param {Object} request
     * @returns {Promise<Object>}
     */
    async process(request) {
        const search = createSearchConfig(request);

        if (this.status) {
            this.status.increment('dataSourceQueries');
        }

        this.log.debug({ request, search }, 'flora-elasticsearch req -> search');
        const response = (await this.client.search(search)).body;

        if (request._explain) {
            // eslint-disable-next-line require-atomic-updates
            request._explain.elasticsearch = {
                search: JSON.stringify(search),
                took: response.took,
                _shards: response._shards,
                timed_out: response.timed_out
            };
        }

        let result = null;
        if (response && response.hits && response.hits.hits) {
            const data = response.hits.hits.map(function (hit) {
                hit._source._id = hit._id;
                hit._source._type = hit._type;

                /* XXX: API-769 */
                return flattenObjectKeys(hit._source);
            });

            const totalCount = response.hits.total ? response.hits.total.value : null;

            result = {
                data,
                totalCount
            };

            if (response.aggregations) {
                const transformedAggregateResponse = transformAggregateResponse(
                    request.aggregateTest,
                    response.aggregations
                );
                result.aggregations = JSON.parse(JSON.stringify(transformedAggregateResponse));
            }
        }

        return result;
    }

    /**
     * @returns {Promise}
     */
    close() {
        return Promise.resolve();
    }
}

module.exports = DataSource;

function flattenObjectKeys(obj) {
    const result = {};

    _.forEach(obj, function (value, key) {
        if (_.isPlainObject(value)) {
            const flat = flattenObjectKeys(value);
            _.forEach(flat, function (v, subKey) {
                result[key + '.' + subKey] = v;
            });
        } else {
            result[key] = value;
        }
    });

    return result;
}

function transformBuckets(agg, buckets) {
    buckets.forEach(function (bucket) {
        bucket.count = bucket.doc_count;
        delete bucket.doc_count;
    });

    return buckets;
}

function transformAggregateResponse(floraAggregate, elasticAggregations) {
    const result = {};
    floraAggregate.forEach(function (agg) {
        const elasticAgg = elasticAggregations[agg.alias];
        let r = null;
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
