'use strict';

const elasticsearch = require('@elastic/elasticsearch');
const _ = require('lodash');

const createSearchConfig = require('./create-search-config');
const { RequestError } = require('@florajs/errors');

class DataSource {
    /**
     * @param {flora.Api} api
     * @param {Object} config
     * @param {string} config.node
     * @param {Array} config.nodes
     * @param {Object} config._status
     * @param {Object} config.Connection    Used just for testing.
     */
    constructor(api, config) {
        this.api = api;
        this.log = api.log.child({ component: 'datasource-elasticsearch' });
        this.client = new elasticsearch.Client({
            node: config.node,
            nodes: config.nodes,
            ...(config.Connection ? { Connection: config.Connection } : {})
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

        this.log.debug({ request, search }, 'datasource-elasticsearch req -> search');
        let response = null;
        try {
            response = (await this.client.search(search)).body;
        } catch (err) {
            if (err && err.meta && err.meta.statusCode === 400) {
                /* in order to at least expose some internal information from the elasticsearch error, extract the reason and error
                name and re-throw it as a flora RequestError */
                const elasticsearchErrorName = err.name || 'Unknown error';
                const requestError = new RequestError(elasticsearchErrorName + ' from elasticsearch.');
                requestError.info = { originalError: err };
                throw requestError;
            }
            throw err;
        }

        if (request._explain) {
            request._explain.elasticsearch = {
                search: JSON.stringify(search),
                took: response.took,
                _shards: response._shards,
                timed_out: response.timed_out
            };
        }

        return {
            data:
                response && response.hits && response.hits.hits
                    ? response.hits.hits.map(({ _id, _type, _source }) =>
                          /* XXX: API-769 */ flattenObjectKeys({ ..._source, _id, _type })
                      )
                    : [],
            totalCount: response.hits.total ? response.hits.total.value : null
        };
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

    _.forEach(obj, (value, key) => {
        if (_.isPlainObject(value)) {
            const flat = flattenObjectKeys(value);
            _.forEach(flat, (v, subKey) => {
                result[key + '.' + subKey] = v;
            });
        } else {
            result[key] = value;
        }
    });

    return result;
}
