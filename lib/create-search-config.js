'use strict';

const _ = require('lodash');

module.exports = function createSearchConfig(request) {
    let body = { size: typeof request.limit === 'number' ? request.limit : 1000000 };

    if (request.filter) {
        body.query = createFilter(request.filter);
    }

    if (request.page) {
        body.from = (request.page - 1) * request.limit;
    }

    /* TODO: think about whether it should be possible to pass a custom elasticsearch query json
       from the client. actually, "search" param should be used to pass user's queries into the fulltext
       engine, which is what we do below now. */
    /*if (request.search) {
        var parsedSearchParameter = JSON.parse(request.search);
        _.forEach(parsedSearchParameter, function (value, key) {
            body[key] = value;
        });
    }*/

    const search = { index: request.esindex, body };

    /* if elasticsearchQuery has been constructed in resource, use that query */
    let { elasticsearchQuery } = request;

    /* ...otherwise, build a query from the search string in request.search */
    if (!elasticsearchQuery && request.search && request.search.length > 0) {
        const { search, queryOptions = {} } = request;
        let query = {
            multi_match: {
                type: 'phrase_prefix',
                query: search,
                fields: !queryOptions.boost ? ['_all'] : queryOptions.boost
            }
        };

        if (queryOptions.field_value_factor) {
            query = {
                function_score: {
                    query,
                    field_value_factor: queryOptions.field_value_factor
                }
            };
        }

        elasticsearchQuery = query;
    }

    if (elasticsearchQuery) {
        // if we have constructed a query before, combine it with the elasticsearchQuery
        search.body.query = search.body.query
            ? { bool: { must: [search.body.query, elasticsearchQuery] } }
            : elasticsearchQuery;
    }

    if (request.aggregateTest) {
        // this.log.debug({ aggregateTest: request.aggregateTest }, 'TODO: elasticsearch aggregations');
        body.aggs = convertAggregate(request.aggregateTest);
        if (!request.limit) {
            body.size = 0;
            search.search_type = 'count';
        }
    }

    if (request.order) {
        const sortMap = typeof request.sortMap === 'string' ? JSON.parse(request.sortMap) : null;
        search.body.sort = [
            ...request.order.map(({ attribute, direction }) => ({
                [sortMap && sortMap[attribute] ? sortMap[attribute] : attribute]: direction
            })),
            '_score'
        ];
    }

    return search;
};

function convertAggregate(aggregate) {
    const aggregations = {};
    aggregate.forEach((agg, key) => {
        if (!agg.alias) {
            /* TODO: think about this. */
            agg.alias = key;
        }

        const result = {};

        if (agg.functionName === 'count') {
            /* -> term aggregation */
            if (!agg.fields || agg.fields.length !== 1) {
                // console.log(agg);
                throw new Error('Invalid count aggregation: requires exactly one field');
            }

            result.terms = { field: agg.fields[0] };
            if (agg.options.limit) {
                result.terms.size = parseInt(agg.options.limit, 10);
            }
            if (agg.aggregate.length) {
                result.terms.aggs = convertAggregate(agg.aggregate);
            }
        } else if (agg.functionName === 'values') {
            if (!agg.fields || agg.fields.length !== 1) {
                // console.log(agg);
                throw new Error('Invalid count aggregation: requires exactly one field');
            }

            result.terms = { field: agg.fields[0] };
            if (agg.options.limit) {
                result.terms.size = parseInt(agg.options.limit, 10);
            }
            if (agg.aggregate.length) {
                result.terms.aggs = convertAggregate(agg.aggregate);
            }
        } else if (agg.functionName === 'max' || agg.functionName === 'min') {
            if (!agg.fields || agg.fields.length !== 1) {
                throw new Error('Invalid ' + agg.functionName + ' aggregation: requires exactly one field');
            }

            result[agg.functionName] = { field: agg.fields[0] };
        } else {
            throw new Error('Unsupported aggregate function: ' + agg.functionName);
        }

        aggregations[agg.alias] = result;
    });

    return aggregations;
}

function combineFilters(conditions, attribute) {
    const result = {};

    _.forEach(conditions, ({ operator, value }) => {
        if (operator === 'equal') {
            if (attribute === '_id') {
                result.ids = result.ids || {};
                result.ids.values = result.ids.values || [];
                if (_.isArray(value)) {
                    result.ids.values = result.ids.values.concat(value);
                } else {
                    result.ids.values.push(value);
                }
            } else {
                /*  term/terms filter */
                if (_.isArray(value)) {
                    result.terms = {};
                    result.terms[attribute] = value;
                } else {
                    result.term = {};
                    result.term[attribute] = value;
                }
            }
        } else if (operator === 'greater') {
            result.range = result.range || {};
            result.range[attribute] = result.range[attribute] || {};
            result.range[attribute].gt = value;
        } else if (operator === 'greaterOrEqual') {
            result.range = result.range || {};
            result.range[attribute] = result.range[attribute] || {};
            result.range[attribute].gte = value;
        } else if (operator === 'less') {
            result.range = result.range || {};
            result.range[attribute] = result.range[attribute] || {};
            result.range[attribute].lt = value;
        } else if (operator === 'lessOrEqual') {
            result.range = result.range || {};
            result.range[attribute] = result.range[attribute] || {};
            result.range[attribute].lte = value;
        } else {
            throw new Error('not yet implemented: operator ' + operator);
        }
    });

    return result;
}

function convertAndFilters(andFilters) {
    const byAttribute = _.groupBy(andFilters, ({ attribute }) => attribute);

    let andConditions = [];
    _.forEach(byAttribute, (filters, attribute) => {
        const f = combineFilters(filters, attribute);
        if (_.isArray(f)) andConditions = andConditions.concat(f);
        else if (f) andConditions.push(f);
    });

    if (andConditions.length > 1) {
        return {
            bool: {
                must: andConditions
            }
        };
    }

    if (andConditions.length === 1) {
        return andConditions[0];
    }

    return null;
}

function createFilter(floraFilter) {
    const orConditions = floraFilter.map(convertAndFilters);

    if (orConditions.length > 1) {
        return {
            bool: {
                should: orConditions
            }
        };
    }

    if (orConditions.length === 1) {
        return orConditions[0];
    }

    return null;
}
