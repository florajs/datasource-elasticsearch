'use strict';

const { ImplementationError } = require('@florajs/errors');

const OPERATOR_MAPPING = {
    greater: 'gt',
    greaterOrEqual: 'gte',
    less: 'lt',
    lessOrEqual: 'lte'
};

module.exports = function createSearchConfig(request) {
    /* TODO: think about whether it should be possible to pass a custom elasticsearch query json
       from the client. actually, "search" param should be used to pass user's queries into the fulltext
       engine, which is what we do below now. */
    /*if (request.search) {
        var parsedSearchParameter = JSON.parse(request.search);
        _.forEach(parsedSearchParameter, function (value, key) {
            body[key] = value;
        });
    }*/

    const search = {
        index: request.esindex,
        _source: request.attributes.map((attribute) =>
            attribute.includes('.') ? `${attribute.split('.')[0]}.*` : attribute
        ),
        body: {
            size: typeof request.limit === 'number' ? request.limit : 1000000,
            ...(request.filter ? { query: createFilter(request.filter) } : {}),
            ...(request.page ? { from: (request.page - 1) * request.limit } : {})
        }
    };

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

function combineFilters(conditions, attribute) {
    return conditions.reduce((result, { operator, value }) => {
        if (operator === 'equal') {
            if (attribute === '_id') {
                result.ids = result.ids || {};
                result.ids.values = [...(result.ids.values || []), ...(Array.isArray(value) ? value : [value])];
                return result;
            }

            /*  term/terms filter */
            return { ...result, ['term' + (Array.isArray(value) ? 's' : '')]: { [attribute]: value } };
        }

        if (Object.prototype.hasOwnProperty.call(OPERATOR_MAPPING, operator)) {
            return {
                ...result,
                range: {
                    [attribute]: {
                        [OPERATOR_MAPPING[operator]]: value
                    }
                }
            };
        }

        throw new ImplementationError(`Operator "${operator}" not implemented`);
    }, {});
}

function convertAndFilters(andFilters) {
    const byAttribute = andFilters.reduce(
        (acc, filter) => ({
            ...acc,
            [filter.attribute]: [...(acc[filter.attribute] || []), filter]
        }),
        {}
    );

    const andConditions = Object.entries(byAttribute).reduce((conditions, [attribute, filters]) => {
        const f = combineFilters(filters, attribute);
        return [...conditions, ...(Array.isArray(f) ? f : [f])];
    }, []);

    if (andConditions.length) {
        return andConditions.length > 1
            ? {
                  bool: {
                      must: andConditions
                  }
              }
            : andConditions[0];
    }

    return null;
}

function createFilter(floraFilter) {
    const orConditions = floraFilter.map(convertAndFilters);

    if (orConditions.length) {
        return orConditions.length > 1
            ? {
                  bool: {
                      should: orConditions
                  }
              }
            : orConditions[0];
    }

    return null;
}
