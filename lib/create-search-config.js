'use strict';

const { ImplementationError, RequestError } = require('@florajs/errors');

const OPERATOR_MAPPING = {
    greater: 'gt',
    greaterOrEqual: 'gte',
    less: 'lt',
    lessOrEqual: 'lte'
};

const MAX_LIMIT = 10000;

module.exports = function createSearchConfig(request) {
    if (request.limit === 'unlimited' || (typeof request.limit === 'number' && request.limit > MAX_LIMIT)) {
        throw new RequestError(`"limit" must not exceed ${MAX_LIMIT}`);
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

    const search = {
        index: request.esindex,
        _source: request.attributes.map((attribute) =>
            attribute.includes('.') ? `${attribute.split('.')[0]}.*` : attribute
        ),
        body: {
            ...(typeof request.limit === 'number' && { size: request.limit }),
            ...(request.filter && { query: createFilter(request.filter) }),
            ...(request.page && { from: (request.page - 1) * request.limit })
        }
    };

    /* if elasticsearchQuery has been constructed in resource, use that query */
    let { elasticsearchQuery } = request;

    /* ...otherwise, build a query from the search string in request.search */
    if (!elasticsearchQuery && request.search && request.search.length > 0) {
        const { search, queryOptions = {} } = request;

        if (!queryOptions.boost) {
            throw new ImplementationError('"boost" query option is required for full-text search');
        }

        let query = {
            multi_match: {
                type: 'phrase_prefix',
                query: search,
                fields: queryOptions.boost
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
        search.body.sort = [...request.order.map(({ attribute, direction }) => ({ [attribute]: direction })), '_score'];
    }

    return search;
};

function createTermFilter(attribute, value) {
    if (attribute === '_id') {
        return { ids: { values: Array.isArray(value) ? value : [value] } };
    }

    /*  term/terms filter */
    return { ['term' + (Array.isArray(value) ? 's' : '')]: { [attribute]: value } };
}

function combineFilters(conditions, attribute) {
    const mustNotConditions = [];

    const condition = conditions.reduce((result, { operator, value }) => {
        if (operator === 'equal') {
            if (attribute === '_id') {
                const values = Array.isArray(value) ? value : [value];
                result.ids = result.ids || {};
                result.ids.values = intersect(values, result.ids.values || values);
                return result;
            }

            return { ...result, ...createTermFilter(attribute, value) };
        }

        if (operator === 'notEqual') {
            mustNotConditions.push(createTermFilter(attribute, value));
            return result;
        }

        if (Object.hasOwn(OPERATOR_MAPPING, operator)) {
            return {
                ...result,
                range: {
                    [attribute]: {
                        ...result.range?.[attribute],
                        [OPERATOR_MAPPING[operator]]: value
                    }
                }
            };
        }

        throw new ImplementationError(`Operator "${operator}" not implemented`);
    }, {});

    return [
        ...(Object.keys(condition).length ? [condition] : []),
        ...(mustNotConditions.length ? [{ bool: { must_not: mustNotConditions } }] : [])
    ];
}

function intersect(arrayA, arrayB) {
    if (arrayA == arrayB) return arrayA; // shortcut for the same array
    return arrayA.filter((value) => arrayB.includes(value));
}

function convertAndFilters(andFilters) {
    const filtersByAttribute = Object.groupBy(andFilters, (filter) => filter.attribute);

    const andConditions = Object.entries(filtersByAttribute).flatMap(([attribute, filters]) =>
        combineFilters(filters, attribute)
    );

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
