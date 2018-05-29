'use strict';

const _get = require('lodash.get');
const chunk = require('lodash.chunk');
const qs = require('querystring');
const DataLoader = require('dataloader');
const graphqlFields = require('graphql-fields');

const INCLUDE_DEPTH = 1;
const CHUNK_SIZE = 70;
// Chunk size may be unreliable. If wrong, may get "Bad Request" from graphQL
const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 1000;
const FORBIDDEN_QUERY_PARAMS = ['skip', 'limit', 'include', 'content_type', 'locale'];
const MAX_FIELDS = 50

module.exports = createEntryLoader;

const getSelectedFields = info => {
  if (!info) {
    return null;
  }
  const topLevelFields = Object.keys(graphqlFields(info)).filter(key => key !== 'sys');
  if (topLevelFields.length < 1 || topLevelFields.length > MAX_FIELDS) {
    // There is a limit to the number of fields we can select. If too many get everything
    return null;
  }
  const contentfulFields = topLevelFields.map(fieldKey => `fields.${fieldKey}`).join(',')
  return `sys,${contentfulFields}`
}

function createEntryLoader (http) {
  const loader = new DataLoader(load);
  const assets = {};

  return {
    get: getOne,
    getMany: loader.loadMany.bind(loader),
    query: (ctId, args, info) => query(ctId, args, info).then(res => res.items),
    count: (ctId, args) => query(ctId, args).then(res => res.total),
    queryAll,
    getIncludedAsset: id => assets[id],
    getTimeline: () => http.timeline
  };

  function load (ids) {
    // we need to chunk IDs and fire multiple requests so we don't produce URLs
    // that are too long (for the server to handle)
    const requests = chunk(ids, CHUNK_SIZE)
    .map(ids => http.get('/entries', {
      limit: CHUNK_SIZE,
      skip: 0,
      include: INCLUDE_DEPTH,
      'sys.id[in]': ids.join(',')
    }));

    return Promise.all(requests)
    .then(responses => responses.reduce((acc, res) => {
      prime(res);
      _get(res, ['items'], []).forEach(e => acc[e.sys.id] = e);
      return acc;
    }, {}))
    .then(byId => ids.map(id => byId[id]));
  }

  function getOne (id, forcedCtId, info) {
    console.log('getOne', id, info && graphqlFields(info))
    return loader.load(id)
    .then(res => {
      const ctId = _get(res, ['sys', 'contentType', 'sys', 'id']);
      if (forcedCtId && ctId !== forcedCtId) {
        throw new Error('Does not match the forced Content Type ID.');
      } else {
        return res;
      }
    });
  }

  function query (ctId, {q = '', skip = 0, limit = DEFAULT_LIMIT} = {}, info) {
    console.log('query', ctId, info && graphqlFields(info))
    const parsed = qs.parse(q);
    Object.keys(parsed).forEach(key => {
      if (FORBIDDEN_QUERY_PARAMS.includes(key)) {
        throw new Error(`Cannot use a query param named "${key}" here.`);
      }
    });

    const params = Object.assign({
      limit,
      skip,
      include: INCLUDE_DEPTH,
      content_type: ctId
    }, parsed);

    const selectedFields = getSelectedFields(info)
    if (selectedFields) {
      params.select = selectedFields
    }
    return http.get('/entries', params).then(prime);
  }

  function queryAll (ctId) {
    const paramsFor = page => ({
      limit: MAX_LIMIT,
      skip: page*MAX_LIMIT,
      include: INCLUDE_DEPTH,
      content_type: ctId
    });

    return http.get('/entries', paramsFor(0))
    .then(firstResponse => {
      const length = Math.ceil(firstResponse.total/MAX_LIMIT)-1;
      const pages = Array.apply(null, {length}).map((_, i) => i+1);
      const requests = pages.map(page => http.get('/entries', paramsFor(page)));
      return Promise.all([Promise.resolve(firstResponse)].concat(requests));
    })
    .then(responses => responses.reduce((acc, res) => {
      prime(res);
      return res.items.reduce((acc, item) => {
        if (!acc.some(e => e.sys.id === item.sys.id)) {
          return acc.concat([item]);
        } else {
          return acc;
        }
      }, acc);
    }, []));
  }

  function prime (res) {
    _get(res, ['items'], [])
    .concat(_get(res, ['includes', 'Entry'], []))
    .forEach(e => loader.prime(e.sys.id, e));

    _get(res, ['includes', 'Asset'], [])
    .forEach(a => assets[a.sys.id] = a);

    return res;
  }
}
