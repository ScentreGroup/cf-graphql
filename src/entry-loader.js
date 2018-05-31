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
const MAX_FIELDS = 50;
const IGNORE_FIELDS = ['sys','_backrefs', '__typename'];

module.exports = createEntryLoader;

const getSelectedFields = info => {
  if (!info) {
    return null;
  }
  const topLevelFields =
    Object.keys(graphqlFields(info)).filter(key => !IGNORE_FIELDS.includes(key));

  if (topLevelFields.length < 1 || topLevelFields.length > MAX_FIELDS) {
    // There is a limit to the number of fields we can select. If too many get everything
    return null;
  }
  const contentfulFields = topLevelFields.map(fieldKey => `fields.${fieldKey}`).sort().join(',');

  return `sys,${contentfulFields}`;
};

function createEntryLoader (http) {
  const loader = new DataLoader(load);
  const assets = {};

  return {
    get: getOne,
    getMany,
    query: (ctId, args, info) => query(ctId, args, info).then(res => res.items),
    count: (ctId, args) => query(ctId, args).then(res => res.total),
    queryAll,
    getIncludedAsset: id => assets[id],
    getTimeline: () => http.timeline
  };

  function load (idsInfo) {
    // we need to chunk IDs and fire multiple requests so we don't produce URLs
    // that are too long (for the server to handle)
    console.log('load', idsInfo)
    const contentType = idsInfo[0].split('&&')[1]
    const selectedFields = idsInfo[0].split('&&')[2]
    console.log(selectedFields)
    const allIds = idsInfo.map(idInfo => idInfo.split('&&')[0])
    const requests = chunk(allIds, CHUNK_SIZE)
    .map(ids => {
      console.log('chunk ids', ids);
      const params = {
        limit: CHUNK_SIZE,
        skip: 0,
        include: INCLUDE_DEPTH,
        'sys.id[in]': ids.join(',')
      }

      if (selectedFields) {
        params.content_type = contentType;
        params.select = selectedFields;
      }

      return http.get('/entries', params)
    });

    return Promise.all(requests)
    .then(responses => responses.reduce((acc, res) => {
      // TODO: Don't prime or prime with field and content info
      prime(res);
      _get(res, ['items'], []).forEach(e => acc[e.sys.id] = e);
      return acc;
    }, {}))
    .then(byId => allIds.map(id => byId[id]));
  }

  function getOne (id, forcedCtId) {
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

  function getMany(ids, forcedCtId, info) {
    console.log('get many', ids)
    const selectedFields = forcedCtId && getSelectedFields(info);
    const idInfoList = ids.map(id => `${id}${selectedFields ? `&&${forcedCtId}&&${selectedFields}` : ''}`)
    console.log('id info list', idInfoList)

    return loader.loadMany.bind(loader)(idInfoList)
  }
  function query (ctId, {q = '', skip = 0, limit = DEFAULT_LIMIT} = {}, info) {
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

    const selectedFields = getSelectedFields(info);
    if (selectedFields) {
      params.select = selectedFields;
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
