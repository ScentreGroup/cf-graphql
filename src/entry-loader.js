'use strict';

const _get = require('lodash.get');
const chunk = require('lodash.chunk');
const uniq = require('lodash.uniq');
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

const keysToContentfulFields = keys => keys.map(fieldKey => `fields.${fieldKey}`).sort().join(',');

const mergeSelectedFields = (fields1, fields2) => {
  if (!fields1 || !fields2) {
    return null
  }
  return uniq(`${fields1},${fields2}`.split(',')).sort().join(',');
};

const getRequestedFields = (contextData, id) => {
  if (typeof contextData === 'string') {
    return contextData;
  }
  // Get matching fields for the id
  return contextData.reduce((acc, data) => {
    const [sysId, contentType, fields] = data.split('&&')

    if (sysId === id) {
      acc = fields;
    }
  }, null);
}
const makeCacheKeyFromResponseItem = ({ fields, sys }, contextData) => {
  // Combine fields requested and fields available
  const receivedFields = keysToContentfulFields(Object.keys(fields));
  const requestedFields = getRequestedFields(contextData, sys.id);
  const allFields = mergeSelectedFields(receivedFields, requestedFields);
  const contentType = _get(sys, 'contentType.sys.id');
  let suffix = ''
  if (contentType) {
    suffix = `&&${contentType}&&sys,${allFields}`;
  }

  return `${sys.id}${suffix}`;
};

const consolidateManyIdsInfo = idsInfo => {
  const byContentType = idsInfo.reduce((acc, idInfo) => {
    const [id, contentType, selectedFields] = idInfo.split('&&');
    const currentContent = acc[contentType]

    if (currentContent) {
      acc[contentType] = {
        contentType,
        selectedFields: mergeSelectedFields(currentContent.selectedFields, selectedFields),
        ids: uniq([...currentContent.ids, id]),
      }
    } else {
      acc[contentType] = {
        contentType,
        ids: [id],
        selectedFields: selectedFields || null,
      }
    }

    return acc
  }, {});
  const contentFetches = Object.keys(byContentType).map(key => byContentType[key]);

  return contentFetches;
}

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
  const contentfulFields = keysToContentfulFields(topLevelFields)

  return `sys,${contentfulFields}`;
};

const arrayIsSubset = (subSet, fullSet) => {
  return subSet.every(val => {
    return fullSet.includes(val)
  })
}

const matchCacheKey = inputKey => cacheKey => {
  if (inputKey === cacheKey) {
    return true;
  }

  const [inputId, inContentType, inFields] = inputKey.split('&&');
  const [cacheId, cacheContentType, cacheFields] = cacheKey.split('&&');
  if (inputId === cacheId && inContentType === cacheContentType && inFields && cacheFields) {
    // Check if fields in input are a setup of fields in cache
    const inFieldsSplit = inFields.split(',');
    const cacheFieldsSplit = cacheFields.split(',')

    return arrayIsSubset(inFieldsSplit, cacheFieldsSplit);
  }

  return false;
}

function MyCache() {
  const _map = new Map();

  function myGet(key) {
    const keyArray = Array.from(_map.keys());
    const foundKey = keyArray.find(matchCacheKey(key));

    return _map.get(foundKey || key);
  }

  return {
    get: myGet,
    set: function(key, val) {
      // TODO: mySet, remove old subset cache key if adding more fields
      return _map.set(key, val);
    },
    delete: function(key) {
      return _map.delete(key);
    },
    clear: function() {
      return _map.clear();
    }
  };
};

function createEntryLoader (http) {
  const loader = new DataLoader(load, {
    cacheMap: MyCache(),
  });
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
    const consolidatedFetchInfo = consolidateManyIdsInfo(idsInfo)

    const requests = consolidatedFetchInfo.reduce((acc, contentInfo) => {
      const contentRequests = chunk(contentInfo.ids, CHUNK_SIZE)
        .map(ids => {
          const params = {
            limit: CHUNK_SIZE,
            skip: 0,
            include: INCLUDE_DEPTH,
            'sys.id[in]': ids.join(',')
          };
          if (contentInfo.selectedFields && contentInfo.contentType) {
            params.content_type = contentInfo.contentType;
            params.select = contentInfo.selectedFields;
          }
          return http.get('/entries', params)
        })

      return [...acc, ...contentRequests];
    }, []);

    const allIds = idsInfo.map(idInfo => idInfo.split('&&')[0]);

    return Promise.all(requests)
    .then(responses => responses.reduce((acc, res) => {
      // prime with requested fields as empty fields not returned
      prime(res, idsInfo);
      _get(res, ['items'], []).forEach(e => acc[e.sys.id] = e);
      return acc;
    }, {}))
    .then(byId => allIds.map(id => byId[id]));
  }

  function getOne (id, forcedCtId, info) {
    const selectedFields = forcedCtId && getSelectedFields(info);
    return loader.load(`${id}${selectedFields ? `&&${forcedCtId}&&${selectedFields}` : ''}`)
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
    const selectedFields = forcedCtId && getSelectedFields(info);
    const idInfoList = ids.map(id => `${id}${selectedFields ? `&&${forcedCtId}&&${selectedFields}` : ''}`)

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
    return http.get('/entries', params)
      .then(res => prime(res, selectedFields));
  }

  function queryAll (ctId) {
    const paramsFor = page => ({
      limit: MAX_LIMIT,
      skip: page*MAX_LIMIT,
      include: INCLUDE_DEPTH,
      content_type: ctId
    });

    // TODO: Get selected fields only here
    return http.get('/entries', paramsFor(0))
    .then(firstResponse => {
      const length = Math.ceil(firstResponse.total/MAX_LIMIT)-1;
      const pages = Array.apply(null, {length}).map((_, i) => i+1);
      const requests = pages.map(page => http.get('/entries', paramsFor(page)));
      return Promise.all([Promise.resolve(firstResponse)].concat(requests));
    })
    .then(responses => responses.reduce((acc, res) => {
      // TODO: Prime with selected fields?
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

  function prime (res, contextData) {
    _get(res, ['items'], [])
    .concat(_get(res, ['includes', 'Entry'], []))
    .forEach(e => {
      const key = makeCacheKeyFromResponseItem(e, contextData)
      return loader.prime(key, e)
    });

    // TODO: Ensure assets are ok with selected fields
    _get(res, ['includes', 'Asset'], [])
    .forEach(a => assets[a.sys.id] = a);

    return res;
  }
}
