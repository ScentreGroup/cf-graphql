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

const mergeSelectedFields = (fields1, fields2) => {
  if (!fields1 || !fields2) {
    return null
  }
  return uniq(`${fields1},${fields2}`.split(',')).join(',');
}

// TODO: consildate by content types with a union of all the fields
const consolidateManyIdsInfo = idsInfo => {
  console.log('consolidating', idsInfo);

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
  console.log('content fetches', contentFetches);

  return contentFetches;

  /*
  // Get unique list of entries that we want
  const entryKeys = idsInfo.reduce((acc, idInfo) => {
    const [id, contentType, selectedFields] = idInfo.split('&&');
    const currentInfo = acc[id];

    if (currentInfo) {
      if (contentType && currentInfo.contentType && currentInfo.contentType !== contentType) {
        // TODO: This will cause an error anyway throw?
        console.log('bad content type match', currentInfo.contentType, contentType)
      }
      acc[id] = {
        ...currentInfo,
        selectedFields: mergeSelectedFields(currentInfo.selectedFields, selectedFields),
      }
    } else {
      acc[id] = {
        id,
        contentType,
        selectedFields: selectedFields || null,
      };
    }

    return acc;
  }, {})

  // Group by contentType and matching field set
  const matchedData  = Object.keys(entryKeys).reduce((acc, key) => {
    const { selectedFields: selFields, id, contentType } = entryKeys[key]
    if (!selFields) {
      acc.everything.push(id)
    } else {
      const partialKey = `${contentType}&&${selFields}`
      const contentInfo = acc.partial[partialKey]
      if (contentInfo) {
        contentInfo.ids.push(id)
      } else {
        acc.partial[partialKey] = {
          ids: [id],
          contentType,
          selectedFields: selFields,
        }
      }
    }
    return acc
  }, {
    everything: [],
    partial: {},
  })

  return {
    ...matchedData,
    partial: Object.keys(matchedData.partial).map(key => matchedData.partial[key])
  };
  */
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
    const consolidatedFetchInfo = consolidateManyIdsInfo(idsInfo)
    console.log('consolidated', consolidatedFetchInfo)

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

    /*
    const everythingRequests = chunk(consolidatedFetchInfo.everything, CHUNK_SIZE)
    .map(ids => {
      return http.get('/entries', {
        limit: CHUNK_SIZE,
        skip: 0,
        include: INCLUDE_DEPTH,
        'sys.id[in]': ids.join(',')
      })
    });
    const partialRequests = consolidatedFetchInfo.partial.reduce((acc, partialInfo) => {
      const requests = chunk(partialInfo.ids, CHUNK_SIZE)
        .map(ids => {
          return http.get('/entries', {
            limit: CHUNK_SIZE,
            skip: 0,
            include: INCLUDE_DEPTH,
            content_type: partialInfo.contentType,
            select: partialInfo.selectedFields,
            'sys.id[in]': ids.join(',')
          })
        });

      return [...acc, ...requests];
    }, []);
    */

    const allIds = idsInfo.map(idInfo => idInfo.split('&&')[0]);

    return Promise.all(requests)
    .then(responses => responses.reduce((acc, res) => {
      // TODO: prime with related fields
      // prime(res);
      _get(res, ['items'], []).forEach(e => acc[e.sys.id] = e);
      return acc;
    }, {}))
    .then(byId => allIds.map(id => byId[id]));
  }

  // TODO: Get only required fields here too
  function getOne (id, forcedCtId, info) {
    console.log('getOne', id, info && getSelectedFields(info))
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
    // TODO: Don't prime if fields
    return http.get('/entries', params)
      .then(res => {
        if (!selectedFields) {
          prime(res);
        }
        return res;
      });
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
    // TODO: loader.prime - use key as sys.id and field information
    console.log('priming.....', res.length)
    _get(res, ['items'], [])
    .concat(_get(res, ['includes', 'Entry'], []))
    .forEach(e => loader.prime(e.sys.id, e));

    _get(res, ['includes', 'Asset'], [])
    .forEach(a => assets[a.sys.id] = a);

    return res;
  }
}
