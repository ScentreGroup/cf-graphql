'use strict';

const _get = require('lodash.get');

const {
  GraphQLString,
  GraphQLInt,
  GraphQLFloat,
  GraphQLBoolean,
  GraphQLList
} = require('graphql');

const {AssetType, EntryType, LocationType} = require('./base-types.js');

const NOTHING = {};

const is = type => entity => typeof entity === type;
const isString = is('string');
const isObject = is('object');

module.exports = {
  String: field => createFieldConfig(GraphQLString, field),
  Int: field => createFieldConfig(GraphQLInt, field),
  Float: field => createFieldConfig(GraphQLFloat, field),
  Bool: field => createFieldConfig(GraphQLBoolean, field),
  Location: field => createFieldConfig(LocationType, field),
  Object: createObjectFieldConfig,
  'Array<String>': createArrayOfStringsFieldConfig,
  'Link<Asset>': createAssetFieldConfig,
  'Array<Link<Asset>>': createArrayOfAssetsFieldConfig,
  'Link<Entry>': createEntryFieldConfig,
  'Array<Link<Entry>>': createArrayOfEntriesFieldConfig
};

function createFieldConfig (Type, field, resolveFn) {
  return {
    type: Type,
    resolve: (entity, _, ctx, info) => {
      const fieldValue = _get(entity, ['fields', field.id], NOTHING);
      if (fieldValue !== NOTHING) {
        return resolveFn ? resolveFn(fieldValue, ctx, info) : fieldValue;
      }
    }
  };
}

function createObjectFieldConfig (field) {
  return createFieldConfig(GraphQLString, field, val => JSON.stringify(val));
}

function createArrayOfStringsFieldConfig (field) {
  return createFieldConfig(new GraphQLList(GraphQLString), field);
}

function createAssetFieldConfig (field) {
  return createFieldConfig(AssetType, field, getAsset);
}

function createArrayOfAssetsFieldConfig (field) {
  return createFieldConfig(new GraphQLList(AssetType), field, (links, ctx) => {
    if (Array.isArray(links)) {
      return links.map(link => getAsset(link, ctx)).filter(isObject);
    }
  });
}

function getAsset (link, ctx) {
  const linkedId = getLinkedId(link);
  if (isString(linkedId)) {
    return ctx.entryLoader.getIncludedAsset(linkedId);
  }
}

function createEntryFieldConfig (field, ctIdToType) {
  return createFieldConfig(typeFor(field, ctIdToType), field, (link, ctx, info) => {
    const linkedId = getLinkedId(link);
    if (isString(linkedId)) {
      console.log('getOne from field config!!!!!!!!!!!!!!!!!!!!!!!!!!!')
      return ctx.entryLoader.get(linkedId, field.linkedCts && field.linkedCts[0], info);
    }
  });
}

function createArrayOfEntriesFieldConfig (field, ctIdToType) {
  const Type = new GraphQLList(typeFor(field, ctIdToType));

  return createFieldConfig(Type, field, (links, ctx, info) => {
    if (Array.isArray(links)) {
      const ids = links.map(getLinkedId).filter(isString);
      console.log('getMany from field config ++++++++++++++++++++++++++++++++', Type, field.linkedCts[0]);
      return ctx.entryLoader.getMany(ids, field.linkedCts && field.linkedCts[0], info).then(coll => coll.filter(isObject));
    }
  });
}

function getLinkedId (link) {
  return _get(link, ['sys', 'id']);
}

function typeFor ({linkedCts}, ctIdToType = {}) {
  if (linkedCts && linkedCts.length === 1) {
    return ctIdToType[linkedCts[0]] || EntryType;
  } else {
    return EntryType;
  }
}
