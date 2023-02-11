import type { ItemList } from "aws-sdk/clients/dynamodb";
import BbPromise from 'bluebird';
import _ from "lodash";
import path from "node:path";
import fs from "node:fs";

// DynamoDB has a 25 item limit in batch requests
// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
const MAX_MIGRATION_CHUNK = 25;

// TODO: let this be configurable
const MIGRATION_SEED_CONCURRENCY = 5;

/**
 * Writes a batch chunk of migration seeds to DynamoDB. DynamoDB has a limit on the number of
 * items that may be written in a batch operation.
 * @param {function} dynamodbWriteFunction The DynamoDB DocumentClient.batchWrite or DynamoDB.batchWriteItem function
 * @param {string} tableName The table name being written to
 * @param {ItemList[]} seeds The migration seeds being written to the table
 */
async function writeSeedBatch(dynamodbWriteFunction: Function, tableName: string, seeds: ItemList) {
  const params = {
    RequestItems: {
      [tableName]: seeds.map((seed) => ({
        PutRequest: {
          Item: seed,
        },
      })),
    },
  };
  return new Promise<void>((resolve, reject) => {
    // interval lets us know how much time we have burnt so far. This lets us have a backoff mechanism to try
    // again a few times in case the Database resources are in the middle of provisioning.
    let interval = 0;
    function execute(interval: number) {
      setTimeout(() => dynamodbWriteFunction(params, (err?: {code: string}) => {
        if (err) {
          if (err.code === "ResourceNotFoundException" && interval <= 5000) {
            execute(interval + 1000);
          } else {
            reject(err);
          }
        } else {
          resolve();
        }
      }), interval);
    }
    execute(interval);
  });
}

/**
 * Writes a seed corpus to the given database table
 * @param {function} dynamodbWriteFunction The DynamoDB DocumentClient.batchWrite or DynamoDB.batchWriteItem function
 * @param {string} tableName The table name
 * @param {any[]} seeds The seed values
 */
export function writeSeeds(dynamodbWriteFunction: Function, tableName: string, seeds: ItemList) {
  if (!dynamodbWriteFunction) {
    throw new Error("dynamodbWriteFunction argument must be provided");
  }
  if (!tableName) {
    throw new Error("table name argument must be provided");
  }
  if (!seeds) {
    throw new Error("seeds argument must be provided");
  }

  if (seeds.length > 0) {
    const seedChunks = _.chunk(seeds, MAX_MIGRATION_CHUNK);
    return BbPromise.map(
      seedChunks,
      (chunk) => writeSeedBatch(dynamodbWriteFunction, tableName, chunk),
      { concurrency: MIGRATION_SEED_CONCURRENCY }
    )
      .then(() => console.log("Seed running complete for table: " + tableName));
  }
  return Promise.resolve();
}

/**
 * A promise-based function that determines if a file exists
 * @param {string} fileName The path to the file
 */
function fileExists(fileName: string) {
  return new Promise((resolve) => {
    fs.access(fileName, (exists) => resolve(exists));
  });
}

/**
 * Transform all selerialized Buffer value in a Buffer value inside a json object
 *
 * @param {json} json with serialized Buffer value.
 * @return {json} json with Buffer object.
 */
function unmarshalBuffer(json: any) {
  _.forEach(json, function(value, key) {
    // Null check to prevent creation of Buffer when value is null
    if (value !== null && value.type==="Buffer") {
      json[key]= Buffer.from(value.data);
    }
  });
  return json;
}

/**
 * Scrapes seed files out of a given location. This file may contain
 * either a simple json object, or an array of simple json objects. An array
 * of json objects is returned.
 *
 * @param {any} location the filename to read seeds from.
 */
function getSeedsAtLocation(location: string) {
  // load the file as JSON
  const result = JSON.parse(fs.readFileSync(location, "utf8"));

  // Ensure the output is an array
  if (Array.isArray(result)) {
    return _.forEach(result, unmarshalBuffer);
  } else {
    return [ unmarshalBuffer(result) ];
  }
}

/**
 * Locates seeds given a set of files to scrape
 * @param {string[]} sources The filenames to scrape for seeds
 */
export function locateSeeds(sources: string[] = [], cwd = process.cwd()) {

  const locations = sources.map((source) => path.join(cwd, source));
  return BbPromise.map(locations, (location) => {
    return fileExists(location).then((exists) => {
      if(!exists) {
        throw new Error("source file " + location + " does not exist");
      }
      return getSeedsAtLocation(location);
    });
  // Smash the arrays together
  }).then(_.flatten);
}

export default { writeSeeds, locateSeeds };
