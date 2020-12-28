import * as functions from 'firebase-functions';
import * as express from 'express';
import * as bodyParser from 'body-parser';
import * as moment from 'moment';
import { BigQuery } from '@google-cloud/bigquery';
import * as Config from './config';
import * as cors from 'cors';

import { getRealTimeEstimate } from './helpers';

/**
 *  Set up express app
 */
const app = express();
app.use(cors());

const main = express();
main.use('/v1', app);
main.use(cors());
main.use(bodyParser.json());
main.use(bodyParser.urlencoded({ extended: false }));

// expose app for firebase cloud func
export const rewardsApi = functions.https.onRequest(main);

// establish a bigquery connection
const bq = new BigQuery({ projectId: Config.projectId });

/*
 *  *****************************
 *  ********* ENDPOINTS**********
 *  *****************************
 */

app.get('/pool/:id', async (req, res) => {
  try {
    const address = req?.params?.id?.toLowerCase();
  
    if (!address) {
      throw new Error('must specify an address');
    }
  
    const query = `select * from ${Config.dataset}.${Config.poolsTableName} where lower(address) = @address`;
  
    const options = {
      query: query,
      params: { address },
    };

    const requestedAt = new Date();
  
    const [rows] = await bq.query(options);
    if (!rows?.length) throw new Error('Error, no pools were found');

    const row = rows[0];
  
    if (!row?.address) {
      throw new Error(`no address found for ${req?.params?.id}`);
    }
  
    const response =  {
      success: true,
      current_timestamp: requestedAt,
      snapshot_timestamp: moment(row.timestamp * 1000).toDate(),
      address: row.address,
      reward_estimate: `${getRealTimeEstimate(row.earned, row.timestamp * 1000, row.velocity)}`,
      velocity: row.velocity,
    };
  
    return res.status(200).send(response);
  } catch (e) {
    return res.status(400).send({ success: false, error: e?.message });
  }
});

app.get('/wallet/:id', async (req, res) => {
  try {
    const address = req?.params?.id?.toLowerCase();
  
    if (!address) {
      throw new Error('must specify an address');
    }
  
    const query = `select * from ${Config.dataset}.${Config.walletsTableName} where lower(address) = @address`;
  
    const options = {
      query: query,
      params: { address },
    };

    const requestedAt = new Date();
  
    const [rows] = await bq.query(options);
    if (!rows?.length) throw new Error('Error, no wallets were found');

    const row = rows[0];
  
    if (!row?.address) {
      throw new Error(`no rewards found for address ${req?.params?.id}`);
    }
  
    const response =  {
      success: true,
      current_timestamp: requestedAt,
      snapshot_timestamp: moment(row.timestamp * 1000).toDate(),
      address: row.address,
      reward_estimate: `${getRealTimeEstimate(row.earned, row.timestamp * 1000, row.velocity)}`,
      velocity: row.velocity,
    };
  
    return res.status(200).send(response);
  } catch (e) {
    return res.status(400).send({ success: false, error: e?.message });
  }
});

// NOTE: GET requests to Cloud Functions must have an EMPTY body, must pass in params in the URL itself
app.get('/wallets/:ids', async (req, res) => {
  try {
    const addresses = req?.params?.ids?.toLowerCase()?.split(',');
    if (!addresses?.length) throw new Error('Please provide a list of addresses to get wallets');

    const query = `select * from ${Config.dataset}.${Config.walletsTableName} where lower(address) in @addresses`;
  
    const options = {
      query: query,
      params: { addresses },
    };

    const requestedAt = new Date();

    const [rows] = await bq.query(options);
    if (!rows?.length) throw new Error('No wallets were found');
  
    const results = rows.map((row) => {
      if (!row?.address) throw new Error('there was an error getting wallets');

      return {
        snapshot_timestamp: moment(row.timestamp * 1000).toDate(),
        address: row.address,
        reward_estimate: `${getRealTimeEstimate(row.earned, row.timestamp * 1000, row.velocity)}`,
        velocity: row.velocity,
      }
    });

  
    const response =  {
      success: true,
      current_timestamp: requestedAt,
      wallets: results,
    };
  
    return res.status(200).send(response);
  } catch (e) {
    return res.status(400).send({ success: false, error: e?.message });
  }
});

app.get('/pools/:ids', async (req, res) => {
  try {
    const addresses = req?.params?.ids?.toLowerCase()?.split(',');
    if (!addresses?.length) throw new Error('Please provide a list of addresses to get pools');

    const query = `select * from ${Config.dataset}.${Config.poolsTableName} where lower(address) in unnest(@addresses)`;

    const options = {
      query: query,
      params: { addresses: addresses },
    };
  
    const requestedAt = new Date();

    const [rows] = await bq.query(options);
    if (!rows?.length) throw new Error('No pools were found');
  
    const results = rows.map((row) => {
      if (!row?.address) throw new Error('there was an error getting pools');

      return {
        snapshot_timestamp: moment(row.timestamp * 1000).toDate(),
        address: row.address,
        reward_estimate: `${getRealTimeEstimate(row.earned, row.timestamp * 1000, row.velocity)}`,
        velocity: row.velocity,
      }
    });
  
    const response =  {
      success: true,
      current_timestamp: requestedAt,
      pools: results,
    };
  
    return res.status(200).send(response);
  } catch (e) {
    return res.status(400).send({ success: false, error: e?.message });
  }
});

