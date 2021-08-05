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
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cors());

const main = express();
main.use('/liquidity-mining/v1', app);
main.use(cors());
main.use(bodyParser.json());
main.use(bodyParser.urlencoded({ extended: false }));

// expose app for firebase cloud func
export const liquidity_mining_estimates = functions.https.onRequest(main);

// establish a bigquery connection
const bq = new BigQuery({ projectId: Config.projectId });

/*
 *  *****************************
 *  ********* ENDPOINTS**********
 *  *****************************
 */

app.get('/gas/:id', async (req, res) => {
  try {
    const address = req?.params?.id?.toLowerCase();
  
    if (!address) {
      throw new Error('must specify an address');
    }
  
    const query = `select coalesce(sum(bal_reimbursement),0) as earned from ${Config.dataset}.${Config.gasTableName} where lower(address) = @address`;
    
    const options = {
      query: query,
      params: { address },
    };
  
    const [rows] = await bq.query(options);
    if (!rows?.length) throw new Error('Error, no results');

    const current_estimate = rows[0].earned

    const response =  {
      success: true,
      result: current_estimate
    };
  
    return res.status(200).send(response);
  } catch (e) {
    return res.status(400).send({ success: false, error: e?.message });
  }
});

app.get('/gas', async (req, res) => {
  try {
  
    var query = `select coalesce(sum(bal_reimbursement),0) as earned from ${Config.dataset}.${Config.gasTableName}`;

    const options = {
      query: query
    };
  
    const [rows] = await bq.query(options);
    if (!rows?.length) throw new Error('Error, no results');

    const current_estimate = rows[0].earned

    const response =  {
      success: true,
      result: current_estimate
    };
  
    return res.status(200).send(response);
  } catch (e) {
    return res.status(400).send({ success: false, error: e?.message });
  }
});

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

    const results = rows.map((row) => {
      if (!row?.address) throw new Error('there was an error getting pools');

      return {
        snapshot_timestamp: moment(row.timestamp * 1000).toDate(),
        address: row.address,
        current_estimate: `${getRealTimeEstimate(row.earned, row.timestamp * 1000, row.velocity)}`,
        velocity: row.velocity,
        week: row?.week,
      }
    });
  
    const response =  {
      success: true,
      result: {
        current_timestamp: requestedAt,
        pools: results,
      },
    };
  
    return res.status(200).send(response);
  } catch (e) {
    return res.status(400).send({ success: false, error: e?.message });
  }
});

app.get('/liquidity-provider/:id', async (req, res) => {
  try {
    const address = req?.params?.id?.toLowerCase();
  
    if (!address) {
      throw new Error('must specify an address');
    }
  
    const query = `select * from ${Config.dataset}.${Config.liquidityProviderTableName} where lower(address) = @address`;
  
    const options = {
      query: query,
      params: { address },
    };

    const requestedAt = new Date();
  
    const [rows] = await bq.query(options);
    if (!rows?.length) throw new Error('Error, no liquidity providers were found');

    const results = rows.map((row) => {
      if (!row?.address) throw new Error('there was an error getting pools');

      return {
        snapshot_timestamp: moment(row.timestamp * 1000).toDate(),
        address: row.address,
        current_estimate: `${getRealTimeEstimate(row.earned, row.timestamp * 1000, row.velocity)}`,
        velocity: row.velocity,
        week: row?.week,
      }
    });
  
    const response =  {
      success: true,
      result: {
        current_timestamp: requestedAt,
        'liquidity-providers': results,
      },
    };
  
    return res.status(200).send(response);
  } catch (e) {
    return res.status(400).send({ success: false, error: e?.message });
  }
});

app.get('/liquidity-provider-multitoken/:id', async (req, res) => {
  try {
    const address = req?.params?.id?.toLowerCase();
  
    if (!address) {
      throw new Error('must specify an address');
    }
  
    const query = `select * from ${Config.dataset}.${Config.multitokenLiquidityProviderTableName} where lower(address) = @address`;
  
    const options = {
      query: query,
      params: { address },
    };

    const requestedAt = new Date();
  
    const [rows] = await bq.query(options);
    if (!rows?.length) throw new Error('Error, no liquidity providers were found');

    const results = rows.map((row) => {
      if (!row?.address) throw new Error('there was an error getting pools');

      return {
        snapshot_timestamp: moment(row.timestamp * 1000).toDate(),
        address: row.address,
        token_address: row.token_address,
        chain_id: row.chain_id,
        current_estimate: `${getRealTimeEstimate(row.earned, row.timestamp * 1000, row.velocity)}`,
        velocity: row.velocity,
        week: row?.week,
      }
    });
  
    const response =  {
      success: true,
      result: {
        current_timestamp: requestedAt,
        'liquidity-providers': results,
      },
    };
  
    return res.status(200).send(response);
  } catch (e) {
    return res.status(400).send({ success: false, error: e?.message });
  }
});

app.get('/liquidity-provider-multitoken-status', async (req, res) => {
  try {
    const query = `select TIMESTAMP_DIFF(
      CURRENT_TIMESTAMP(),
      TIMESTAMP_SECONDS(max(timestamp)),
      MINUTE) > 80 AS alert from ${Config.dataset}.${Config.multitokenLiquidityProviderTableName}`;
  
    const options = {
      query: query
    };

    const [rows] = await bq.query(options);
    if (!rows?.length) throw new Error('Error, no results');
    const alert = rows[0].alert;
    const response =  {
      success: alert
    };
  
    return res.status(200).send(response);
  } catch (e) {
    return res.status(400).send({ success: false, error: e?.message });
  }
});

app.post('/liquidity-providers', async (req, res) => {
  try {
    const _addresses = req?.body?.addresses;
    if (!_addresses?.length) throw new Error('Please provide a list of addresses to get liquidity providers');

    const addresses = _addresses.map((a: any) => a.toLowerCase());

    const query = `select * from ${Config.dataset}.${Config.liquidityProviderTableName} where lower(address) in unnest(@addresses)`;
  
    const options = {
      query: query,
      params: { addresses },
    };

    const requestedAt = new Date();

    const [rows] = await bq.query(options);
    if (!rows?.length) throw new Error('No liquidity providers were found');
  
    const results = rows.map((row) => {
      if (!row?.address) throw new Error('there was an error getting liquidity providers');

      return {
        snapshot_timestamp: moment(row.timestamp * 1000).toDate(),
        address: row.address,
        current_estimate: `${getRealTimeEstimate(row.earned, row.timestamp * 1000, row.velocity)}`,
        velocity: row.velocity,
        week: row?.week,
      }
    });
  
    const response =  {
      success: true,
      result: {
        current_timestamp: requestedAt,
        'liquidity-providers': results,
      },
    };
  
    return res.status(200).send(response);
  } catch (e) {
    return res.status(400).send({ success: false, error: e?.message });
  }
});

app.post('/pools', async (req, res) => {
  try {
    const _addresses = req?.body?.addresses;
    if (!_addresses?.length) throw new Error('Please provide a list of addresses to get liquidity providers');

    const addresses = _addresses.map((a: any) => a.toLowerCase());

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
        current_estimate: `${getRealTimeEstimate(row.earned, row.timestamp * 1000, row.velocity)}`,
        velocity: row.velocity,
        week: row?.week,
      }
    });
  
    const response =  {
      success: true,
      result: {
        current_timestamp: requestedAt,
        pools: results,
      },
    };
  
    return res.status(200).send(response);
  } catch (e) {
    return res.status(400).send({ success: false, error: e?.message });
  }
});

const port = process.env.PORT || 3000
main.listen(port, () => {
  console.log(`BAL rewards estimation API started on port ${port}`)
})
