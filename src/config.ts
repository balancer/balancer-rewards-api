// GCP Project ID - should be common across the cloud function and the Bigquery Instance
export const projectId = process.env.GCLOUD_PROJECT || 'balancer-rewards-api';
export const dataset = process.env.BIGQUERY_DATASET || 'bal_mining_estimates';
export const poolsTableName = process.env.BIGQUERY_POOL_TABLE ||  'pool_estimates';
export const liquidityProviderTableName = process.env.BIGQUERY_LP_TABLE || 'lp_estimates';
