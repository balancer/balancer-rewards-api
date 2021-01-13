// GCP Project ID - should be common across the cloud function and the Bigquery Instance
export const projectId = 'balancer-rewards-api';

// Bigquery Dataset Name
export const dataset = 'balancer_rewards';

// Bigquery Table names - shouold be within the `dataset` above
export const poolsTableName = 'pools';
export const liquidityProviderTableName = 'wallets';
