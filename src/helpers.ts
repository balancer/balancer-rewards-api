import * as moment from 'moment';
import { BigNumber } from 'bignumber.js';

/**
 *  Formula:
 *  real_time_estimate = earned + ((current_time - time_of_last_snapshot) * velocity)
 */
export const getRealTimeEstimate = (earned: string, lastSnapshotDate: number, velocity: string) => {
    // Use bigNumebr to avoid any rounding errors
    // NOTE: Do Not convert to number, keep in Bignumber or string format to avoid JS floating point errors
    const _earned = new BigNumber(earned);
    const _velocity = new BigNumber(velocity);

    // Get the time since the last snapshot
    const now = moment();
    const diff = new BigNumber(now.diff(moment(lastSnapshotDate), 'seconds')); // velocity is in bal/second

    // Use toFixed to return a string with no rounding - avoiding .toString() as it will provide the string
    // in exponential form
    return (diff.times(_velocity).plus(_earned)).toFixed();
};
