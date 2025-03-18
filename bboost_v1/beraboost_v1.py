import numpy as np
import pandas as pd


def run_beraboost_v1(val_pubkey, user_rv_state, vd_state):
    # Compute the total staked amount and user share
    total_staked = user_rv_state.groupby('rv_address').amount.sum()
    user_rv_state['user_share'] = user_rv_state.apply(
        lambda x: x.amount / total_staked[x.rv_address], axis=1)

    # Filter active delegators of the val_pubkey
    delegators_val = vd_state.loc[vd_state.validator_address == val_pubkey].reset_index(
        drop=True)
    delegators_val = delegators_val.loc[delegators_val.delegator_address.isin(
        user_rv_state.user_address)].reset_index(drop=True)
    delegators_val['amount_bgt_delegated_norm'] = delegators_val.amount_bgt_delegated / \
        delegators_val.amount_bgt_delegated.sum()

    # Compute the user share of the active delegators
    delegator_rv = user_rv_state.loc[user_rv_state.user_address.isin(
        delegators_val.delegator_address)].reset_index(drop=True)
    delegator_rv_matrix = delegator_rv.pivot(
        index='user_address', columns='rv_address', values='user_share').fillna(0)

    # Get max user share for each delegator
    max_liq = np.argmax(delegator_rv_matrix, axis=1)

    # Compute the weight
    w = pd.DataFrame({'delegator_address': delegator_rv_matrix.index,
                      'rv_address': delegator_rv_matrix.columns[max_liq]}).merge(delegators_val[['delegator_address', 'amount_bgt_delegated_norm']],
                                                                                 on='delegator_address',
                                                                                 how='left')

    cb = w.groupby('rv_address').amount_bgt_delegated_norm.sum()
    cb = round(cb*1e4)  # Converting values in BP and rounding
    cb = cb.loc[cb > 0]  # Filtering non zero values
    cb_formatted = list(cb.items())
    return cb_formatted
