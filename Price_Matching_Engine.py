# set up package environment
import sys
import warnings
# data processing
import numpy as np
import pandas as pd
# pandas > 0.24
import os
import copy
import csv

warnings.filterwarnings('ignore')


def price_match(input_order_flow: list, wait_queue: dict, output: list) -> list:
    """
    input: order inflow list according to time
    output: filled orders appended according to time
    """
    while len(input_order_flow) > 0:
        new_order = copy.deepcopy(input_order_flow[0])
        # print(new_order)
        if new_order[1] in wait_queue.keys():
            buy_queue = wait_queue[new_order[1]][0]
            sell_queue = wait_queue[new_order[1]][1]
        else:
            wait_queue[new_order[1]] = [[], []]
            buy_queue = wait_queue[new_order[1]][0]
            sell_queue = wait_queue[new_order[1]][1]
        # print(buy_queue)
        # print(wait_queue[new_order[1]][0])
        if new_order[3] == 'Sell':
            if len(buy_queue) == 0:
                sell_queue.append(new_order)
                input_order_flow.pop(0)
            else:
                if len(sell_queue) != 0:
                    sell_mkt_order_index = np.where(np.array(sell_queue)[:, 2] == 'MKT')[0]
                else:
                    sell_mkt_order_index = np.array([])
                mkt_order_index = np.where(np.array(buy_queue)[:, 2] == 'MKT')[0]
                # 1 mkt order or not?
                if new_order[2] == 'MKT':
                    if len(sell_mkt_order_index) == 0:
                        if len(mkt_order_index) == 0:
                            # trade according to highest buy order price
                            highest_buy_order_id = np.argmax(
                                np.around(np.array(buy_queue)[:, 2].astype('float32'), 1))  # type: int
                            highest_buy_order = copy.deepcopy(buy_queue[highest_buy_order_id])
                            highest_buy_price = highest_buy_order[2]
                            trade_price = highest_buy_price
                            if new_order[4] > highest_buy_order[4]:
                                trade_volm = highest_buy_order[4]
                                # ? if only 1 row problem
                                input_order_flow[0][4] = input_order_flow[0][4] - trade_volm
                                buy_queue.pop(highest_buy_order_id)
                            elif new_order[4] == highest_buy_order[4]:
                                trade_volm = highest_buy_order[4]
                                input_order_flow.pop(0)
                                buy_queue.pop(highest_buy_order_id)
                            else:
                                trade_volm = new_order[4]
                                input_order_flow.pop(0)
                                buy_queue[highest_buy_order_id][4] = buy_queue[highest_buy_order_id][4] - trade_volm
                            output.append(['Fill'] + highest_buy_order + [trade_price, trade_volm])
                            output.append(['Fill'] + new_order + [trade_price, trade_volm])
                        else:
                            # trade
                            buy_limit_orders = [limit_order for limit_order in buy_queue if limit_order[2] != 'MKT']
                            if len(buy_limit_orders) == 0:
                                # no trade
                                sell_queue.append(new_order)
                                input_order_flow.pop(0)
                            else:
                                highest_buy_order_id = np.argmax(
                                    np.around(np.array(buy_limit_orders)[:, 2].astype('float32'), 1))
                                highest_buy_order = copy.deepcopy(buy_limit_orders[highest_buy_order_id])
                                highest_buy_price = highest_buy_order[2]
                                trade_buy_order = buy_queue[mkt_order_index[0]]
                                if new_order[4] > trade_buy_order[4]:
                                    trade_volm = trade_buy_order[4]
                                    # ? if only 1 row problem
                                    input_order_flow[0][4] = input_order_flow[0][4] - trade_volm
                                    buy_queue.pop(mkt_order_index[0])
                                elif new_order[4] == trade_buy_order[4]:
                                    trade_volm = trade_buy_order[4]
                                    input_order_flow.pop(0)
                                    buy_queue.pop(mkt_order_index[0])
                                else:
                                    trade_volm = new_order[4]
                                    input_order_flow.pop(0)
                                    buy_queue[mkt_order_index[0]][4] = buy_queue[mkt_order_index[0]][4] - trade_volm
                                output.append(['Fill'] + trade_buy_order + [highest_buy_price, trade_volm])
                                output.append(['Fill'] + new_order + [highest_buy_price, trade_volm])
                    else:
                        # no trade
                        sell_queue.append(new_order)
                        input_order_flow.pop(0)
                else:
                    # if there is mkt order in buy queue or not
                    if len(sell_mkt_order_index) == 0:
                        if len(mkt_order_index) == 0:
                            # no mkt order in buy queue
                            # find highest buy order
                            highest_buy_order_id = np.argmax(np.around(np.array(buy_queue)[:, 2].astype('float32'), 1))
                            highest_buy_order = copy.deepcopy(buy_queue[highest_buy_order_id])
                            highest_buy_price = highest_buy_order[2]
                            if highest_buy_price >= new_order[2]:
                                # trade
                                trade_price = highest_buy_price
                                if new_order[4] > highest_buy_order[4]:
                                    trade_volm = highest_buy_order[4]
                                    # ? if only 1 row problem
                                    input_order_flow[0][4] = input_order_flow[0][4] - trade_volm
                                    buy_queue.pop(highest_buy_order_id)
                                elif new_order[4] == highest_buy_order[4]:
                                    trade_volm = highest_buy_order[4]
                                    input_order_flow.pop(0)
                                    buy_queue.pop(highest_buy_order_id)
                                else:
                                    trade_volm = new_order[4]
                                    input_order_flow.pop(0)
                                    buy_queue[highest_buy_order_id][4] = buy_queue[highest_buy_order_id][
                                                                              4] - trade_volm
                                output.append(['Fill'] + highest_buy_order + [trade_price, trade_volm])
                                output.append(['Fill'] + new_order + [trade_price, trade_volm])
                            else:
                                # no trade
                                sell_queue.append(new_order)
                                input_order_flow.pop(0)
                        else:
                            # trade
                            # there is mkt order in buy queue
                            trade_buy_order = copy.deepcopy(buy_queue[mkt_order_index[0]])
                            # trade according to new order price, lower volm
                            trade_price = new_order[2]
                            if new_order[4] > trade_buy_order[4]:
                                trade_volm = trade_buy_order[4]
                                # ? if only 1 row problem
                                input_order_flow[0][4] = input_order_flow[0][4] - trade_volm
                                buy_queue.pop(mkt_order_index[0])
                            elif new_order[4] == trade_buy_order[4]:
                                trade_volm = trade_buy_order[4]
                                input_order_flow.pop(0)
                                buy_queue.pop(mkt_order_index[0])
                            else:
                                trade_volm = new_order[4]
                                input_order_flow.pop(0)
                                buy_queue[mkt_order_index[0]][4] = buy_queue[mkt_order_index[0]][4] - trade_volm
                            output.append(['Fill'] + trade_buy_order + [trade_price, trade_volm])
                            output.append(['Fill'] + new_order + [trade_price, trade_volm])
                    else:
                        if len(mkt_order_index) != 0:
                            # trade
                            # there are both mkt orders in sell and buy, need to trade them first
                            trade_price = new_order[2]
                            trade_buy_order = copy.deepcopy(buy_queue[mkt_order_index[0]])
                            trade_sell_order = copy.deepcopy(sell_queue[sell_mkt_order_index[0]])
                            if trade_sell_order[4] > trade_buy_order[4]:
                                trade_volm = trade_buy_order[4]
                                # ? if only 1 row problem
                                sell_queue[sell_mkt_order_index[0]][4] = trade_sell_order[4] - trade_volm
                                buy_queue.pop(mkt_order_index[0])
                            elif trade_sell_order[4] == trade_buy_order[4]:
                                trade_volm = trade_buy_order[4]
                                sell_queue.pop(sell_mkt_order_index[0])
                                buy_queue.pop(mkt_order_index[0])
                            else:
                                trade_volm = trade_sell_order[4]
                                sell_queue.pop(sell_mkt_order_index[0])
                                buy_queue[mkt_order_index[0]][4] = trade_buy_order[4] - trade_volm
                            if int(trade_buy_order[0][-1]) < int(trade_sell_order[0][-1]):
                                output.append(['Fill'] + trade_buy_order + [trade_price, trade_volm])
                                output.append(['Fill'] + trade_sell_order + [trade_price, trade_volm])
                            else:
                                output.append(['Fill'] + trade_sell_order + [trade_price, trade_volm])
                                output.append(['Fill'] + trade_buy_order + [trade_price, trade_volm])
                        else:
                            # no trade
                            sell_queue.append(new_order)
                            input_order_flow.pop(0)
            # print(buy_queue)
            # print(wait_queue[new_order[1]][0])
        elif new_order[3] == 'Buy':
            if len(sell_queue) == 0:
                buy_queue.append(new_order)
                input_order_flow.pop(0)
            else:
                if len(buy_queue) != 0:
                    mkt_order_index = np.where(np.array(buy_queue)[:, 2] == 'MKT')[0]
                else:
                    mkt_order_index = np.array([])
                sell_mkt_order_index = np.where(np.array(sell_queue)[:, 2] == 'MKT')[0]
                # print(new_order[0])
                # print(len(mkt_order_index))
                # 1 mkt order or not?
                if new_order[2] == 'MKT':
                    if len(mkt_order_index) == 0:
                        if len(sell_mkt_order_index) == 0:
                            # trade according to lowest sell order price
                            lowest_sell_order_id = np.argmin(np.around(np.array(sell_queue)[:, 2].astype('float32'), 1))
                            lowest_sell_order = copy.deepcopy(sell_queue[lowest_sell_order_id])
                            lowest_sell_price = lowest_sell_order[2]
                            trade_price = lowest_sell_price
                            if new_order[4] > lowest_sell_order[4]:
                                trade_volm = lowest_sell_order[4]
                                # ? if only 1 row problem
                                input_order_flow[0][4] = input_order_flow[0][4] - trade_volm
                                sell_queue.pop(lowest_sell_order_id)
                            elif new_order[4] == lowest_sell_order[4]:
                                trade_volm = lowest_sell_order[4]
                                input_order_flow.pop(0)
                                sell_queue.pop(lowest_sell_order_id)
                            else:
                                trade_volm = new_order[4]
                                input_order_flow.pop(0)
                                sell_queue[lowest_sell_order_id][4] = lowest_sell_order[4] - trade_volm
                            output.append(['Fill'] + lowest_sell_order + [trade_price, trade_volm])
                            output.append(['Fill'] + new_order + [trade_price, trade_volm])
                        else:
                            # trade
                            sell_limit_orders = [limit_order for limit_order in sell_queue if limit_order[2] != 'MKT']
                            # print(len(sell_limit_orders))
                            if len(sell_limit_orders) == 0:
                                # no trade
                                buy_queue.append(new_order)
                                input_order_flow.pop(0)
                            else:
                                lowest_sell_order_id = np.argmax(
                                    np.around(np.array(sell_limit_orders)[:, 2].astype('float32'), 1))
                                lowest_sell_order = copy.deepcopy(sell_limit_orders[lowest_sell_order_id])
                                lowest_sell_price = lowest_sell_order[2]
                                trade_sell_order = sell_queue[sell_mkt_order_index[0]]
                                trade_price = lowest_sell_price
                                if new_order[4] > trade_sell_order[4]:
                                    trade_volm = trade_sell_order[4]
                                    # ? if only 1 row problem
                                    input_order_flow[0][4] = input_order_flow[0][4] - trade_volm
                                    sell_queue.pop(sell_mkt_order_index[0])
                                elif new_order[4] == trade_sell_order[4]:
                                    trade_volm = trade_sell_order[4]
                                    input_order_flow.pop(0)
                                    sell_queue.pop(sell_mkt_order_index[0])
                                else:
                                    trade_volm = new_order[4]
                                    input_order_flow.pop(0)
                                    sell_queue[sell_mkt_order_index[0]][4] = sell_queue[sell_mkt_order_index[0]][
                                                                                 4] - trade_volm
                                output.append(['Fill'] + trade_sell_order + [trade_price, trade_volm])
                                output.append(['Fill'] + new_order + [trade_price, trade_volm])
                    else:
                        # no trade
                        buy_queue.append(new_order)
                        input_order_flow.pop(0)
                else:
                    if len(mkt_order_index) == 0:
                        # if there is mkt order in sell queue or not
                        if len(sell_mkt_order_index) == 0:
                            # no mkt order in sell queue
                            # find lowest sell order
                            lowest_sell_order_id = np.argmin(np.around(np.array(sell_queue)[:, 2].astype('float32'), 1))
                            lowest_sell_order = copy.deepcopy(sell_queue[lowest_sell_order_id])
                            lowest_sell_price = lowest_sell_order[2]
                            if lowest_sell_price <= new_order[2]:
                                # trade
                                trade_price = new_order[2]
                                if new_order[4] > lowest_sell_order[4]:
                                    trade_volm = lowest_sell_order[4]
                                    # ? if only 1 row problem
                                    input_order_flow[0][4] = input_order_flow[0][4] - trade_volm
                                    sell_queue.pop(lowest_sell_order_id)
                                elif new_order[4] == lowest_sell_order[4]:
                                    trade_volm = lowest_sell_order[4]
                                    input_order_flow.pop(0)
                                    sell_queue.pop(lowest_sell_order_id)
                                else:
                                    trade_volm = new_order[4]
                                    input_order_flow.pop(0)
                                    sell_queue[lowest_sell_order_id][4] = sell_queue[lowest_sell_order_id][
                                                                              4] - trade_volm
                                    # print(lowest_sell_order)
                                    # print(new_order)
                                output.append(['Fill'] + lowest_sell_order + [trade_price, trade_volm])
                                output.append(['Fill'] + new_order + [trade_price, trade_volm])
                            else:
                                # no trade
                                sell_queue.append(new_order)
                                input_order_flow.pop(0)
                        else:
                            # trade
                            # there is mkt order in sell queue
                            trade_sell_order = copy.deepcopy(sell_queue[sell_mkt_order_index[0]])
                            # trade according to new order price, lower volm
                            trade_price = new_order[2]
                            if new_order[4] > trade_sell_order[4]:
                                trade_volm = trade_sell_order[4]
                                # ? if only 1 row problem
                                input_order_flow[0][4] = input_order_flow[0][4] - trade_volm
                                sell_queue.pop(sell_mkt_order_index[0])
                            elif new_order[4] == trade_sell_order[4]:
                                trade_volm = trade_sell_order[4]
                                input_order_flow.pop(0)
                                sell_queue.pop(sell_mkt_order_index[0])
                            else:
                                trade_volm = new_order[4]
                                input_order_flow.pop(0)
                                sell_queue[sell_mkt_order_index[0]][4] = sell_queue[sell_mkt_order_index[0]][
                                                                             4] - trade_volm
                            output.append(['Fill'] + trade_sell_order + [trade_price, trade_volm])
                            output.append(['Fill'] + new_order + [trade_price, trade_volm])
                    else:
                        if len(sell_mkt_order_index) != 0:
                            # trade
                            # there are both mkt orders in sell and buy, need to trade them first
                            trade_price = new_order[2]
                            trade_buy_order = copy.deepcopy(buy_queue[mkt_order_index[0]])
                            trade_sell_order = copy.deepcopy(sell_queue[sell_mkt_order_index[0]])
                            if trade_sell_order[4] > trade_buy_order[4]:
                                trade_volm = trade_buy_order[4]
                                # ? if only 1 row problem
                                sell_queue[sell_mkt_order_index[0]][4] = trade_sell_order[4] - trade_volm
                                buy_queue.pop(mkt_order_index[0])
                            elif trade_sell_order[4] == trade_buy_order[4]:
                                trade_volm = trade_buy_order[4]
                                sell_queue.pop(sell_mkt_order_index[0])
                                buy_queue.pop(mkt_order_index[0])
                            else:
                                trade_volm = trade_sell_order[4]
                                sell_queue.pop(sell_mkt_order_index[0])
                                buy_queue[mkt_order_index[0]][4] = trade_buy_order[4] - trade_volm
                            if int(trade_buy_order[0][-1]) < int(trade_sell_order[0][-1]):
                                output.append(['Fill'] + trade_buy_order + [trade_price, trade_volm])
                                output.append(['Fill'] + trade_sell_order + [trade_price, trade_volm])
                            else:
                                output.append(['Fill'] + trade_sell_order + [trade_price, trade_volm])
                                output.append(['Fill'] + trade_buy_order + [trade_price, trade_volm])
                        else:
                            # no trade
                            buy_queue.append(new_order)
                            input_order_flow.pop(0)
            # print(buy_queue)
            # print(wait_queue[new_order[1]][0])

        else:
            raise Exception('unrecognized order side!')

    return output


def record_lineup(input_df):
    input_df['OrderQuantity'].loc[input_df['ActionType'] == 'Fill'] = input_df['OrderQuantity'].loc[
        input_df['ActionType'] == 'Ack'].values[0]
    return input_df


def standard_oi(input_df):
    output_df = copy.deepcopy(input_df)
    output_df.insert(0, 'ActionType', 'Ack')
    output_df['FillPrice'] = np.nan
    output_df['FillQuantity'] = np.nan
    output_df['ActionType'].loc[output_df['OrderQuantity'] > 1000000] = 'Reject'
    input_flow = input_df.loc[input_df['OrderQuantity'] <= 1000000].values.tolist()
    output_lists = price_match(copy.deepcopy(input_flow), {}, [])
    output_df = output_df.append(pd.DataFrame(output_lists,
                                              columns=['ActionType', 'OrderID', 'Symbol', 'Price',
                                                       'Side', 'OrderQuantity', 'FillPrice', 'FillQuantity']),
                                 ignore_index=True)
    output_df['FillQuantity'] = output_df['FillQuantity'].astype('Int64')
    if len(output_lists) > 0:
        output_df = output_df.groupby(['OrderID']).apply(lambda x: record_lineup(x))
    return output_df


if __name__ == '__main__':
    sample_a = pd.DataFrame({'OrderID': ['Order1', 'Order2', 'Order3'],
                             'Symbol': ['0700.HK', '0700.HK', '0700.HK'],
                             'Price': [610, 610, 610, ],
                             'Side': ['Sell', 'Sell', 'Buy'],
                             'OrderQuantity': [20000, 10000, 10000]})
    sample_b = pd.DataFrame({'OrderID': ['Order1', 'Order2', 'Order3'],
                             'Symbol': ['0700.HK', '0700.HK', '0700.HK'],
                             'Price': [610, 'MKT', 610, ],
                             'Side': ['Sell', 'Sell', 'Buy'],
                             'OrderQuantity': [20000, 10000, 10000]})
    sample_c = pd.DataFrame({'OrderID': ['Order1', 'Order2'],
                             'Symbol': ['0700.HK', '0700.HK'],
                             'Price': [610, 610, ],
                             'Side': ['Sell', 'Buy'],
                             'OrderQuantity': [10000, 10000000]})
    sample_d = pd.DataFrame({'OrderID': ['Order1', 'Order2', 'Order3'],
                             'Symbol': ['0700.HK', '0005.HK', '0005.HK'],
                             'Price': [610, 49.8, 49.8, ],
                             'Side': ['Sell', 'Sell', 'Buy'],
                             'OrderQuantity': [10000, 10000, 10000]})
    sample_e = pd.DataFrame({'OrderID': ['Order1', 'Order2', 'Order3'],
                             'Symbol': ['0700.HK', '0700.HK', '0700.HK'],
                             'Price': ['MKT', 'MKT', 610, ],
                             'Side': ['Sell', 'Buy', 'Buy'],
                             'OrderQuantity': [20000, 10000, 10000]})

    output_a = standard_oi(sample_a)
    output_b = standard_oi(sample_b)
    output_c = standard_oi(sample_c)
    output_d = standard_oi(sample_d)
    output_e = standard_oi(sample_e)
    print(output_a)
    print(output_b)
    print(output_c)
    print(output_d)
    print(output_e)




