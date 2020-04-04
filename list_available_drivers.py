"""Creates and returns a tuple of all available drivers;
    Also creates a tuple of (time to pick up location, index of the driver)"""


def driver_list(driver_pool, epoch_elapsing):
    """Create list of available drivers"""
    driver_list = []
    for tup in driver_pool.itertuples():
        with_passenger = tup[11]
        end_work_time = tup[5]
        if with_passenger == 0 and epoch_elapsing <= end_work_time:
            # emp_id, cab_type, cab_product, start_work_time, end_work_time, start_ride_time, end_ride_time,
            # current_location, destination, time_to_destination
            driver_list.append([tup[1], tup[2], tup[3], tup[4], tup[5], tup[6], tup[7], tup[8], tup[9],
                                tup[10], tup[11]])
    return list(driver_list)


def time_list(driver_list, cab_type, cab_product, time_distance_dict, pickup_location):
    """Create time list of driver current location to customer pick up; return the time[0] and driver_list index[1] of
       the driver_list"""
    time_list = []
    for item in driver_list:
        with_passenger = item[10]
        d_cab_type = item[1]
        d_cab_product = item[2]
        d_current_location = item[7]
        d_index = driver_list.index(item)
        if with_passenger == 0 and d_cab_type == cab_type and d_cab_product == cab_product:
            try:
                time_list.append([time_distance_dict[d_current_location, pickup_location],
                                  d_index])
            except:
                time_list.append([time_distance_dict[pickup_location, d_current_location],
                                  d_index])
    time_list.sort()
    return list(time_list)


def drop_off_riders(driver_pool, current_time):
    """Analyze drivers with passengers and return those that have reached the drop off time to original settings to
       allow these drivers to be sourced in future requests"""
    for tup in driver_pool.itertuples():
        with_passenger = tup[11]
        end_ride_time = tup[7]
        if with_passenger == 1 and current_time >= end_ride_time:
            emp_id = tup[1]
            row = driver_pool.loc[driver_pool['emp_id'] == emp_id].index[0]
            driver_pool.at[row, 'start_ride_time'] = 0
            driver_pool.at[row, 'end_ride_time'] = 0
            driver_pool.at[row, 'time_to_destination'] = 0
            driver_pool.at[row, 'current_location'] = driver_pool.at[row, 'destination']
            driver_pool.at[row, 'destination'] = ''
            driver_pool.at[row, 'with_passenger'] = 0
