"""Assigns the closest, available driver to the rider"""
import random


def assign_driver(time_list, driver_list, time_distance_dict, pickup_location, dropoff_location, main_data, driver_pool,
                  indx_main_data, epoch_elapsing, remaining_requests, cab_type, cab_product, rider_id, analyzing_req):
    """If there are any available drivers, apply the first in the sorted time_list (by min time to pick
       up rider) time_list = [time, driver_list index]"""
    if len(time_list) > 0:
        time_to_pickup = time_list[0][0]
        # Include a variable 2-4 minutes to simulate the time it takes the driver to accept the ride and pickup the
        # passenger, variable_time is in seconds
        variable_time = random.randrange(120, 241)
        time_to_pickup += variable_time
        indx = time_list[0][1]

        try:
            time_to_destination = time_distance_dict[pickup_location, dropoff_location]
        except:
            time_to_destination = time_distance_dict[dropoff_location, pickup_location]

        emp_id = driver_list[indx][0]
        indx_driver_pool = driver_pool.loc[driver_pool['emp_id'] == emp_id].index[0]

        main_data.at[indx_main_data, 'emp_id'] = emp_id
        main_data.at[indx_main_data, 'simulated_pickup_time'] = epoch_elapsing + time_to_pickup

        driver_pool.at[indx_driver_pool, 'with_passenger'] = 1
        driver_pool.at[indx_driver_pool, 'start_ride_time'] = epoch_elapsing + time_to_pickup
        driver_pool.at[indx_driver_pool, 'time_to_destination'] = time_to_destination
        driver_pool.at[indx_driver_pool, 'end_ride_time'] = epoch_elapsing + time_to_pickup + time_to_destination
        driver_pool.at[indx_driver_pool, 'destination'] = dropoff_location
    else:
        # Add request to the backlog for analysis by first come, first serve
        # When analyzing_req == False, drivers are not being assigned from analyzing the remaining_request list
        # If this parameter were not included, assigning available drivers to remaining_requests would result in an
        # infinite loop if no driver were available because the request would be appended to the back of the same list
        if not analyzing_req:
            main_data.at[indx_main_data, 'emp_id'] = 'No drivers available'
            request_time = epoch_elapsing
            elapsed_request_time = 0
            remaining_requests.append([cab_type, cab_product, pickup_location, dropoff_location, rider_id,
                                       request_time, elapsed_request_time])
