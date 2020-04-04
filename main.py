"""Runs a simulation to assign drivers to riders within the 2018 Boston Lyft/Uber data set
    import: os, random, list_available_drivers, assign_driver, from efficient_algorithnms import binary_search
    import: pandas as pd"""
import os
import random
import list_available_drivers
import assign_driver
from efficient_algorithms import binary_search
import pandas as pd


def import_data(path, filename):
    os.chdir(path)
    df = pd.read_csv(path + filename)
    return df


def main():
    path = input('Input the path name to the files here (C:/user/folder/): ')
    final_path_main_data = input('Input the final path and path name for the main data: ')
    final_path_driver_pool = input('Input the final path and path name for the simulated driver pool: ')
    main_data = import_data(path, 'boston_simulation.csv')
    driver_pool = import_data(path, 'driver_pool.csv')
    time_distance = import_data(path, 'time_distance.csv')

    main_data['emp_id'] = main_data['emp_id'].astype('object')
    driver_pool['destination'] = driver_pool['destination'].astype('object')

    # Create dictionary to append times to driver location to pick up times by route_1_seconds
    time_distance_dict = {}
    for tup in time_distance.itertuples():
        time_distance_dict[tup[1], tup[2]] = tup[3]

    hours = 4
    epoch_elapsing = 1543203600
    epoch_end = 1543203600 + (60 * 60 * hours)

    # Create list for binary/bi-directional search
    bs_lst = []
    for tup in main_data.itertuples():
        record_epoch_time = tup[3]
        bs_lst.append(record_epoch_time)

    # List of requests that were not sourced the first time
    remaining_requests = []

    while epoch_elapsing != epoch_end:
        # If starting_point is None, no requests for that epoch time found
        starting_point = binary_search(bs_lst, epoch_elapsing)
        if starting_point is not None:
            starting_up = starting_point + 1

            # Drop off any passengers and return driver status to available
            list_available_drivers.drop_off_riders(driver_pool=driver_pool,
                                                   current_time=epoch_elapsing)

            # Service previous requests that have not been filled first
            if len(remaining_requests) > 0:
                print('Analyzing remaining requests:', len(remaining_requests))
                # Create list of available drivers
                driver_list = list_available_drivers.driver_list(driver_pool, epoch_elapsing)

                for request in remaining_requests:
                    # If time exceeds rider's wiling to wait_time (seconds), rider cancels the ride
                    # (lim of our 400 drivers)
                    indx_elapsed_time = remaining_requests.index(request)
                    elapsed_request_time = epoch_elapsing - request[5]
                    remaining_requests[indx_elapsed_time][6] = elapsed_request_time

                    # Willing to wait time less than between 10 to 15 minutes
                    wait_time = random.randint(1200, 1800)
                    if elapsed_request_time >= wait_time:
                        rider_id = request[4]
                        indx_main_data = main_data.loc[main_data['id'] == rider_id].index[0]
                        main_data.at[indx_main_data, 'emp_id'] = 'Ride canceled'
                        main_data.at[indx_main_data, 'simulated_pickup_time'] = 0

                        indx_remove_request = remaining_requests.index(request)
                        remaining_requests.remove(remaining_requests[indx_remove_request])
                    else:
                        # Try to assign remaining requests after removing cancelled rides
                        # Criteria
                        cab_type = request[0]
                        cab_product = request[1]
                        pickup_location = request[2]
                        dropoff_location = request[3]
                        rider_id = request[4]
                        indx_remaining_request = main_data.loc[main_data['id'] == rider_id].index[0]

                        # Sort/return time list of driver current location to customer pick up; return the time[0]
                        # and index[1]
                        time_list = list_available_drivers.time_list(driver_list=driver_list,
                                                                     cab_type=cab_type,
                                                                     cab_product=cab_product,
                                                                     time_distance_dict=time_distance_dict,
                                                                     pickup_location=pickup_location)

                        # If there are any available drivers, apply the first in the sorted time_list (by min time to
                        # pick up rider) time_list = [time, driver_list index]
                        assign_driver.assign_driver(time_list=time_list,
                                                    driver_list=driver_list,
                                                    time_distance_dict=time_distance_dict,
                                                    pickup_location=pickup_location,
                                                    dropoff_location=dropoff_location,
                                                    main_data=main_data,
                                                    driver_pool=driver_pool,
                                                    indx_main_data=indx_remaining_request,
                                                    epoch_elapsing=epoch_elapsing,
                                                    remaining_requests=remaining_requests,
                                                    cab_type=cab_type,
                                                    cab_product=cab_product,
                                                    rider_id=rider_id,
                                                    analyzing_req=True)

            # Start at the indexed starting point and decrease the index by 1 until the
            # record's epoch time != epoch_elapsing time
            print('Analyzing new requests...')
            df_epoch_time = main_data.iloc[starting_point][2]
            while df_epoch_time == epoch_elapsing:
                # Create list of available drivers
                driver_list = list_available_drivers.driver_list(driver_pool, epoch_elapsing)

                # Criteria
                cab_type = main_data.iloc[starting_point][0]
                cab_product = main_data.iloc[starting_point][1]
                pickup_location = main_data.iloc[starting_point][4]
                dropoff_location = main_data.iloc[starting_point][7]
                rider_id = main_data.iloc[starting_point][10]

                # Sort/return time list of driver current location to customer pick up; return the time[0] and index[1]
                time_list = list_available_drivers.time_list(driver_list=driver_list,
                                                             cab_type=cab_type,
                                                             cab_product=cab_product,
                                                             time_distance_dict=time_distance_dict,
                                                             pickup_location=pickup_location)

                # If there are any available drivers, apply the first in the sorted time_list (by min time to pick
                # up rider) time_list = [time, driver_list index]
                assign_driver.assign_driver(time_list=time_list,
                                            driver_list=driver_list,
                                            time_distance_dict=time_distance_dict,
                                            pickup_location=pickup_location,
                                            dropoff_location=dropoff_location,
                                            main_data=main_data,
                                            driver_pool=driver_pool,
                                            indx_main_data=starting_point,
                                            epoch_elapsing=epoch_elapsing,
                                            remaining_requests=remaining_requests,
                                            cab_type=cab_type,
                                            cab_product=cab_product,
                                            rider_id=rider_id,
                                            analyzing_req=False)

                starting_point -= 1
                df_epoch_time = main_data.iloc[starting_point][2]

            # Start searching records by increasing index by 1 until the record's epoch time != the epoch elapsing time
            df_epoch_time = main_data.iloc[starting_up][2]

            while df_epoch_time == epoch_elapsing :
                # Create list of available drivers
                driver_list = list_available_drivers.driver_list(driver_pool, epoch_elapsing)

                # Criteria
                cab_type = main_data.iloc[starting_up][0]
                cab_product = main_data.iloc[starting_up][1]
                pickup_location = main_data.iloc[starting_up][4]
                dropoff_location = main_data.iloc[starting_up][7]
                rider_id = main_data.iloc[starting_point][10]

                # Sort/return time list of driver current location to customer pick up; return the time[0] and index[1]
                time_list = list_available_drivers.time_list(driver_list=driver_list,
                                                             cab_type=cab_type,
                                                             cab_product=cab_product,
                                                             time_distance_dict=time_distance_dict,
                                                             pickup_location=pickup_location)

                # If there are any available drivers, apply the first in the sorted time_list (by min time to pick
                # up rider) time_list = [time, driver_list index]
                assign_driver.assign_driver(time_list=time_list,
                                            driver_list=driver_list,
                                            time_distance_dict=time_distance_dict,
                                            pickup_location=pickup_location,
                                            dropoff_location=dropoff_location,
                                            main_data=main_data,
                                            driver_pool=driver_pool,
                                            indx_main_data=starting_up,
                                            epoch_elapsing=epoch_elapsing,
                                            remaining_requests=remaining_requests,
                                            cab_type=cab_type,
                                            cab_product=cab_product,
                                            rider_id=rider_id,
                                            analyzing_req=False)

                starting_up += 1
                df_epoch_time = main_data.iloc[starting_up][2]
        epoch_elapsing += 1
        print(epoch_end - epoch_elapsing)

    print('Final count of remaining requests:', len(remaining_requests))
    print('Remaining requests:', remaining_requests)
    main_data.to_csv(final_path_main_data, index=False)
    driver_pool.to_csv(final_path_driver_pool, index=False)


if __name__ == '__main__':
    main()