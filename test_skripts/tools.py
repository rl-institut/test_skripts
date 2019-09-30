import os
import pandas as pd
import numpy as np
from pandas.util.testing import assert_frame_equal
from egoio.tools import db
from ding0.core import NetworkDing0
from sqlalchemy.orm import sessionmaker
from ding0.tools.results import save_nd_to_pickle, calculate_mvgd_stats, calculate_lvgd_stats, \
    calculate_mvgd_voltage_current_stats, calculate_lvgd_voltage_current_stats

import logging


def create_grid_component_dataframes_from_network(nw):
    '''
    Creates dictionary of components dataframes. Used to examine whether creation of NetworkDing0 is deterministic.
    :param nw: NetworkDing0
    :return: components: Dictionary of components DataFrames
    '''
    # initiate all dicts
    mv_grids_dict = {}
    lv_grids_dict = {}
    mv_cable_distributor_dict = {}
    lv_cable_distributor_dict = {}
    circuit_breaker_dict = {}
    generator_dict = {}
    load_dict = {}
    station_dict = {}
    transformer_dict = {}
    mv_branch_dict = {}
    lv_branch_dict = {}
    # iterate through all mv_grids
    for mv_grid_district in nw.mv_grid_districts():
        # check whether station with same name exists
        try:
            tmp = mv_grids_dict[repr(mv_grid_district.mv_grid)]
            print("There are two MV stations with name " + repr(mv_grid_district.mv_grid))
        except:
            # get values for all MVGridDing0 and MVGridDistrictDing0
            mv_grids_dict[repr(mv_grid_district.mv_grid)] = {
                'mv_number_cable_distributors': len(mv_grid_district.mv_grid._cable_distributors),
                'mv_number_circuit_breaker': len(mv_grid_district.mv_grid._circuit_breakers),
                'mv_number_generators': len(mv_grid_district.mv_grid._generators),
                'mv_number_loads': len(mv_grid_district.mv_grid._loads),
                'mv_number_rings': len(mv_grid_district.mv_grid._rings),
                'mv_default_branch_kind': mv_grid_district.mv_grid.default_branch_kind,
                'mv_default_branch_kind_aggregated': mv_grid_district.mv_grid.default_branch_kind_aggregated,
                'mv_default_branch_kind_settle': mv_grid_district.mv_grid.default_branch_kind_settle,
                'mv_default_branch_type': mv_grid_district.mv_grid.default_branch_type['name'],
                'mv_default_branch_type_aggregated': mv_grid_district.mv_grid.default_branch_type_aggregated['name'],
                'mv_default_branch_type_settle': mv_grid_district.mv_grid.default_branch_type_settle['name'],
                'mv_nominal_voltage': mv_grid_district.mv_grid.v_level,
                'mv_peak_load': mv_grid_district.peak_load,
                'mv_peak_load_aggregated': mv_grid_district.peak_load_aggregated,
                'mv_peak_load_satellites': mv_grid_district.peak_load_satellites
            }
        # check whether station with same name exists
        try:
            tmp = station_dict[repr(mv_grid_district.mv_grid._station)]
            print("There are two MV stations with name " + repr(mv_grid_district.mv_grid._station))
        except:
            # get values for MVStationDing0
            station_dict[repr(mv_grid_district.mv_grid._station)] = {
                'number_trafos': len(mv_grid_district.mv_grid._station._transformers),
                'lat': mv_grid_district.mv_grid._station.geo_data.x,
                'long': mv_grid_district.mv_grid._station.geo_data.y,
                'peak_load': mv_grid_district.mv_grid._station.peak_load,
                'nominal_voltage': mv_grid_district.mv_grid._station.v_level_operation,
                'voltage_res_0': mv_grid_district.mv_grid._station.voltage_res[0],
                'voltage_res_1': mv_grid_district.mv_grid._station.voltage_res[1]
            }
        # iterate through transformers
        trafo_count = 0
        for trafo in mv_grid_district.mv_grid._station._transformers:
            trafo_count += 1
            # check whether transformer with same name exists
            try:
                tmp = transformer_dict[repr(mv_grid_district.mv_grid) + str(trafo_count)]
                print("There are two MV transformers with name " + repr(mv_grid_district.mv_grid) + str(trafo_count))
            except:
                transformer_dict[repr(mv_grid_district.mv_grid) + str(trafo_count)] = {
                    'nominal_power': trafo.s_max_a,
                    'nominal_voltage': trafo.v_level
                }
        # iterate through cable distributors
        for mv_cable_distributor in mv_grid_district.mv_grid._cable_distributors:
            # check whether cable distributor with same name exists
            try:
                tmp = mv_cable_distributor_dict[mv_cable_distributor.pypsa_bus_id]
                print("There are two MV cable distributors with name " + mv_cable_distributor.pypsa_bus_id)
            except:
                # get values for MVCableDistributorDing0
                mv_cable_distributor_dict[mv_cable_distributor.pypsa_bus_id] = {
                    'voltage_res_0': mv_cable_distributor.voltage_res[0],
                    'voltage_res_1': mv_cable_distributor.voltage_res[1]
                }
        # iterate through circuit breaker
        for circuit_breaker in mv_grid_district.mv_grid._circuit_breakers:
            # check whether circuit breaker with same name exists
            try:
                tmp = circuit_breaker_dict[repr(circuit_breaker.grid) + repr(circuit_breaker)]
                print("There are two MV generators with name " + repr(circuit_breaker.grid) + repr(circuit_breaker))
            except:
                # get values for CircuitBreakerDing0
                circuit_breaker_dict[repr(circuit_breaker.grid) + repr(circuit_breaker)] = {
                    'branch': repr(circuit_breaker.branch),
                    'branch_node_0': repr(circuit_breaker.branch_nodes[0]),
                    'branch_node_1': repr(circuit_breaker.branch_nodes[1]),
                    'lat': circuit_breaker.geo_data.x,
                    'long': circuit_breaker.geo_data.y,
                    'status': circuit_breaker.status
                }
        # iterate through generators
        for generator in mv_grid_district.mv_grid._generators:
            # check whether generator with same name exists
            try:
                tmp = generator_dict[repr(generator)]
                print("There are two MV generators with name " + repr(generator))
            except:
                # get values for MV GeneratorDing0
                generator_dict[repr(generator)] = {
                    'capacity': generator.capacity,
                    'capacity_factor': generator.capacity_factor,
                    'lat': generator.geo_data.x,
                    'long': generator.geo_data.y,
                    'pypsa_bus_id': generator.pypsa_bus_id,
                    'subtype': generator.subtype,
                    'type': generator.type,
                    'voltage_level': generator.v_level
                }
        # iterate through branches
        for branch in mv_grid_district.mv_grid.graph_edges():
            try:
                tmp = mv_branch_dict[repr(branch)]
                print("There are two MV branches with name " + repr(branch))
            except:
                mv_branch_dict[repr(branch['branch'])] = {
                    'adj_node_0': repr(branch['adj_nodes'][0]),
                    'adj_node_1': repr(branch['adj_nodes'][1]),
                    'type_kind': branch['branch'].kind,
                    'type_name': branch['branch'].type['name'],
                    'length': branch['branch'].length,
                    'limiting_current': branch['branch'].type['I_max_th'],
                    'resistance': branch['branch'].type['R_per_km'],
                    'inductance': branch['branch'].type['L_per_km']
                }

        # iterate through lv_grids
        for lv_load_area in mv_grid_district.lv_load_areas():
            for lv_grid_district in lv_load_area.lv_grid_districts():
                # check whether grid with same name exists
                try:
                    tmp = lv_grids_dict[repr(lv_grid_district.lv_grid)]
                    print("There are two LV grids with name " + repr(lv_grid_district.lv_grid))
                except:
                    # get values for all LVGridDing0 and LVGridDistrictDing0
                    lv_grids_dict[repr(lv_grid_district.lv_grid)] = {
                        'lv_number_cable_distributors': len(lv_grid_district.lv_grid._cable_distributors),
                        'lv_number_generators': len(lv_grid_district.lv_grid._generators),
                        'lv_number_loads': len(lv_grid_district.lv_grid._loads),
                        'lv_default_branch_kind': lv_grid_district.lv_grid.default_branch_kind,
                        'lv_nominal_voltage': lv_grid_district.lv_grid.v_level,
                        'lv_population': lv_grid_district.population,
                        'lv_peak_load': lv_grid_district.peak_load,
                        'lv_peak_load_agricultural': lv_grid_district.peak_load_agricultural,
                        'lv_peak_load_industrial': lv_grid_district.peak_load_industrial,
                        'lv_peak_load_residential': lv_grid_district.peak_load_residential,
                        'lv_peak_load_retail': lv_grid_district.peak_load_retail,
                        'lv_sector_consumption_agricultural': lv_grid_district.sector_consumption_agricultural,
                        'lv_sector_consumption_industrial': lv_grid_district.sector_consumption_industrial,
                        'lv_sector_consumption_residential': lv_grid_district.sector_consumption_residential,
                        'lv_sector_consumption_retail': lv_grid_district.sector_consumption_retail,
                        'lv_sector_count_agricultural': lv_grid_district.sector_count_agricultural,
                        'lv_sector_count_industrial': lv_grid_district.sector_count_industrial,
                        'lv_sector_count_residential': lv_grid_district.sector_count_residential,
                        'lv_sector_count_retail': lv_grid_district.sector_count_retail
                    }
                # check whether station with same name exists
                try:
                    tmp = station_dict[repr(lv_grid_district.lv_grid._station)]
                    print("There are two LV stations with name " + repr(lv_grid_district.lv_grid._station))
                except:
                    # get values for LVStationDing0
                    station_dict[repr(lv_grid_district.lv_grid._station)] = {
                        'number_trafos': len(lv_grid_district.lv_grid._station._transformers),
                        'lat': lv_grid_district.lv_grid._station.geo_data.x,
                        'long': lv_grid_district.lv_grid._station.geo_data.y,
                        'peak_load': lv_grid_district.lv_grid._station.peak_load,
                        'peak_generation': lv_grid_district.lv_grid._station.peak_generation,
                        'nominal_voltage': lv_grid_district.lv_grid._station.v_level_operation,
                    }
                # iterate through transformers
                trafo_count = 0
                for trafo in lv_grid_district.lv_grid._station._transformers:
                    trafo_count += 1
                    # check whether transformer with same name exists
                    try:
                        tmp = transformer_dict[repr(lv_grid_district.lv_grid) + str(trafo_count)]
                        print("There are two LV transformers with name " + repr(lv_grid_district.lv_grid) + str(
                            trafo_count))
                    except:
                        # get values vor LV tranformers
                        transformer_dict[repr(lv_grid_district.lv_grid) + str(trafo_count)] = {
                            'nominal_power': trafo.s_max_a,
                            'nominal_voltage': trafo.v_level,
                            'rated_resistance': trafo.r,
                            'rated_reactance': trafo.x
                        }
                # iterate through cable distributors
                for lv_cable_distributor in lv_grid_district.lv_grid._cable_distributors:
                    # check whether cable distributors with same name exists
                    try:
                        tmp = lv_cable_distributor_dict[
                            repr(lv_cable_distributor.grid) + "_cld_" + str(lv_cable_distributor.id_db)]
                        print("There are two cable distributors with name " + repr(
                            lv_cable_distributor.grid) + "_cld_" + str(lv_cable_distributor.id_db))
                    except:
                        # get values for all LVCableDistributorsDing0
                        lv_cable_distributor_dict[
                            repr(lv_cable_distributor.grid) + "_cld_" + str(lv_cable_distributor.id_db)] = {
                            'branch_number': lv_cable_distributor.branch_no,
                            'load_number': lv_cable_distributor.load_no,
                            'string_id': lv_cable_distributor.string_id
                        }
                # iterate through generators
                for generator in lv_grid_district.lv_grid._generators:
                    # check whether generator with same name exists
                    try:
                        tmp = generator_dict[repr(generator)]
                        print("There are two generators with name " + repr(generator))
                    except:
                        # get values for LV GeneratorDing0
                        generator_dict[repr(generator)] = {
                            'capacity': generator.capacity,
                            'capacity_factor': generator.capacity_factor,
                            'lat': generator.geo_data.x,
                            'long': generator.geo_data.y,
                            'pypsa_bus_id': generator.pypsa_bus_id,
                            'subtype': generator.subtype,
                            'type': generator.type,
                            'voltage_level': generator.v_level
                        }
                # iterate through loads
                for load in lv_grid_district.lv_grid._loads:
                    # check whether load with same name exists
                    try:
                        tmp = load_dict[repr(load.grid) + "_" + repr(load)]
                        print("There are two loads with name " + repr(load))
                    except:
                        # get values for loads
                        load_dict[repr(load.grid) + "_" + repr(load)] = {
                            'peak_load': load.peak_load
                        }
                        for consumption_type in load.consumption.keys():
                            load_dict[repr(load.grid) + "_" + repr(load)]["consumption_" + consumption_type] = \
                            load.consumption[consumption_type]

                # iterate through branches
                for branch in lv_grid_district.lv_grid.graph_edges():
                    # check whether branch with same name exists
                    try:
                        tmp = lv_branch_dict[repr(branch)]
                        print("There are two branches with name " + repr(branch))
                    except:
                        # get values for branches
                        lv_branch_dict[repr(branch['branch'])] = {
                            'adj_node_0': repr(branch['adj_nodes'][0]),
                            'adj_node_1': repr(branch['adj_nodes'][1]),
                            'type_kind': branch['branch'].kind,
                            'type_name': branch['branch'].type.name,
                            'length': branch['branch'].length,
                            'limiting_current': branch['branch'].type['I_max_th'],
                            'resistance': branch['branch'].type['R_per_km'],
                            'inductance': branch['branch'].type['L_per_km']
                        }
    print("Finished filling dictionaries.")
    # create dataframes from dictionaries
    mv_grids = pd.DataFrame.from_dict(mv_grids_dict, orient='index')
    lv_grids = pd.DataFrame.from_dict(lv_grids_dict, orient='index')
    mv_cable_distributors = pd.DataFrame.from_dict(mv_cable_distributor_dict, orient='index')
    lv_cable_distributors = pd.DataFrame.from_dict(lv_cable_distributor_dict, orient='index')
    circuit_breakers = pd.DataFrame.from_dict(circuit_breaker_dict, orient='index')
    generators = pd.DataFrame.from_dict(generator_dict, orient='index')
    loads = pd.DataFrame.from_dict(load_dict, orient='index')
    stations = pd.DataFrame.from_dict(station_dict, orient='index')
    transformers = pd.DataFrame.from_dict(transformer_dict, orient='index')
    mv_branches = pd.DataFrame.from_dict(mv_branch_dict, orient='index')
    lv_branches = pd.DataFrame.from_dict(lv_branch_dict, orient='index')

    components = {'mv_grids':mv_grids, 'lv_grids':lv_grids, 'mv_cable_distributors':mv_cable_distributors, \
                  'lv_cable_distributors':lv_cable_distributors, 'circuit_breakers':circuit_breakers,\
                  'generators':generators, 'loads':loads, 'stations':stations, 'transformers':transformers,\
                  'mv_branches':mv_branches, 'lv_branches':lv_branches}

    return components

def create_alternative_lv_branch_dataframe_from_network(nw):
    '''
    Creates alternative lv_branch dict in which lines are represented by the names of their neighboring nodes. Used to find
    whether naming is problematic or other fault occurs.
    :param nw: NetworkDing0
    :return: lv_branches: panda.DataFrame
    '''
    lv_branch_dict = {}
    for mv_grid_district in nw.mv_grid_districts():
        # iterate through lv_grids
        for lv_load_area in mv_grid_district.lv_load_areas():
            for lv_grid_district in lv_load_area.lv_grid_districts():
                # iterate through branches
                for branch in lv_grid_district.lv_grid.graph_edges():
                    # check whether branch with same name exists
                    try:
                        tmp = lv_branch_dict[repr(branch)]
                        print("There are two branches with name " + repr(branch))
                    except:
                        # get values for branches
                        lv_branch_dict[repr(branch['adj_nodes'][0])+repr(branch['adj_nodes'][1])] = {
                            'name':repr(branch['branch']),
                            'type_kind': branch['branch'].kind,
                            'type_name': branch['branch'].type.name,
                            'length': branch['branch'].length,
                            'limiting_current': branch['branch'].type['I_max_th'],
                            'resistance': branch['branch'].type['R_per_km'],
                            'inductance': branch['branch'].type['L_per_km']
                        }
    lv_branches = pd.DataFrame.from_dict(lv_branch_dict, orient='index')
    return lv_branches

def create_lv_branch_dataframe_from_lv_grid_district(lv_grid_district):
    lv_branch_dict = {}
    for branch in lv_grid_district.lv_grid.graph_edges():
        # check whether branch with same name exists
        try:
            tmp = lv_branch_dict[repr(branch)]
            print("There are two branches with name " + repr(branch))
        except:
            # get values for branches
            lv_branch_dict[repr(branch['branch'])] = {
                'adj_node_0': repr(branch['adj_nodes'][0]),
                'adj_node_1': repr(branch['adj_nodes'][1]),
                'type_kind': branch['branch'].kind,
                'type_name': branch['branch'].type.name,
                'length': branch['branch'].length,
                'limiting_current': branch['branch'].type['I_max_th'],
                'resistance': branch['branch'].type['R_per_km'],
                'inductance': branch['branch'].type['L_per_km']
            }
    lv_branches = pd.DataFrame.from_dict(lv_branch_dict,orient= 'index')
    return lv_branches

def get_lengths_of_line_type(lv_lines):
    '''
    Creates a dataframe with the cumulated lengths of all line types existing in the grid. Used to compare lv_grid.

    :param lv_lines: DataFrame
    :return: df_cum_length_of_line_type: DataFrame
    '''
    if len(lv_lines)>0:
        line_types = lv_lines['type_name'].unique()
        cum_length_of_line_type = {}
        for name in line_types:
            cum_length_of_line_type[name] = np.sum(lv_lines['length'].loc[lv_lines['type_name']==name])
        df_cum_length_of_line_type = pd.DataFrame.from_dict(cum_length_of_line_type, orient='index')
        return df_cum_length_of_line_type
    else:
        return None

def compare_networks_by_line_type_lengths(nw1,nw2, step):
    lines1 = create_alternative_lv_branch_dataframe_from_network(nw1)
    lines2 = create_alternative_lv_branch_dataframe_from_network(nw2)
    line_lengths1 = get_lengths_of_line_type(lines1)
    line_lengths2 = get_lengths_of_line_type(lines2)
    if line_lengths1 is not None and line_lengths2 is not None:
        try:
            assert_frame_equal(line_lengths1, line_lengths2)
        except Exception as e:
            print("Fault in step " + str(step))
            raise e
    elif (line_lengths2 is None and line_lengths1 is not None) or \
            (line_lengths2 is not None and line_lengths1 is None):
        raise Exception('lv_branch_lengths are not the same, one None')


def find_list_entry_by_name(name, list):
    '''
    Finds and returns certain entry of list by iterating through list. If no element with representative name is found
    function will return None
    :param name: string
    :param list: list of elements
    :return: element
    '''
    for element in list:
        if repr(element) == name:
            return element

    return None

def create_test_grids_with_stats(path):
    '''
    If changes in electrical data have been made, run this function to update the saved test data in folder.
    Test are run on mv_grid_district 460.
    :param path: directory where testdata ist stored.
    :return: mvgd_stats
    '''

    # database connection/ session
    engine = db.connection(section='oedb')
    session = sessionmaker(bind=engine)()

    # instantiate new ding0 network object
    nd = NetworkDing0(name='network')

    # choose MV Grid Districts to import
    mv_grid_districts = [460]

    # run DING0 on selected MV Grid District
    nd.run_ding0(session=session,
                 mv_grid_districts_no=mv_grid_districts)

    # save network
    if not os.path.exists(path):
        os.makedirs(path)
    save_nd_to_pickle(nd, path=path, filename=None)

    mvgd_stats = calculate_mvgd_stats(nd)
    mvgd_stats.to_csv(os.path.join(path,'mvgd_stats.csv'))
    mvgd_voltage_current_stats = calculate_mvgd_voltage_current_stats(nd)
    mvgd_current_branches = mvgd_voltage_current_stats[1]
    mvgd_current_branches.to_csv(os.path.join(path, 'mvgd_current_branches.csv'))
    mvgd_voltage_nodes = mvgd_voltage_current_stats[0]
    mvgd_voltage_nodes.to_csv(os.path.join(path, 'mvgd_voltage_nodes.csv'))

    lvgd_stats = calculate_lvgd_stats(nd)
    lvgd_stats.to_csv(os.path.join(path, 'lvgd_stats.csv'))
    lvgd_voltage_current_stats = calculate_lvgd_voltage_current_stats(nd)
    lvgd_current_branches = lvgd_voltage_current_stats[1]
    lvgd_current_branches.to_csv(os.path.join(path, 'lvgd_current_branches.csv'))
    lvgd_voltage_nodes = lvgd_voltage_current_stats[0]
    lvgd_voltage_nodes.to_csv(os.path.join(path, 'lvgd_voltage_nodes.csv'))

    return  mvgd_stats, mvgd_voltage_nodes, mvgd_current_branches, lvgd_stats, lvgd_voltage_nodes, lvgd_current_branches
