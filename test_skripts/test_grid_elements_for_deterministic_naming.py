# ===== IMPORTS AND CONFIGURATION =====
from test_skripts.tools import create_grid_component_dataframes_from_network, create_alternative_lv_branch_dataframe_from_network
import os
from numpy import array_equal
# import DB interface
from egoio.tools import db

# import required modules of DING0
from ding0.core import NetworkDing0
from ding0.tools.logger import setup_logger
from ding0.tools.results import save_nd_to_pickle, load_nd_from_pickle
from sqlalchemy.orm import sessionmaker
import oedialect
from pandas.util.testing import assert_frame_equal
import pandas as pd

# define logger
logger = setup_logger()

# ===== MAIN =====

#region SETTINGS
# general
is_create_grids = True
is_save_to_csv = False
is_check_only_lv_lines = True
is_alternative_lv_line_naming = True
is_load_from_csv = True
nr_test_runs = 1
test_path = 'C:/Users/Anya.Heider/open_BEA/ding0/testdata'
# choose MV Grid Districts to import
mv_grid_districts = [460]
#endregion

#region CREATE GRIDS
if is_create_grids == True:
    # database connection/ session
    engine = db.connection(section='oedb')
    session = sessionmaker(bind=engine)()

    for i in range(nr_test_runs):
        # instantiate new ding0 network object
        nd = NetworkDing0(name='network')

        # run DING0 on selected MV Grid District
        nd.run_ding0(session=session,
                     mv_grid_districts_no=mv_grid_districts)

        # export grid to file (pickle)
        save_nd_to_pickle(nd, path = test_path, filename='ding0_grids_example{}.pkl'.format(i))
#endregion




#region EXTRACT DATAFRAMES
components_for_comparison = {}
lines_for_comparison = {}
for i in range(nr_test_runs):
    try:
        if not is_load_from_csv:
            nw = load_nd_from_pickle(path = test_path, filename='ding0_grids_example{}.pkl'.format(i))

            if not is_alternative_lv_line_naming:
                # extract component dataframes from network
                components = create_grid_component_dataframes_from_network(nw)
                if is_save_to_csv:
                    # save dataframes as csv
                    dir = test_path + "/" + str(i)
                    if not os.path.exists(dir):
                        os.makedirs(dir)
                    components['mv_grids'].to_csv(os.path.join(dir, '{}_mv_grids.csv'.format(i)), sep=';')
                    components['lv_grids'].to_csv(os.path.join(dir, '{}_lv_grids.csv'.format(i)), sep=';')
                    components['mv_cable_distributors'].to_csv(os.path.join(dir, '{}_mv_cable_distributors.csv'.format(i)), sep=';')
                    components['lv_cable_distributors'].to_csv(os.path.join(dir, '{}_lv_cable_distributors.csv'.format(i)), sep=';')
                    components['circuit_breakers'].to_csv(os.path.join(dir, '{}_circuit_breakers.csv'.format(i)), sep=';')
                    components['generators'].to_csv(os.path.join(dir, '{}_generators.csv'.format(i)), sep=';')
                    components['loads'].to_csv(os.path.join(dir, '{}_loads.csv'.format(i)), sep=';')
                    components['stations'].to_csv(os.path.join(dir, '{}_stations.csv'.format(i)), sep=';')
                    components['transformers'].to_csv(os.path.join(dir, '{}_transformers.csv'.format(i)), sep=';')
                    components['mv_branches'].to_csv(os.path.join(dir, '{}_mv_branches.csv'.format(i)), sep=';')
                    components['lv_branches'].to_csv(os.path.join(dir, '{}_lv_branches.csv'.format(i)), sep=';')
                # save in dictionary
                components_for_comparison[str(i)] = components
            else:
                lines_for_comparison[str(i)] = create_alternative_lv_branch_dataframe_from_network(nw)
                if is_save_to_csv:
                    dir = test_path + "/" + str(i)
                    if not os.path.exists(dir):
                        os.makedirs(dir)
                    lines_for_comparison[str(i)].to_csv(os.path.join(dir, '{}_lv_branches_alt.csv'.format(i)), sep=';')
        else:
            if is_check_only_lv_lines:
                dir = test_path + "/" + str(i)
                if is_alternative_lv_line_naming:
                    lines_for_comparison[str(i)] = pd.read_csv(os.path.join(dir, '{}_lv_branches_alt.csv'.format(i)),
                                                               sep=';',header = 0, index_col= 0)
                else:
                    lines_for_comparison[str(i)] = pd.read_csv(os.path.join(dir, '{}_lv_branches.csv'.format(i)), sep=';', \
                                                           header = 0, index_col= 0, usecols=[0,3,4,5,6,7,8])
    except:
        continue
#endregion

#region COMPARE DATAFRAMES
for i in range(1,nr_test_runs):
    if not is_check_only_lv_lines:
        try:
            assert_frame_equal(components_for_comparison[str(i)]['mv_grids'],components_for_comparison[str(i-1)]['mv_grids'])
        except:
            print("Parameters of mv_grids are not the same.")
        try:
            assert_frame_equal(components_for_comparison[str(i)]['lv_grids'],
                               components_for_comparison[str(i - 1)]['lv_grids'])
        except:
            print("Parameters of lv_grids are not the same.")
        try:
            assert_frame_equal(components_for_comparison[str(i)]['mv_cable_distributors'],
                               components_for_comparison[str(i - 1)]['mv_cable_distributors'])
        except:
            print("Parameters of mv_cable_distributors are not the same.")
        try:
            assert_frame_equal(components_for_comparison[str(i)]['lv_cable_distributors'],
                               components_for_comparison[str(i - 1)]['lv_cable_distributors'])
        except:
            print("Parameters of lv_cable_distributors are not the same.")
        try:
            assert_frame_equal(components_for_comparison[str(i)]['circuit_breakers'],
                               components_for_comparison[str(i - 1)]['circuit_breakers'])
        except:
            print("Parameters of circuit_breakers are not the same.")
        try:
            assert_frame_equal(components_for_comparison[str(i)]['generators'],
                               components_for_comparison[str(i - 1)]['generators'])
        except:
            print("Parameters of generators are not the same.")
        try:
            assert_frame_equal(components_for_comparison[str(i)]['loads'],
                               components_for_comparison[str(i - 1)]['loads'])
        except:
            print("Parameters of loads are not the same.")
        try:
            assert_frame_equal(components_for_comparison[str(i)]['stations'],
                               components_for_comparison[str(i - 1)]['stations'])
        except:
            print("Parameters of stations are not the same.")
        try:
            assert_frame_equal(components_for_comparison[str(i)]['transformers'],
                               components_for_comparison[str(i - 1)]['transformers'])
        except:
            print("Parameters of transformers are not the same.")
        try:
            assert_frame_equal(components_for_comparison[str(i)]['mv_branches'],
                               components_for_comparison[str(i - 1)]['mv_branches'])
        except:
            print("Parameters of mv_branches are not the same.")
        try:
            assert_frame_equal(components_for_comparison[str(i)]['lv_branches'],
                               components_for_comparison[str(i - 1)]['lv_branches'])
        except:
            print("Parameters of lv_branches are not the same.")
    else:
        try:
            assert_frame_equal(lines_for_comparison[str(i)],lines_for_comparison[str(i-1)])
        except Exception as e:
            print(e)
            is_values_are_same = array_equal(lines_for_comparison[str(i)].values, lines_for_comparison[str(i - 1)].values)
            if is_values_are_same:
                print("Neighboring nodes are not named equally, values are the same."+ str(i))
            else:
                print("Neither names nor values are the same." + str(i))
#endregion

