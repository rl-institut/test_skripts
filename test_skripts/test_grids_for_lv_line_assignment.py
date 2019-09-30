# Script to check where lines are chosen differently in same networks

#region IMPORT
import pandas as pd
from pandas.util.testing import assert_frame_equal
from egoio.tools import db
from sqlalchemy.orm import sessionmaker
#import required modules of DING0
from ding0.core import NetworkDing0
from ding0.tools.results import load_nd_from_pickle
from ding0.grid.lv_grid.build_grid import grid_model_params_ria, select_grid_model_residential, build_lv_graph_ria, build_lv_graph_residential
from ding0.tools.logger import setup_logger
from test_skripts.tools import find_list_entry_by_name, create_lv_branch_dataframe_from_lv_grid_district, get_lengths_of_line_type
#endregion

# define logger
logger = setup_logger()

#region SETTINGS
nr_test_runs = 2
test_path = 'C:/Users/Anya.Heider/open_BEA/ding0/testdata'
# choose MV Grid Districts to import
mv_grid_districts = [460]
#endgregion

#region CREATE NW
# database connection/ session
engine = db.connection(section='oedb')
session = sessionmaker(bind=engine)()


# instantiate new ding0 network object
nw1 = NetworkDing0(name='network')
nw2 = NetworkDing0(name='network')

# run DING0 on selected MV Grid District
nw1.run_ding0_for_lv_grid_test(session=session,
             mv_grid_districts_no=mv_grid_districts)
nw2.run_ding0_for_lv_grid_test(session=session,
             mv_grid_districts_no=mv_grid_districts)

#endregion

#region LOAD NW


model_params_ri_df = pd.DataFrame()
model_params_agr_df = pd.DataFrame()
model_params_res_df = pd.DataFrame()


def check_lvgd_branch_assignment(lv_grid_district,lv_grid_district2):
    '''
    Checks if assignment of line types in two differing grids is the same.
    :param lv_grid_district: LVGridDistrictDing0
    :param lv_grid_district2: LVGridDistrictDing0
    :return:
    '''
    lv_branches = create_lv_branch_dataframe_from_lv_grid_district(lv_grid_district)
    lv_branches2 = create_lv_branch_dataframe_from_lv_grid_district(lv_grid_district2)
    lv_branch_lengths = get_lengths_of_line_type(lv_branches)
    lv_branch_lengths2 = get_lengths_of_line_type(lv_branches2)
    if lv_branch_lengths is not None and lv_branch_lengths2 is not None:
        try:
            assert_frame_equal(lv_branch_lengths, lv_branch_lengths2)
        except:
            print("Fault in grid district " + repr(lv_grid_district))
    elif (lv_branch_lengths2 is None and lv_branch_lengths is not None) or \
            (lv_branch_lengths2 is not None and lv_branch_lengths is None):
        raise Exception('lv_branch_lengths are not the same, one None')


# get the same mv_grid_district for both networks
for mv_grid_district in nw1.mv_grid_districts():
    mv_grid_district2 = find_list_entry_by_name(repr(mv_grid_district), nw2.mv_grid_districts())
    # get the same lv_load_area for both networks
    for lv_load_area in mv_grid_district.lv_load_areas():
        lv_load_area2 = find_list_entry_by_name(repr(lv_load_area), mv_grid_district2.lv_load_areas())
        if lv_load_area2 is None:
            print(repr(lv_load_area))
        # get the same lv_grid_district for both networks and perform lv_grid buildup
        for lv_grid_district in lv_load_area.lv_grid_districts():
            lv_grid_district2 = find_list_entry_by_name(repr(lv_grid_district),lv_load_area2.lv_grid_districts())
            if lv_grid_district2 is None:
                print(repr(lv_grid_district))
            # get model parameters for retail/industrial and agricultural load and compare them
            tmp_model_params_ria = grid_model_params_ria(lv_grid_district)
            tmp_model_params_ria2 = grid_model_params_ria(lv_grid_district2)
            if tmp_model_params_ria['retail/industrial'] is not None and tmp_model_params_ria2['retail/industrial'] is not None:
                assert_frame_equal(pd.DataFrame.from_dict(tmp_model_params_ria['retail/industrial'], orient='index'), \
                               pd.DataFrame.from_dict(tmp_model_params_ria2['retail/industrial'], orient='index'))
            elif (tmp_model_params_ria['retail/industrial'] is None and tmp_model_params_ria2['retail/industrial'] is not None) or\
                    (tmp_model_params_ria['retail/industrial'] is not None and tmp_model_params_ria2['retail/industrial'] is None):
                raise Exception ('Model parameters ria not the same, one None')
            if tmp_model_params_ria['agricultural'] is not None and tmp_model_params_ria2['agricultural'] is not None:
                assert_frame_equal(pd.DataFrame.from_dict(tmp_model_params_ria['agricultural'], orient='index'), \
                                   pd.DataFrame.from_dict(tmp_model_params_ria2['agricultural'], orient='index'))
            elif (tmp_model_params_ria['agricultural'] is None and tmp_model_params_ria2['agricultural'] is not None) or \
                    (tmp_model_params_ria['agricultural'] is not None and tmp_model_params_ria2['agricultural'] is None):
                raise Exception('Model parameters ria not the same, one None')
            # build graph and compare
            build_lv_graph_ria(lv_grid_district, tmp_model_params_ria)
            build_lv_graph_ria(lv_grid_district2, tmp_model_params_ria2)
            check_lvgd_branch_assignment(lv_grid_district,lv_grid_district2)
            # get model parameters for residential load and compare them
            if lv_grid_district.population > 0 and lv_grid_district.peak_load_residential > 0:
                tmp_model_params_res = select_grid_model_residential(lv_grid_district)
                tmp_model_params_res2 = select_grid_model_residential(lv_grid_district2)
                assert_frame_equal(tmp_model_params_res,tmp_model_params_res2)
                # build residential graph and compare
                build_lv_graph_residential(lv_grid_district,tmp_model_params_res)
                build_lv_graph_residential(lv_grid_district2,tmp_model_params_res2)
                check_lvgd_branch_assignment(lv_grid_district,lv_grid_district2)
# can be used to compare in the end instead of for every lv_grid district
#            if tmp_model_params_ria['retail/industrial'] is not None:
#                model_params_ri_df = model_params_ri_df.append(
#                    pd.DataFrame.from_dict(tmp_model_params_ria['retail/industrial'],orient='index').T,ignore_index = True)
#            if tmp_model_params_ria['agricultural'] is not None:
#                model_params_agr_df = model_params_agr_df.append(
#                    pd.DataFrame.from_dict(tmp_model_params_ria['agricultural'], orient='index').T,ignore_index = True)
#            if lv_grid_district.population > 0 and lv_grid_district.peak_load_residential > 0:
#                model_params_res_df = model_params_res_df.append(select_grid_model_residential(lv_grid_district))
print("Finished filling dictionaries")
#endregion

#region COMPARE DATA
for i in range(1,nr_test_runs):
    assert_frame_equal(model_params_ri_df,model_params_ri_df)
    assert_frame_equal(model_params_agr_df, model_params_agr_df)
    assert_frame_equal(model_params_res_df, model_params_res_df)
#endregion
