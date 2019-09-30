import os
import pandas as pd
from test_skripts.tools import create_test_grids_with_stats
from pandas.util.testing import assert_frame_equal

#region SETTINGS
nr_test_runs = 100
test_path = 'C:/Users/Anya.Heider/open_BEA/ding0/testdata/Python35'
is_create_data = 1
#endregion

#region DATA IMPORT

for i in range(nr_test_runs):
    dir = test_path + "/" + str(i)
    if is_create_data:
        mvgd_stats, mvgd_voltage_nodes, mvgd_current_branches, lvgd_stats, lvgd_voltage_nodes, lvgd_current_branches \
            = create_test_grids_with_stats(dir)
    else:
        mvgd_stats = pd.DataFrame.from_csv(os.path.join(dir, 'mvgd_stats.csv'))
        mvgd_voltage_nodes = pd.DataFrame.from_csv(os.path.join(dir, 'mvgd_voltage_nodes.csv'))
        mvgd_current_branches = pd.DataFrame.from_csv(os.path.join(dir, 'mvgd_current_branches.csv'))
        lvgd_stats = pd.DataFrame.from_csv(os.path.join(dir, 'lvgd_stats.csv'))
        lvgd_voltage_nodes = pd.DataFrame.from_csv(os.path.join(dir, 'lvgd_voltage_nodes.csv'))
        lvgd_current_branches = pd.DataFrame.from_csv(os.path.join(dir, 'lvgd_current_branches.csv'))

    if (i>0):
        assert_frame_equal(mvgd_stats,mvgd_stats_pre, check_dtype = False)
        assert_frame_equal(mvgd_voltage_nodes, mvgd_voltage_nodes_pre, check_dtype=False)
        assert_frame_equal(mvgd_current_branches, mvgd_current_branches_pre, check_dtype=False)
        assert_frame_equal(lvgd_stats,lvgd_stats_pre, check_dtype = False)
        assert_frame_equal(lvgd_voltage_nodes, lvgd_voltage_nodes_pre, check_dtype=False)
        assert_frame_equal(lvgd_current_branches, lvgd_current_branches_pre, check_dtype=False)

    mvgd_stats_pre = mvgd_stats
    mvgd_voltage_nodes_pre = mvgd_voltage_nodes
    mvgd_current_branches_pre = mvgd_current_branches
    lvgd_stats_pre = lvgd_stats
    lvgd_voltage_nodes_pre = lvgd_voltage_nodes
    lvgd_current_branches_pre = lvgd_current_branches
print("Finished importing data.")
#endregion


