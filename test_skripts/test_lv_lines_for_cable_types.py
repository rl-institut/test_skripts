#Script used for checking overall lengths of line types of series of NetworkDing0

#region PACKAGE IMPORT
import os
import pandas as pd
from test_skripts.tools import get_lengths_of_line_type
from pandas.util.testing import assert_frame_equal
#endregion

#region SETTINGS
nr_test_runs = 100
test_path = 'C:/Users/Anya.Heider/open_BEA/ding0/testdata'
#endregion

#region DATA IMPORT
lines_lengths_for_comparison = pd.DataFrame()
for i in range(nr_test_runs):
    dir = test_path + "/" + str(i)
    df_lv_lines = pd.read_csv(os.path.join(dir, '{}_lv_branches.csv'.format(i)), sep=';', \
                                               header=0, index_col=0)
    line_length= get_lengths_of_line_type(df_lv_lines)
    lines_lengths_for_comparison = lines_lengths_for_comparison.append(line_length.T,ignore_index=True)
print("Finished importing data.")
overall_length=lines_lengths_for_comparison.sum(1)
#endregion

#region COMPARISON DATA
for i in range(1,nr_test_runs):
    try:
        assert_frame_equal(lines_lengths_for_comparison[str(i)],lines_lengths_for_comparison[str(i-1)])
    except Exception as e:
        if overall_length[i]==overall_length[i-1]:
            print("Overall length is same, but line types differ.")
        else:
            print('Line types and overall length differ.')

#endregion