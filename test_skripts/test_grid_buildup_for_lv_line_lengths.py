# Script to check where lines are chosen differently in same networks

#region IMPORT
import pandas as pd
from pandas.util.testing import assert_frame_equal
from egoio.tools import db
from sqlalchemy.orm import sessionmaker
#import required modules of DING0
from ding0.core import NetworkDing0
from ding0.tools.logger import setup_logger
import logging
from test_skripts.tools import compare_networks_by_line_type_lengths
#endregion
logger = logging.getLogger('debug')


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
compare_networks_by_line_type_lengths(nw1,nw2, 0)
logger.debug("########## New Networks initiated #############")

# STEP 1: Import MV Grid Districts and subjacent objects
nw1.import_mv_grid_districts(session,
                              mv_grid_districts_no=mv_grid_districts)
nw2.import_mv_grid_districts(session,
                              mv_grid_districts_no=mv_grid_districts)
compare_networks_by_line_type_lengths(nw1,nw2, 1)
logger.debug("########## Step 1 finished: grids imported #############")

# STEP 2: Import generators
nw1.import_generators(session, debug=False)
nw2.import_generators(session, debug=False)
compare_networks_by_line_type_lengths(nw1,nw2, 2)
logger.debug("########## Step 2 finished: generators imported #############")

# STEP 3: Parametrize MV grid
nw1.mv_parametrize_grid(debug=False)
nw2.mv_parametrize_grid(debug=False)
compare_networks_by_line_type_lengths(nw1,nw2, 3)
logger.debug("########## Step 3 finished: MV grids parametrized #############")

# STEP 4: Validate MV Grid Districts
msg = nw1.validate_grid_districts()
msg = nw2.validate_grid_districts()
compare_networks_by_line_type_lengths(nw1,nw2, 4)
logger.debug("########## Step 4 finished: grid districts validated #############")

# STEP 5: Build LV grids
nw1.build_lv_grids()
nw2.build_lv_grids()
compare_networks_by_line_type_lengths(nw1,nw2, 5)
logger.debug("########## Step 5 finished: LV grids built #############")

# STEP 6: Build MV grids
nw1.mv_routing(debug=False)
nw2.mv_routing(debug=False)
compare_networks_by_line_type_lengths(nw1,nw2, 6)
logger.debug("########## Step 6 finished: MV routing finished #############")

# STEP 7: Connect MV and LV generators
nw1.connect_generators(debug=False)
logger.info("########## ___________NETWORK 2_____________ #############")
nw2.connect_generators(debug=False)
compare_networks_by_line_type_lengths(nw1,nw2, 7)
logger.debug("########## Step 7 finished: generators connected #############")

# STEP 8: Set IDs for all branches in MV and LV grids
nw1.set_branch_ids()
nw2.set_branch_ids()
compare_networks_by_line_type_lengths(nw1,nw2, 8)
logger.debug("########## Step 8 finished: branch IDs set #############")

# STEP 9: Relocate switch disconnectors in MV grid
nw1.set_circuit_breakers(debug=False)
nw2.set_circuit_breakers(debug=False)
compare_networks_by_line_type_lengths(nw1,nw2, 9)
logger.debug("########## Step 9 finished: circuit breakers set #############")

# STEP 10: Open all switch disconnectors in MV grid
nw1.control_circuit_breakers(mode='open')
nw2.control_circuit_breakers(mode='open')
compare_networks_by_line_type_lengths(nw1,nw2, 10)
logger.debug("########## Step 10 finished: circuit breakers controlled #############")

# STEP 11: Do power flow analysis of MV grid
nw1.run_powerflow(session, method='onthefly', export_pypsa=False, debug=False)
nw2.run_powerflow(session, method='onthefly', export_pypsa=False, debug=False)
compare_networks_by_line_type_lengths(nw1,nw2, 11)
logger.debug("########## Step 11 finished: powerflow executed #############")

# STEP 12: Reinforce MV grid
nw1.reinforce_grid()
logger.info("########## ___________NETWORK 2_____________ #############")
nw2.reinforce_grid()
compare_networks_by_line_type_lengths(nw1,nw2, 12)
logger.debug("########## Step 12 finished: grid reinforced #############")

# STEP 13: Close all switch disconnectors in MV grid
nw1.control_circuit_breakers(mode='close')
nw2.control_circuit_breakers(mode='close')
compare_networks_by_line_type_lengths(nw1,nw2, 13)
logger.debug("########## Step 13 finished: circuit breakers controlled #############")