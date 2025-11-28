from qlib.config import C
from qlib.data import D
from qlib.utils import init_instance

# Initialize Qlib environment
C["qlib_home"] = "./qlib_data"
init_instance()
# Load historical data
df = D.features(["AAPL"], fields=["$close", "$volume"], freq="day")
print(df.head())