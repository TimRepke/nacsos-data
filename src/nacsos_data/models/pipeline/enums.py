from enum import Enum


# Can be used to indicate how CPU intensive a task usually is. Guide below
# VHIGH -> Full throttle on all available cores all the time
# HIGH -> Bursts/periods of full throttle on all cores, but intermittent
# MEDIUM -> Uses one or a few cores quite intensely
# LOW -> Uses one or a few cores sparingly
# MINIMAL -> Rather insignificant CPU needs (either because it's a very short computation or just not comp. heavy)
class CPULoadClassification(str, Enum):
    VHIGH = 'VHIGH'
    HIGH = 'HIGH'
    MEDIUM = 'MEDIUM'
    LOW = 'LOW'
    MINIMAL = 'MINIMAL'


class TaskStatus(str, Enum):
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'


class ExecutionLocation(str, Enum):
    LOCAL = 'LOCAL'
    PIK = 'PIK'
