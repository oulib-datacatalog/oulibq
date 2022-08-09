from copy import deepcopy
from requests import patch
from oulibq.tasks.workflows import replicate, managed_replication
from oulibq.tasks.config import BAG_LOCATIONS
import pytest

import bagit
from six import PY2

if PY2:
    from mock import MagicMock, Mock, patch
else:
    from unittest.mock import MagicMock, Mock, patch
